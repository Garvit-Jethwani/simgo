package hosted

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNormalizeRejectsUnsafeInput(t *testing.T) {
	base := Job{Repo: "https://github.com/x/y", Ref: "main", Package: "./pkg", Test: "TestX", Seeds: 10}
	cases := map[string]func(*Job){
		"shell in repo":       func(j *Job) { j.Repo = "https://github.com/x/y;rm -rf /" },
		"non-https repo":      func(j *Job) { j.Repo = "git@github.com:x/y" },
		"option as ref":       func(j *Job) { j.Ref = "--upload-pack=evil" },
		"ref with dots":       func(j *Job) { j.Ref = "a..b" },
		"package escapes":     func(j *Job) { j.Package = "./../../etc" },
		"absolute package":    func(j *Job) { j.Package = "/etc" },
		"regexp in test":      func(j *Job) { j.Test = "Test.*" },
		"subtest":             func(j *Job) { j.Test = "TestX/sub" },
		"zero seeds":          func(j *Job) { j.Seeds = 0 },
		"too many seeds":      func(j *Job) { j.Seeds = MaxSeedsPerJob + 1 },
		"shard size too big":  func(j *Job) { j.ShardSize = MaxShardSize + 1 },
		"local path (hosted)": func(j *Job) { j.Repo = "/home/me/repo" },
	}
	for name, mutate := range cases {
		j := base
		mutate(&j)
		if err := j.Normalize(false); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: expected ErrInvalid, got %v", name, err)
		}
	}
	j := base
	if err := j.Normalize(false); err != nil {
		t.Fatalf("valid job rejected: %v", err)
	}
	if j.ID == "" || j.FirstSeed != 1 || j.ShardSize != DefaultShardSize || j.Created.IsZero() {
		t.Fatalf("defaults not filled: %+v", j)
	}
}

func TestShardsCoverEverySeedExactlyOnce(t *testing.T) {
	j := Job{FirstSeed: 101, Seeds: 2503, ShardSize: 1000}
	shards := j.Shards()
	if len(shards) != 3 {
		t.Fatalf("got %d shards, want 3", len(shards))
	}
	next := uint64(101)
	for i, s := range shards {
		if s.Index != i || s.FirstSeed != next {
			t.Fatalf("shard %d starts at %d, want %d", i, s.FirstSeed, next)
		}
		next += uint64(s.Seeds)
	}
	if next != 101+2503 {
		t.Fatalf("shards cover up to %d, want %d", next, 101+2503)
	}
}

func TestGroupKeyMasksNumbers(t *testing.T) {
	a := GroupKey("no double charge", "job 7 charged twice: first by n2, again by n3")
	b := GroupKey("no double charge", "job 31 charged twice: first by n1, again by n4")
	c := GroupKey("no double charge", "job 7 never charged")
	d := GroupKey("other invariant", "job 7 charged twice: first by n2, again by n3")
	if a != b {
		t.Error("same bug with different numbers should group together")
	}
	if a == c || a == d {
		t.Error("different messages or invariants must not group together")
	}
}

func newService() (*Service, *MemoryQueue) {
	q := NewMemoryQueue()
	return &Service{Queue: q, Results: NewMemoryResults(), Blobs: DirBlobs{Dir: os.TempDir()}, AllowLocalRepos: true}, q
}

func TestAPI(t *testing.T) {
	svc, q := newService()
	srv := httptest.NewServer((&API{Service: svc, Token: "s3cret"}).Handler())
	defer srv.Close()

	do := func(method, path, token, body string) (int, map[string]any) {
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}

	if code, _ := do("GET", "/healthz", "", ""); code != 200 {
		t.Errorf("healthz: %d", code)
	}
	if code, _ := do("POST", "/jobs", "", `{}`); code != 401 {
		t.Errorf("no token: %d, want 401", code)
	}
	if code, _ := do("POST", "/jobs", "wrong", `{}`); code != 401 {
		t.Errorf("wrong token: %d, want 401", code)
	}
	if code, out := do("POST", "/jobs", "s3cret", `{"repo":"https://x/y","test":"bad name","seeds":5}`); code != 400 {
		t.Errorf("invalid job: %d %v, want 400", code, out)
	}
	if code, _ := do("POST", "/jobs", "s3cret", `{"repo":"https://x/y","test":"TestX","seeds":5,"surprise":1}`); code != 400 {
		t.Errorf("unknown field: %d, want 400", code)
	}
	code, job := do("POST", "/jobs", "s3cret", `{"repo":"https://github.com/x/y","ref":"main","package":"./p","test":"TestX","seeds":2500,"shard_size":1000,"id":"chosen-by-client"}`)
	if code != 201 || job["id"] == "chosen-by-client" {
		t.Fatalf("submit: %d %v", code, job)
	}
	if q.Len() != 3 {
		t.Fatalf("%d shards enqueued, want 3", q.Len())
	}
	code, st := do("GET", "/jobs/"+job["id"].(string), "s3cret", "")
	if code != 200 || st["state"] != "queued" || st["shards"].(float64) != 3 {
		t.Fatalf("status: %d %v", code, st)
	}
	if code, _ := do("GET", "/jobs/nope", "s3cret", ""); code != 404 {
		t.Errorf("missing job: %d, want 404", code)
	}
}

func TestWorkerRetriesInfrastructureErrors(t *testing.T) {
	svc, q := newService()
	job, err := svc.Submit(context.Background(), Job{Repo: "/nowhere", Test: "TestX", Seeds: 10})
	if err != nil {
		t.Fatal(err)
	}
	w := &Worker{Queue: q, Blobs: svc.Blobs, Results: svc.Results, Workspace: t.TempDir(),
		Checkout: func(context.Context, string, string, string) error { return errors.New("network down") }}
	got, err := w.RunOnce(context.Background())
	if !got || err == nil || !strings.Contains(err.Error(), "network down") {
		t.Fatalf("RunOnce = %v, %v; want a retryable checkout error", got, err)
	}
	if q.Len() != 1 {
		t.Fatal("shard should stay on the queue to be retried")
	}
	if rs, _ := svc.Results.ShardResults(context.Background(), job.ID); len(rs) != 0 {
		t.Fatal("no result should be recorded for a retryable error")
	}
}

// demoRepo creates a git repository with a module whose test uses
// simtest.Explore on a config that fails for some seeds.
func demoRepo(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	_, file, _, _ := runtime.Caller(0)
	simgoRoot, _ := filepath.Abs(filepath.Join(filepath.Dir(file), "..", ".."))
	dir := t.TempDir()
	files := map[string]string{
		"go.mod": fmt.Sprintf("module example.com/demo\n\ngo 1.25\n\nrequire github.com/Garvit-Jethwani/simgo v0.0.0\n\nreplace github.com/Garvit-Jethwani/simgo => %s\n", simgoRoot),
		"demo/demo_test.go": `package demo

import (
	"errors"
	"testing"
	"time"

	"github.com/Garvit-Jethwani/simgo/sim"
	"github.com/Garvit-Jethwani/simgo/simtest"
)

type counter struct{ N int }

func (c *counter) Start(env *sim.Env)                                   { c.tick(env) }
func (c *counter) Receive(env *sim.Env, from sim.NodeID, m sim.Message) {}
func (c *counter) Timer(env *sim.Env, name string)                      { c.N++; c.tick(env) }
func (c *counter) tick(env *sim.Env) {
	env.SetTimer("t", time.Duration(1+env.Rand().IntN(100))*time.Millisecond)
}

func TestFlaky(t *testing.T) {
	simtest.Explore(t, sim.Config{
		Nodes: 1, NewNode: func(sim.NodeID) sim.Node { return &counter{} }, Duration: time.Second,
		Invariants: []sim.Invariant{{Name: "counter stays below 22", Check: func(w *sim.World) error {
			if w.Node(0).(*counter).N >= 22 {
				return errors.New("counter reached 22")
			}
			return nil
		}}},
	}, simtest.Options{Seeds: 10})
}
`,
	}
	for name, body := range files {
		p := filepath.Join(dir, name)
		os.MkdirAll(filepath.Dir(p), 0o755)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{
		{"init", "--quiet"},
		{"add", "."},
		{"-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "--quiet", "-m", "demo"},
	} {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	return dir
}

func drain(t *testing.T, w *Worker, q *MemoryQueue, workers int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for q.Len() > 0 && ctx.Err() == nil {
				if _, err := w.RunOnce(ctx); err != nil {
					t.Errorf("worker: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// TestEndToEnd runs real go test commands: submit, shard, work, report.
func TestEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("runs go test in a temporary module")
	}
	repo := demoRepo(t)
	svc, q := newService()
	blobs := DirBlobs{Dir: t.TempDir()}
	svc.Blobs = blobs
	job, err := svc.Submit(context.Background(), Job{Repo: repo, Ref: "HEAD", Package: "./demo", Test: "TestFlaky", Seeds: 300, ShardSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	w := &Worker{Queue: q, Blobs: blobs, Results: svc.Results, Workspace: t.TempDir(), MaxTraces: 2}
	drain(t, w, q, 2)

	st, err := svc.Status(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if st.State != "done" || st.ShardsDone != 3 || st.SeedsRun != 300 {
		t.Fatalf("status: %+v", st)
	}
	if st.Failures == 0 || len(st.Groups) != 1 {
		t.Fatalf("expected failures in exactly one group, got %d failures in %d groups", st.Failures, len(st.Groups))
	}
	g := st.Groups[0]
	if g.Invariant != "counter stays below 22" || g.Count != st.Failures || g.TraceURL == "" {
		t.Fatalf("group: %+v", g)
	}
	trace, err := os.ReadFile(strings.TrimPrefix(g.TraceURL, "file://"))
	if err != nil || !bytes.Contains(trace, []byte("SimGo")) {
		t.Fatalf("trace not uploaded: %v", err)
	}
	t.Logf("300 seeds in 3 shards: %d failures, 1 group, seeds %v..., trace %s", st.Failures, g.Seeds[:3], g.TraceURL)
}

func TestMissingTestIsRecordedNotRetried(t *testing.T) {
	if testing.Short() {
		t.Skip("runs go test in a temporary module")
	}
	repo := demoRepo(t)
	svc, q := newService()
	job, _ := svc.Submit(context.Background(), Job{Repo: repo, Package: "./demo", Test: "TestDoesNotExist", Seeds: 5})
	w := &Worker{Queue: q, Blobs: svc.Blobs, Results: svc.Results, Workspace: t.TempDir()}
	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatalf("a missing test is the user's problem, not a retryable error: %v", err)
	}
	st, _ := svc.Status(context.Background(), job.ID)
	if st.ShardsFailed != 1 || len(st.Errors) != 1 || !strings.Contains(st.Errors[0], "no SimGo report") {
		t.Fatalf("status: %+v", st)
	}
	if q.Len() != 0 {
		t.Fatal("shard should be removed from the queue")
	}
}
