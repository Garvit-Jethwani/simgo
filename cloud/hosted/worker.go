package hosted

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Garvit-Jethwani/simgo/simtest"
)

// Worker pulls shards from a queue and runs them.
type Worker struct {
	Queue   Queue
	Blobs   Blobs
	Results Results

	// Workspace is where checkouts and per-shard files go. Default: a
	// directory under os.TempDir().
	Workspace string
	// Go is the go command. Default "go".
	Go string
	// MaxTraces caps how many traces one shard uploads (SIMGO_MAX_REPORTED).
	// Default 20.
	MaxTraces int
	// ShardTimeout bounds one shard's go test run. Default 30 minutes.
	ShardTimeout time.Duration
	// Checkout fetches repo at ref into dir. Default: GitCheckout.
	Checkout func(ctx context.Context, repo, ref, dir string) error
	Log      *slog.Logger

	mu        sync.Mutex
	checkouts map[string]*checkout
}

type checkout struct {
	once sync.Once
	dir  string
	err  error
}

func (w *Worker) defaults() {
	if w.Workspace == "" {
		w.Workspace = filepath.Join(os.TempDir(), "simgo-worker")
	}
	if w.Go == "" {
		w.Go = "go"
	}
	if w.MaxTraces == 0 {
		w.MaxTraces = 20
	}
	if w.ShardTimeout == 0 {
		w.ShardTimeout = 30 * time.Minute
	}
	if w.Checkout == nil {
		w.Checkout = GitCheckout
	}
	if w.Log == nil {
		w.Log = slog.Default()
	}
}

// Run processes shards until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		if _, err := w.RunOnce(ctx); err != nil && ctx.Err() == nil {
			w.Log.Error("shard failed", "err", err)
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
			}
		}
	}
	return nil
}

// RunOnce receives and processes at most one shard. It reports whether a
// shard was received. A returned error means the shard was left on the
// queue to be retried (an infrastructure problem); problems with the code
// under test are recorded as shard errors instead.
func (w *Worker) RunOnce(ctx context.Context) (bool, error) {
	w.defaults()
	d, err := w.Queue.Receive(ctx)
	if err != nil || d == nil {
		return false, err
	}
	res, err := w.Process(ctx, d.Shard)
	if err != nil {
		return true, err // not deleted: the queue will redeliver it
	}
	if err := w.Results.PutShardResult(ctx, res); err != nil {
		return true, fmt.Errorf("store shard result: %w", err)
	}
	return true, w.Queue.Delete(ctx, d)
}

// errUserCode marks failures caused by the code under test (it does not
// build, or the test does not use SimGo), which retrying will not fix.
var errUserCode = errors.New("code under test")

// Process runs one shard and stores its failures. It returns an error only
// for infrastructure problems worth retrying.
func (w *Worker) Process(ctx context.Context, s Shard) (ShardResult, error) {
	w.defaults()
	start := time.Now()
	res := ShardResult{JobID: s.Job.ID, Index: s.Index}
	log := w.Log.With("job", s.Job.ID, "shard", s.Index, "seeds", fmt.Sprintf("%d+%d", s.FirstSeed, s.Seeds))

	src, err := w.source(ctx, s.Job)
	if err != nil {
		return res, fmt.Errorf("checkout: %w", err)
	}
	work, err := os.MkdirTemp(w.Workspace, "shard-")
	if err != nil {
		return res, err
	}
	defer os.RemoveAll(work)

	records, output, err := w.goTest(ctx, src, work, s)
	finish := func() ShardResult {
		res.Duration = time.Since(start).Seconds()
		res.Finished = time.Now().UTC()
		return res
	}
	if err != nil {
		if errors.Is(err, errUserCode) {
			res.Status, res.Error = "error", fmt.Sprintf("%v\n%s", err, tail(output, 4000))
			log.Warn("shard could not run the test", "err", err)
			return finish(), nil
		}
		return res, err
	}

	for _, rec := range records {
		res.SeedsRun += rec.Seeds
		for _, f := range rec.Failures {
			fail := Failure{
				JobID: s.Job.ID, Group: GroupKey(f.Invariant, f.Message), Seed: f.Seed,
				Invariant: f.Invariant, Message: f.Message, AtNs: f.AtNs,
				Fingerprint: f.Fingerprint, Reproduction: f.Reproduction,
			}
			if f.Trace != "" {
				body, err := os.ReadFile(f.Trace)
				if err == nil {
					key := fmt.Sprintf("traces/%s/seed-%d.html", s.Job.ID, f.Seed)
					if err := w.Blobs.Put(ctx, key, body, "text/html; charset=utf-8"); err != nil {
						return res, fmt.Errorf("upload trace: %w", err)
					}
					fail.TraceKey = key
				}
			}
			if err := w.Results.PutFailure(ctx, fail); err != nil {
				return res, fmt.Errorf("store failure: %w", err)
			}
			res.Failures++
		}
	}
	res.Status = "done"
	log.Info("shard done", "seeds_run", res.SeedsRun, "failures", res.Failures, "seconds", time.Since(start).Seconds())
	return finish(), nil
}

// goTest runs the shard's seeds and returns the SIMGO_REPORT records.
func (w *Worker) goTest(ctx context.Context, src, work string, s Shard) ([]simtest.RunRecord, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, w.ShardTimeout)
	defer cancel()
	report := filepath.Join(work, "report.jsonl")
	cmd := exec.CommandContext(ctx, w.Go, "test", "-count=1", "-run", "^"+s.Job.Test+"$", s.Job.Package)
	cmd.Dir = src
	cmd.Env = append(os.Environ(),
		"SIMGO_FIRST_SEED="+strconv.FormatUint(s.FirstSeed, 10),
		"SIMGO_SEEDS="+strconv.Itoa(s.Seeds),
		"SIMGO_OUT="+filepath.Join(work, "traces"),
		"SIMGO_REPORT="+report,
		"SIMGO_MAX_REPORTED="+strconv.Itoa(w.MaxTraces),
	)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	runErr := cmd.Run() // a failing test exits non-zero; that is expected
	if ctx.Err() == context.DeadlineExceeded {
		return nil, out.Bytes(), fmt.Errorf("%w: go test exceeded %v", errUserCode, w.ShardTimeout)
	}
	records, err := simtest.ReadRecords(report)
	if err != nil && !os.IsNotExist(err) {
		return nil, out.Bytes(), fmt.Errorf("read report: %w", err)
	}
	var mine []simtest.RunRecord
	for _, r := range records {
		if r.Test == s.Job.Test {
			mine = append(mine, r)
		}
	}
	if len(mine) == 0 {
		// No record: the package did not build, the test does not exist,
		// or it does not call simtest.Explore / simproc.Explore.
		return nil, out.Bytes(), fmt.Errorf("%w: go test produced no SimGo report for %s (exit: %v)", errUserCode, s.Job.Test, runErr)
	}
	return mine, out.Bytes(), nil
}

// source returns a checkout of the job's repo at its ref, shared by every
// shard of the job that this worker processes.
func (w *Worker) source(ctx context.Context, j Job) (string, error) {
	sum := sha256.Sum256([]byte(j.Repo + "\x00" + j.Ref))
	key := hex.EncodeToString(sum[:8])
	w.mu.Lock()
	if w.checkouts == nil {
		w.checkouts = map[string]*checkout{}
	}
	c, ok := w.checkouts[key]
	if !ok {
		c = &checkout{dir: filepath.Join(w.Workspace, "src", key)}
		w.checkouts[key] = c
	}
	w.mu.Unlock()
	c.once.Do(func() {
		if err := os.MkdirAll(filepath.Dir(c.dir), 0o755); err != nil {
			c.err = err
			return
		}
		os.RemoveAll(c.dir)
		c.err = w.Checkout(ctx, j.Repo, j.Ref, c.dir)
	})
	if c.err != nil {
		w.mu.Lock()
		delete(w.checkouts, key) // allow a retry
		w.mu.Unlock()
	}
	return c.dir, c.err
}

// GitCheckout fetches ref from repo into dir. Remote repositories are
// fetched shallowly over https; local ones (local mode) are fetched from
// their path.
func GitCheckout(ctx context.Context, repo, ref, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	var env []string
	if !strings.HasPrefix(repo, "https://") {
		// Local mode: the repository may belong to another user (for
		// example, mounted into the worker container), and git refuses
		// those. Trust exactly that path, through a global config file:
		// unlike -c flags, it also reaches the upload-pack subprocess that
		// reads the source repository.
		cfg := dir + ".gitconfig"
		body := fmt.Sprintf("[safe]\n\tdirectory = %s\n\tdirectory = %s\n", repo, filepath.Join(repo, ".git"))
		if err := os.WriteFile(cfg, []byte(body), 0o644); err != nil {
			return err
		}
		defer os.Remove(cfg)
		env = append(os.Environ(), "GIT_CONFIG_GLOBAL="+cfg)
	}
	run := func(args ...string) error {
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.Dir = dir
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("git %v: %v\n%s", args, err, tail(out, 2000))
		}
		return nil
	}
	if err := run("init", "--quiet"); err != nil {
		return err
	}
	if err := run("fetch", "--quiet", "--depth=1", "--", repo, ref); err != nil {
		return err
	}
	return run("checkout", "--quiet", "FETCH_HEAD")
}

func tail(b []byte, n int) string {
	if len(b) > n {
		b = b[len(b)-n:]
	}
	return string(b)
}
