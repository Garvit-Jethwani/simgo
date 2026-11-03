package simtest

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Garvit-Jethwani/simgo/sim"
)

// counter increments on a timer; the invariant fails once it reaches a
// seed-dependent limit, so only some seeds fail.
type counter struct{ N int }

func (c *counter) Start(env *sim.Env) {
	env.SetTimer("tick", time.Duration(1+env.Rand().IntN(100))*time.Millisecond)
}
func (c *counter) Receive(env *sim.Env, from sim.NodeID, m sim.Message) {}
func (c *counter) Timer(env *sim.Env, name string) {
	c.N++
	env.SetTimer("tick", time.Duration(1+env.Rand().IntN(100))*time.Millisecond)
}
func (c *counter) Snapshot() any { return c }

func flakyConfig() sim.Config {
	return sim.Config{
		Nodes:    1,
		NewNode:  func(sim.NodeID) sim.Node { return &counter{} },
		Duration: time.Second,
		Invariants: []sim.Invariant{{
			Name: "counter stays below 22",
			Check: func(w *sim.World) error {
				if c := w.Node(0).(*counter); c.N >= 22 {
					return errors.New("counter reached 22")
				}
				return nil
			},
		}},
	}
}

// recorder captures test failures instead of failing the real test.
type recorder struct {
	testing.TB
	errors []string
	logs   []string
}

func (r *recorder) Helper() {}
func (r *recorder) Errorf(format string, a ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, a...))
}
func (r *recorder) Logf(format string, a ...any)   { r.logs = append(r.logs, fmt.Sprintf(format, a...)) }
func (r *recorder) Fatalf(format string, a ...any) { r.Errorf(format, a...); panic("fatal") }

func TestSearchFindsOnlyFailingSeeds(t *testing.T) {
	failures := Search(flakyConfig(), Options{Seeds: 40, Parallel: 4})
	if len(failures) == 0 || len(failures) == 40 {
		t.Fatalf("expected some but not all seeds to fail, got %d/40", len(failures))
	}
	for i, r := range failures {
		if !r.Failed() {
			t.Fatalf("Search returned a passing result for seed %d", r.Seed)
		}
		if i > 0 && failures[i-1].Seed >= r.Seed {
			t.Fatal("failures are not sorted by seed")
		}
	}
	// Parallelism must not change which seeds fail.
	again := Search(flakyConfig(), Options{Seeds: 40, Parallel: 1})
	if len(again) != len(failures) {
		t.Fatalf("parallel and sequential searches disagree: %d vs %d", len(failures), len(again))
	}
}

func TestExploreReportsFailuresWithTraceAndReplay(t *testing.T) {
	out := t.TempDir()
	rec := &recorder{TB: t}
	failures := Explore(rec, flakyConfig(), Options{Seeds: 40, OutDir: out, MaxReported: 2})
	if len(failures) == 0 {
		t.Fatal("expected failures")
	}
	// One summary line plus at most MaxReported seeds plus "... and N more".
	if len(rec.errors) < 2 || len(rec.errors) > 4 {
		t.Fatalf("unexpected number of reported errors: %d\n%s", len(rec.errors), strings.Join(rec.errors, "\n"))
	}
	first := rec.errors[1]
	for _, want := range []string{"counter stays below 22", "trace:", "replay: SIMGO_SEED=", "go test -run '^TestExploreReportsFailuresWithTraceAndReplay$' ./simtest"} {
		if !strings.Contains(first, want) {
			t.Errorf("report missing %q:\n%s", want, first)
		}
	}
	if strings.Contains(first, "WARNING") {
		t.Errorf("deterministic code was reported as nondeterministic:\n%s", first)
	}
	entries, _ := os.ReadDir(out)
	if len(entries) == 0 {
		t.Fatal("no trace files written")
	}
}

func TestReplayViaEnv(t *testing.T) {
	failing := Search(flakyConfig(), Options{Seeds: 40})[0].Seed
	t.Setenv("SIMGO_SEED", fmt.Sprint(failing))
	rec := &recorder{TB: t}
	got := Explore(rec, flakyConfig(), Options{Seeds: 1000, OutDir: t.TempDir()})
	if len(got) != 1 || got[0].Seed != failing {
		t.Fatalf("replay should run exactly seed %d, got %d results", failing, len(got))
	}
	if len(rec.errors) != 1 {
		t.Fatalf("expected exactly one reported failure, got %d", len(rec.errors))
	}
}

func TestReplayCommandQuotesTestName(t *testing.T) {
	t.Run("sub/case", func(t *testing.T) {
		cmd := ReplayCommand(t, 7)
		want := "SIMGO_SEED=7 go test -run '^TestReplayCommandQuotesTestName$' ./simtest"
		if cmd != want {
			t.Fatalf("got  %s\nwant %s", cmd, want)
		}
	})
}
