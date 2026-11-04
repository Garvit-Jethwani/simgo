package httpkv

import (
	"testing"
	"testing/synctest"

	"github.com/Garvit-Jethwani/simgo/sim"
	"github.com/Garvit-Jethwani/simgo/simtest"
	"github.com/Garvit-Jethwani/simgo/x/simproc"
)

// TestAsyncReplicationLosesAcknowledgedWrites shows SimGo finding the bug in
// unmodified net/http code, and measures how reproducible the failure is.
func TestAsyncReplicationLosesAcknowledgedWrites(t *testing.T) {
	cfg := Config(AsyncReplication)
	failures := simproc.Search(t, cfg, simproc.Options{Options: simtest.Options{Seeds: 40}})
	if len(failures) == 0 {
		t.Fatal("expected SimGo to find an acknowledged write missing after failover")
	}
	r := failures[0].Result
	path, err := simtest.WriteTrace(t, r, simtest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("violations in %d of 40 seeds; first: seed %d\n    %v\n    trace: %s",
		len(failures), r.Seed, r.Violation, path)
}

// TestFailuresReproduce re-runs failing seeds: goroutine mode is
// best-effort, so this records how reproducible real net/http code is.
func TestFailuresReproduce(t *testing.T) {
	cfg := Config(AsyncReplication)
	failures := simproc.Search(t, cfg, simproc.Options{Options: simtest.Options{Seeds: 40}})
	exact, total := 0, 0
	for _, f := range failures {
		rep := measureReport(t, cfg, f.Result)
		exact += rep.Exact
		total += rep.Replays
	}
	if total == 0 {
		t.Fatal("no failures to replay")
	}
	t.Logf("%d failing seeds replayed %d times: %d byte-identical (%.0f%%)", len(failures), total, exact, 100*float64(exact)/float64(total))
	if exact == 0 {
		t.Fatal("no failure reproduced exactly")
	}
}

func measureReport(t *testing.T, cfg simproc.Config, r *sim.Result) simproc.Report {
	rep := simproc.Report{Result: r, Replays: 3}
	for i := 0; i < rep.Replays; i++ {
		c := cfg
		c.Seed = r.Seed
		var again *sim.Result
		synctest.Test(t, func(*testing.T) { again = simproc.Run(c) })
		if again.Fingerprint() == r.Fingerprint() {
			rep.Exact++
		}
		if again.Failed() && again.Violation.Invariant == r.Violation.Invariant {
			rep.SameViolation++
		}
	}
	return rep
}

// TestSyncReplicationIsSafe is the regression test for the fix.
func TestSyncReplicationIsSafe(t *testing.T) {
	simproc.Explore(t, Config(SyncReplication), simproc.Options{Options: simtest.Options{Seeds: 40}})
}

// TestSyncReplicationMakesProgress guards against a "fix" that never
// acknowledges anything. Without faults every write must be acknowledged:
// the client reads back after each acknowledged write, so acknowledged
// writes and reads must match one to one. (Each iteration takes ~160ms: a
// 100ms pause, a replicated write, and a read.)
func TestSyncReplicationMakesProgress(t *testing.T) {
	cfg := Config(SyncReplication)
	cfg.Seed = 1
	cfg.Faults = simproc.Faults{}
	var r *sim.Result
	synctest.Test(t, func(*testing.T) { r = simproc.Run(cfg) })
	if r.Failed() {
		t.Fatal(r.Violation)
	}
	acks, reads := 0, 0
	for _, e := range r.Trace.Events {
		if e.Kind == sim.KindEmit && e.Detail == EmitAcked {
			acks++
		}
		if e.Kind == sim.KindEmit && e.Detail == EmitRead {
			reads++
		}
	}
	if acks < 100 || reads != acks {
		t.Fatalf("without faults: %d writes acknowledged, %d reads; want >= 100 and equal", acks, reads)
	}
	t.Logf("%d writes acknowledged and read back in 20 simulated seconds", acks)
}
