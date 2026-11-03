package lease

import (
	"testing"

	"github.com/Garvit-Jethwani/simgo/sim"
	"github.com/Garvit-Jethwani/simgo/simtest"
)

// TestNaiveWorkerDoubleCharges shows SimGo finding the bug. It passes when
// the bug is found, and writes the trace so you can step through it.
func TestNaiveWorkerDoubleCharges(t *testing.T) {
	failures := simtest.Search(Config(3, false), simtest.Options{Seeds: 200})
	if len(failures) == 0 {
		t.Fatal("expected SimGo to find a double charge in the naive worker")
	}
	r := failures[0]
	if r.Violation.Invariant != NoDoubleCharge.Name {
		t.Fatalf("expected a double charge, got %v", r.Violation)
	}

	// The failure must reproduce exactly from its seed.
	cfg := Config(3, false)
	cfg.Seed = r.Seed
	if again := sim.Run(cfg); again.Fingerprint() != r.Fingerprint() {
		t.Fatal("replaying the failing seed produced a different trace")
	}

	path, err := simtest.WriteTrace(t, r, simtest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("found the bug in %d of 200 seeds; first: seed %d\n    %v\n    trace: %s",
		len(failures), r.Seed, r.Violation, path)
}

// TestFencedWorkerNeverDoubleCharges is the regression test for the fix.
func TestFencedWorkerNeverDoubleCharges(t *testing.T) {
	simtest.Explore(t, Config(3, true), simtest.Options{Seeds: 500})
}

// TestFencedWorkerMakesProgress guards against "fixing" the bug by never
// charging anything.
func TestFencedWorkerMakesProgress(t *testing.T) {
	cfg := Config(3, true)
	cfg.Seed = 1
	r := sim.Run(cfg)
	charged := 0
	for _, e := range r.Trace.Events {
		if e.Kind == sim.KindEmit && e.Detail == ChargedEvent {
			charged++
		}
	}
	if charged < DefaultJobs/2 {
		t.Fatalf("only %d of %d jobs charged in a minute; the fenced worker is too conservative", charged, DefaultJobs)
	}
}
