package simproc

import (
	"fmt"
	"testing"
	"testing/synctest"

	"github.com/Garvit-Jethwani/simgo/sim"
	"github.com/Garvit-Jethwani/simgo/simtest"
)

// Options controls exploration. The embedded simtest options (seed count,
// first seed, output directory) honour the same SIMGO_* environment
// variables as simtest.
type Options struct {
	simtest.Options
	// Replays is how many times each failing seed is re-run to measure how
	// reproducible it is. Default 5.
	Replays int
}

// Report describes one failing seed.
type Report struct {
	Result *sim.Result
	// Replays is how many times the seed was re-run.
	Replays int
	// Exact counts replays with a byte-identical trace.
	Exact int
	// SameViolation counts replays that broke the same invariant.
	SameViolation int
	// TracePath is where the failing run's trace was written, if it was.
	TracePath string
}

func (r Report) String() string {
	return fmt.Sprintf("seed %d: %v\n    reproduced: %s", r.Result.Seed, r.Result.Violation, r.reproduction())
}

func (r Report) reproduction() string {
	switch {
	case r.Replays == 0:
		return "not measured"
	case r.Exact == r.Replays:
		return fmt.Sprintf("exactly, %d of %d replays", r.Exact, r.Replays)
	case r.SameViolation > 0:
		return fmt.Sprintf("same violation in %d of %d replays (%d byte-identical); a same-instant race inside a process may be involved",
			r.SameViolation, r.Replays, r.Exact)
	default:
		return fmt.Sprintf("0 of %d replays; the failure depends on goroutine scheduling (a same-instant race inside a process)", r.Replays)
	}
}

// runOne executes one seed in its own synctest bubble.
func runOne(t *testing.T, cfg Config, seed uint64) *sim.Result {
	var r *sim.Result
	cfg.Seed = seed
	synctest.Test(t, func(*testing.T) { r = Run(cfg) })
	return r
}

// measure re-runs a failing result and fills in the reproduction counts.
func measure(t *testing.T, cfg Config, r *sim.Result, replays int) Report {
	rep := Report{Result: r, Replays: replays}
	fp := r.Fingerprint()
	for i := 0; i < replays; i++ {
		again := runOne(t, cfg, r.Seed)
		if again.Fingerprint() == fp {
			rep.Exact++
		}
		if again.Failed() && again.Violation.Invariant == r.Violation.Invariant {
			rep.SameViolation++
		}
	}
	return rep
}

func resolve(opt Options) Options {
	opt.Options = opt.Options.Resolve()
	if opt.Replays <= 0 {
		opt.Replays = 5
	}
	return opt
}

// Search runs seeds and returns a report for each failure, without failing
// the test. Use it to assert that a bug is found.
func Search(t *testing.T, cfg Config, opt Options) []Report {
	opt = resolve(opt)
	var reports []Report
	for i := 0; i < opt.Seeds; i++ {
		r := runOne(t, cfg, opt.FirstSeed+uint64(i))
		if r.Failed() {
			reports = append(reports, Report{Result: r})
		}
	}
	return reports
}

// Explore runs seeds and fails the test for each violation. Every failing
// seed is re-run to measure how reproducible it is, and its trace is written
// for the time-travel viewer. SIMGO_SEED runs a single seed and always
// writes its trace.
func Explore(t *testing.T, cfg Config, opt Options) []Report {
	t.Helper()
	opt = resolve(opt)

	if seed, ok := simtest.SeedFromEnv(t); ok {
		r := runOne(t, cfg, seed)
		path, err := simtest.WriteTrace(t, r, opt.Options)
		if err != nil {
			t.Logf("simproc: could not write trace: %v", err)
		}
		if !r.Failed() {
			t.Logf("simproc: seed %d passed (%d events)\n    trace: %s", seed, r.Steps, path)
			return nil
		}
		rep := measure(t, cfg, r, opt.Replays)
		rep.TracePath = path
		t.Errorf("simproc: %v\n    trace: %s", rep, path)
		return []Report{rep}
	}

	failures := Search(t, cfg, opt)
	if len(failures) == 0 {
		t.Logf("simproc: %d seeds explored, no violations", opt.Seeds)
		return nil
	}
	t.Errorf("simproc: %d of %d seeds violated an invariant", len(failures), opt.Seeds)
	var reports []Report
	for i, f := range failures {
		if i == opt.MaxReported {
			t.Errorf("simproc: ... and %d more failing seeds", len(failures)-i)
			break
		}
		rep := measure(t, cfg, f.Result, opt.Replays)
		path, err := simtest.WriteTrace(t, f.Result, opt.Options)
		if err != nil {
			path = fmt.Sprintf("(could not write trace: %v)", err)
		}
		rep.TracePath = path
		t.Errorf("simproc: %v\n    trace:  %s\n    replay: %s", rep, path, simtest.ReplayCommand(t, f.Result.Seed))
		reports = append(reports, rep)
	}
	return reports
}
