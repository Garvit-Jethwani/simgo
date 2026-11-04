// Package simtest runs SimGo simulations from go test.
//
//	func TestNoDoubleCharge(t *testing.T) {
//		simtest.Explore(t, cfg, simtest.Options{Seeds: 500})
//	}
//
// Explore runs many seeds in parallel. For every failing seed it writes a
// time-travel trace (an HTML file) and prints a command that replays exactly
// that seed. Setting SIMGO_SEED runs a single seed and always writes its
// trace, which is how you replay a failure.
//
// Environment variables:
//
//	SIMGO_SEED        run only this seed (replay mode)
//	SIMGO_SEEDS       override the number of seeds to explore
//	SIMGO_FIRST_SEED  override the first seed (used to shard exploration)
//	SIMGO_OUT         directory for trace files (default: $TMPDIR/simgo)
package simtest

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Garvit-Jethwani/simgo/sim"
	"github.com/Garvit-Jethwani/simgo/viewer"
)

// Options controls an exploration.
type Options struct {
	// Seeds is how many seeds to run. Default 100.
	Seeds int
	// FirstSeed is the first seed; seeds run from FirstSeed upward. Default 1.
	FirstSeed uint64
	// Parallel is the number of simulations run at once. Default GOMAXPROCS.
	Parallel int
	// MaxReported caps how many failing seeds are reported individually.
	// Default 3.
	MaxReported int
	// OutDir is where trace files are written. Default $SIMGO_OUT, or
	// $TMPDIR/simgo.
	OutDir string
}

func (o Options) withDefaults() Options {
	if n, err := strconv.Atoi(os.Getenv("SIMGO_SEEDS")); err == nil && n > 0 {
		o.Seeds = n
	}
	if o.Seeds <= 0 {
		o.Seeds = 100
	}
	if n, err := strconv.ParseUint(os.Getenv("SIMGO_FIRST_SEED"), 10, 64); err == nil && n > 0 {
		o.FirstSeed = n
	}
	if o.FirstSeed == 0 {
		o.FirstSeed = 1
	}
	if o.Parallel <= 0 {
		o.Parallel = runtime.GOMAXPROCS(0)
	}
	if o.MaxReported <= 0 {
		o.MaxReported = 3
	}
	if o.OutDir == "" {
		o.OutDir = os.Getenv("SIMGO_OUT")
	}
	if o.OutDir == "" {
		o.OutDir = filepath.Join(os.TempDir(), "simgo")
	}
	return o
}

// Resolve returns the options with defaults and environment overrides
// applied, for runners built on top of this package.
func (o Options) Resolve() Options { return o.withDefaults() }

// SeedFromEnv returns the seed in SIMGO_SEED, if set.
func SeedFromEnv(t testing.TB) (uint64, bool) { return envSeed(t) }

// Explore runs cfg across many seeds and fails the test for every seed that
// violates an invariant. cfg.Seed is ignored. It returns the failing results.
func Explore(t testing.TB, cfg sim.Config, opt Options) []*sim.Result {
	t.Helper()
	opt = opt.withDefaults()

	if seed, ok := envSeed(t); ok {
		r := Replay(t, cfg, seed, opt)
		if r.Failed() {
			return []*sim.Result{r}
		}
		return nil
	}

	failures := Search(cfg, opt)
	if len(failures) == 0 {
		t.Logf("simgo: %d seeds explored, no violations", opt.Seeds)
		return nil
	}
	t.Errorf("simgo: %d of %d seeds violated an invariant", len(failures), opt.Seeds)
	for i, r := range failures {
		if i == opt.MaxReported {
			t.Errorf("simgo: ... and %d more failing seeds", len(failures)-i)
			break
		}
		report(t, cfg, r, opt)
	}
	return failures
}

// Search runs cfg across seeds and returns the failing results, sorted by
// seed. It does not touch the test; use it to assert that a bug is found.
func Search(cfg sim.Config, opt Options) []*sim.Result {
	opt = opt.withDefaults()
	seeds := make(chan uint64)
	var (
		mu       sync.Mutex
		failures []*sim.Result
		wg       sync.WaitGroup
	)
	for w := 0; w < opt.Parallel; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seed := range seeds {
				c := cfg
				c.Seed = seed
				if r := sim.Run(c); r.Failed() {
					mu.Lock()
					failures = append(failures, r)
					mu.Unlock()
				}
			}
		}()
	}
	for i := 0; i < opt.Seeds; i++ {
		seeds <- opt.FirstSeed + uint64(i)
	}
	close(seeds)
	wg.Wait()
	sort.Slice(failures, func(i, j int) bool { return failures[i].Seed < failures[j].Seed })
	return failures
}

// Replay runs a single seed, always writes its trace, and fails the test if
// it violates an invariant.
func Replay(t testing.TB, cfg sim.Config, seed uint64, opt Options) *sim.Result {
	t.Helper()
	opt = opt.withDefaults()
	cfg.Seed = seed
	r := sim.Run(cfg)
	if r.Failed() {
		report(t, cfg, r, opt)
		return r
	}
	path, err := WriteTrace(t, r, opt)
	if err != nil {
		t.Logf("simgo: could not write trace: %v", err)
	}
	t.Logf("simgo: seed %d passed (%d steps, %v simulated)\n    trace: %s", seed, r.Steps, r.End, path)
	return r
}

// report fails the test with a description of r, checks that the failure is
// reproducible, and writes its trace.
func report(t testing.TB, cfg sim.Config, r *sim.Result, opt Options) {
	t.Helper()
	path, err := WriteTrace(t, r, opt)
	if err != nil {
		path = fmt.Sprintf("(could not write trace: %v)", err)
	}
	c := cfg
	c.Seed = r.Seed
	again := sim.Run(c)
	determinism := ""
	if again.Fingerprint() != r.Fingerprint() {
		determinism = "\n    WARNING: replaying this seed produced a different trace. The code under test is" +
			"\n    nondeterministic (goroutines, time.Now, global rand, or map iteration?)."
	}
	t.Errorf("simgo: seed %d: %v\n    trace:  %s\n    replay: %s%s",
		r.Seed, r.Violation, path, ReplayCommand(t, r.Seed), determinism)
}

// WriteTrace writes r's trace as HTML and returns the file path.
func WriteTrace(t testing.TB, r *sim.Result, opt Options) (string, error) {
	opt = opt.withDefaults()
	name := fmt.Sprintf("%s-seed-%d.html", sanitize(t.Name()), r.Seed)
	path := filepath.Join(opt.OutDir, name)
	err := viewer.WriteFile(path, r.Trace, viewer.Options{
		Title:  fmt.Sprintf("%s · seed %d", t.Name(), r.Seed),
		Replay: ReplayCommand(t, r.Seed),
	})
	return path, err
}

// ReplayCommand returns a shell command that reruns exactly this seed.
func ReplayCommand(t testing.TB, seed uint64) string {
	name := t.Name()
	if i := strings.IndexByte(name, '/'); i >= 0 {
		name = name[:i] // subtests: rerun the parent test
	}
	return fmt.Sprintf("SIMGO_SEED=%d go test -run '^%s$' %s", seed, regexp.QuoteMeta(name), packageDir())
}

func envSeed(t testing.TB) (uint64, bool) {
	v := os.Getenv("SIMGO_SEED")
	if v == "" {
		return 0, false
	}
	seed, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		t.Fatalf("simgo: invalid SIMGO_SEED %q: %v", v, err)
	}
	return seed, true
}

// packageDir returns the current package directory relative to the module
// root, like ./examples/lease. go test runs tests in the package directory.
func packageDir() string {
	wd, err := os.Getwd()
	if err != nil {
		return "."
	}
	for dir := wd; ; {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			rel, err := filepath.Rel(dir, wd)
			if err != nil || rel == "." {
				return "."
			}
			return "./" + filepath.ToSlash(rel)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "."
		}
		dir = parent
	}
}

var unsafeChars = regexp.MustCompile(`[^A-Za-z0-9_.-]+`)

func sanitize(s string) string { return unsafeChars.ReplaceAllString(s, "_") }
