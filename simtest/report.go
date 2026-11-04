package simtest

import (
	"encoding/json"
	"os"
	"sync"
	"testing"

	"github.com/Garvit-Jethwani/simgo/sim"
)

// RunRecord is one line of the SIMGO_REPORT file: the outcome of one
// exploration (or one replayed seed). Hosted workers read these records
// instead of parsing test output.
type RunRecord struct {
	Test      string          `json:"test"`
	Package   string          `json:"package"`
	FirstSeed uint64          `json:"first_seed"`
	Seeds     int             `json:"seeds"`
	Failures  []FailureRecord `json:"failures"`
}

// FailureRecord describes one failing seed.
type FailureRecord struct {
	Seed        uint64 `json:"seed"`
	Invariant   string `json:"invariant"`
	Message     string `json:"message"`
	AtNs        int64  `json:"at_ns"`
	Fingerprint string `json:"fingerprint"`
	// Trace is the path of the trace HTML file, if one was written (only
	// the first MaxReported failures get one).
	Trace string `json:"trace,omitempty"`
	// Reproduction is how often re-running the seed reproduced it, if that
	// was measured.
	Reproduction *Reproduction `json:"reproduction,omitempty"`
}

// Reproduction counts replays of a failing seed.
type Reproduction struct {
	Replays       int `json:"replays"`
	Exact         int `json:"exact"`
	SameViolation int `json:"same_violation"`
}

func failureRecord(r *sim.Result, trace string, rep *Reproduction) FailureRecord {
	return FailureRecord{
		Seed:         r.Seed,
		Invariant:    r.Violation.Invariant,
		Message:      r.Violation.Message,
		AtNs:         r.Violation.At,
		Fingerprint:  r.Fingerprint(),
		Trace:        trace,
		Reproduction: rep,
	}
}

func sameViolation(a, b *sim.Result) int {
	if b.Failed() && b.Violation.Invariant == a.Violation.Invariant {
		return 1
	}
	return 0
}

var reportMu sync.Mutex

// AppendRecord appends rec to the SIMGO_REPORT file, if that variable is
// set. Runners built on simtest (such as x/simproc) use it too.
func AppendRecord(rec RunRecord) error {
	path := os.Getenv("SIMGO_REPORT")
	if path == "" {
		return nil
	}
	if rec.Failures == nil {
		rec.Failures = []FailureRecord{}
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	reportMu.Lock()
	defer reportMu.Unlock()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// ReadRecords reads every record from a SIMGO_REPORT file.
func ReadRecords(path string) ([]RunRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []RunRecord
	dec := json.NewDecoder(f)
	for dec.More() {
		var r RunRecord
		if err := dec.Decode(&r); err != nil {
			return out, err
		}
		out = append(out, r)
	}
	return out, nil
}

// PackageDir returns the current package directory relative to the module
// root, such as ./examples/lease.
func PackageDir() string { return packageDir() }

func appendRecord(t testing.TB, rec RunRecord) {
	if err := AppendRecord(rec); err != nil {
		t.Logf("simgo: could not append to SIMGO_REPORT: %v", err)
	}
}
