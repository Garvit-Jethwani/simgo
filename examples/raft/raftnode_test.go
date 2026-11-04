package raftsim

import (
	"testing"

	"github.com/Garvit-Jethwani/simgo/sim"
	"github.com/Garvit-Jethwani/simgo/simtest"
)

// TestCorrectStorageIsSafe: etcd raft, used according to its contract,
// survives crashes, pauses, partitions and message loss.
func TestCorrectStorageIsSafe(t *testing.T) {
	simtest.Explore(t, Config(Options{Storage: SyncEveryReady, DriveElections: true}), simtest.Options{Seeds: 300})
}

// TestCorrectStorageMakesProgress guards against a cluster that is "safe"
// only because it never commits anything.
func TestCorrectStorageMakesProgress(t *testing.T) {
	cfg := Config(Options{Storage: SyncEveryReady, DriveElections: true})
	cfg.Seed = 1
	r := sim.Run(cfg)
	if r.Failed() {
		t.Fatal(r.Violation)
	}
	if n := committed(r); n < 20 {
		t.Fatalf("only %d entries committed in 30s", n)
	}
}

// TestBatchedFsyncBreaksRaft shows SimGo finding the bug: fsyncing on a timer
// instead of before sending lets a crash erase a vote or log entries the node
// already promised to its peers.
func TestBatchedFsyncBreaksRaft(t *testing.T) {
	cfg := Config(Options{Storage: BatchedFsync, DriveElections: true})
	failures := simtest.Search(cfg, simtest.Options{Seeds: 300}) // SIMGO_SEEDS overrides
	if len(failures) == 0 {
		t.Fatal("expected SimGo to find a safety violation with batched fsync")
	}
	byInvariant := map[string]int{}
	for _, r := range failures {
		byInvariant[r.Violation.Invariant]++
	}
	r := failures[0]
	cfg.Seed = r.Seed
	if again := sim.Run(cfg); again.Fingerprint() != r.Fingerprint() {
		t.Fatal("the failing seed did not reproduce exactly")
	}
	path, err := simtest.WriteTrace(t, r, simtest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("violations in %d seeds: %v\n    first: seed %d: %v\n    trace: %s",
		len(failures), byInvariant, r.Seed, r.Violation, path)
}

// TestRaftElectionTimeoutsAreNondeterministic documents why DriveElections
// exists: etcd raft draws election timeouts from crypto/rand, so the same
// seed does not reproduce the same run. SimGo's fingerprint check catches it.
func TestRaftElectionTimeoutsAreNondeterministic(t *testing.T) {
	cfg := Config(Options{Storage: SyncEveryReady, DriveElections: false})
	for seed := uint64(1); seed <= 5; seed++ {
		cfg.Seed = seed
		if sim.Run(cfg).Fingerprint() != sim.Run(cfg).Fingerprint() {
			t.Logf("seed %d: two runs diverged, as expected with crypto/rand timeouts", seed)
			return
		}
	}
	t.Fatal("expected raft's own election timeouts to make runs irreproducible")
}

// TestDrivenElectionsAreDeterministic is the other half: with elections
// driven by SimGo, every seed reproduces exactly.
func TestDrivenElectionsAreDeterministic(t *testing.T) {
	cfg := Config(Options{Storage: SyncEveryReady, DriveElections: true})
	for seed := uint64(1); seed <= 10; seed++ {
		cfg.Seed = seed
		if sim.Run(cfg).Fingerprint() != sim.Run(cfg).Fingerprint() {
			t.Fatalf("seed %d did not reproduce", seed)
		}
	}
}

// The two tests below pin seeds found by searching 3,000 seeds (see
// TestBatchedFsyncBreaksRaft). They show the silent failures: raft itself
// cannot detect them. Seeds are exact replays, but changes to the simulator,
// the fault model or the raft version can change which seeds fail; if that
// happens, search again with:
//
//	SIMGO_SEEDS=3000 go test -run TestBatchedFsyncBreaksRaft -v .
func TestBatchedFsyncElectsTwoLeaders(t *testing.T) {
	expectViolation(t, 404, ElectionSafety.Name)
}

func TestBatchedFsyncLosesCommittedData(t *testing.T) {
	expectViolation(t, 1251, StateMachineSafety.Name)
}

func expectViolation(t *testing.T, seed uint64, invariant string) {
	t.Helper()
	cfg := Config(Options{Storage: BatchedFsync, DriveElections: true})
	cfg.Seed = seed
	r := sim.Run(cfg)
	if !r.Failed() || r.Violation.Invariant != invariant {
		t.Fatalf("seed %d: expected %q, got %v", seed, invariant, r.Violation)
	}
	path, err := simtest.WriteTrace(t, r, simtest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("seed %d: %v\n    trace: %s", seed, r.Violation, path)
}
