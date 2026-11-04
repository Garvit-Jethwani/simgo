package wal

import (
	"testing"

	"github.com/Garvit-Jethwani/simgo/sim"
	"github.com/Garvit-Jethwani/simgo/simtest"
)

func TestAckThenSyncLosesWrites(t *testing.T) {
	failures := simtest.Search(Config(AckThenSync), simtest.Options{Seeds: 100})
	if len(failures) == 0 {
		t.Fatal("expected SimGo to find a lost acknowledged write")
	}
	r := failures[0]
	path, err := simtest.WriteTrace(t, r, simtest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("lost acknowledged writes in %d of 100 seeds; first: seed %d\n    %v\n    trace: %s",
		len(failures), r.Seed, r.Violation, path)
}

func TestSyncThenAckIsDurable(t *testing.T) {
	simtest.Explore(t, Config(SyncThenAck), simtest.Options{Seeds: 300})
}

func TestSyncThenAckMakesProgress(t *testing.T) {
	cfg := Config(SyncThenAck)
	cfg.Seed = 1
	r := sim.Run(cfg)
	acks := 0
	for _, e := range r.Trace.Events {
		if e.Kind == sim.KindEmit && e.Detail == EmitAck {
			acks++
		}
	}
	if acks < 500 {
		t.Fatalf("only %d writes acknowledged in 5s", acks)
	}
}
