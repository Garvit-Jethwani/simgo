package simproc

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Garvit-Jethwani/simgo/sim"
)

// counterServer keeps a counter on disk. Each "inc" line increments it,
// writes it, syncs if syncEvery divides it, and replies with the new value.
func counterServer(syncEvery int) Main {
	return func(ctx context.Context, p *Process) error {
		d := p.Disk()
		n := 0
		if v, ok, _ := d.Read("counter"); ok {
			n, _ = strconv.Atoi(string(v))
		}
		p.Emit("recovered", n)
		ln, err := p.Listen(7000)
		if err != nil {
			return err
		}
		var mu sync.Mutex
		go func() {
			<-ctx.Done()
			ln.Close()
		}()
		for {
			c, err := ln.Accept()
			if err != nil {
				return nil
			}
			go func() {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					if _, err := r.ReadString('\n'); err != nil {
						return
					}
					mu.Lock()
					n++
					v := n
					d.Write("counter", []byte(strconv.Itoa(v)))
					if v%syncEvery == 0 {
						d.Sync()
					}
					mu.Unlock()
					fmt.Fprintf(c, "%d\n", v)
				}
			}()
		}
	}
}

// counterClient increments every 50ms and emits each acknowledged value.
func counterClient(ctx context.Context, p *Process) error {
	for ctx.Err() == nil {
		c, err := p.Dial(ctx, "server:7000")
		if err != nil {
			if !sleepCtx(ctx, 100*time.Millisecond) {
				return nil
			}
			continue
		}
		r := bufio.NewReader(c)
		for {
			if !sleepCtx(ctx, 50*time.Millisecond) {
				c.Close()
				return nil
			}
			fmt.Fprintf(c, "inc\n")
			line, err := r.ReadString('\n')
			if err != nil {
				c.Close()
				break
			}
			v, _ := strconv.Atoi(strings.TrimSpace(line))
			p.Emit("acked", v)
		}
	}
	return nil
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// monotonic: after a restart the server must not hand out a value it already
// acknowledged (it must not lose acknowledged increments).
var monotonic = Invariant{
	Name: "acknowledged counter values are never reused",
	Check: func(h *History) error {
		seen := map[int]bool{}
		for _, e := range h.Emitted("acked") {
			v := e.Value.(int)
			if seen[v] {
				return fmt.Errorf("value %d acknowledged twice", v)
			}
			seen[v] = true
		}
		return nil
	},
}

func counterConfig(syncEvery int, faults Faults) Config {
	return Config{
		Nodes: []Node{
			{Name: "server", Main: counterServer(syncEvery)},
			{Name: "client", Main: counterClient, Protected: true},
		},
		Duration:   10 * time.Second,
		Faults:     faults,
		Invariants: []Invariant{monotonic},
	}
}

func run(t *testing.T, cfg Config) *sim.Result {
	t.Helper()
	var r *sim.Result
	synctest.Test(t, func(*testing.T) { r = Run(cfg) })
	return r
}

func count(r *sim.Result, kind string) int {
	n := 0
	for _, e := range r.Trace.Events {
		if e.Kind == kind {
			n++
		}
	}
	return n
}

func TestRunWithoutFaults(t *testing.T) {
	cfg := counterConfig(1, Faults{})
	cfg.Seed = 1
	r := run(t, cfg)
	if r.Failed() {
		t.Fatal(r.Violation)
	}
	acks := 0
	for _, e := range r.Trace.Events {
		if e.Kind == sim.KindEmit && e.Detail == "acked" {
			acks++
		}
	}
	if acks < 150 {
		t.Fatalf("only %d acknowledged increments in 10s", acks)
	}
	// Every delivery is linked to an earlier send.
	sent := map[uint64]bool{}
	for _, e := range r.Trace.Events {
		switch e.Kind {
		case sim.KindSend:
			sent[e.MsgID] = true
		case sim.KindDeliver:
			if !sent[e.MsgID] {
				t.Fatalf("event %d: delivery of message %d without a send", e.Index, e.MsgID)
			}
		}
	}
	if len(r.Trace.Nodes) != 2 || r.Trace.Nodes[0].Name != "server" {
		t.Fatalf("unexpected trace nodes: %+v", r.Trace.Nodes)
	}
}

func TestCrashLosesUnsyncedStateAndRestarts(t *testing.T) {
	// syncEvery=1000: effectively never fsyncs, so a crash loses increments
	// and the restarted server reuses acknowledged values.
	cfg := counterConfig(1000, Faults{Interval: 2 * time.Second, Crashes: true, MaxDowntime: time.Second})
	found := false
	for seed := uint64(1); seed <= 10 && !found; seed++ {
		cfg.Seed = seed
		r := run(t, cfg)
		if count(r, sim.KindCrash) == 0 {
			continue
		}
		if count(r, sim.KindRestart) == 0 && !r.Failed() {
			t.Fatalf("seed %d: crash without restart", seed)
		}
		if r.Failed() && r.Violation.Invariant == monotonic.Name {
			found = true
		}
	}
	if !found {
		t.Fatal("expected a reused counter value after a crash without fsync")
	}
}

func TestSyncedCounterSurvivesCrashes(t *testing.T) {
	cfg := counterConfig(1, Faults{Interval: time.Second, Crashes: true, Partitions: true, MaxDowntime: time.Second})
	for seed := uint64(1); seed <= 10; seed++ {
		cfg.Seed = seed
		r := run(t, cfg)
		if r.Failed() {
			t.Fatalf("seed %d: %v", seed, r.Violation)
		}
	}
}

func TestViolationStopsTheRunEarly(t *testing.T) {
	cfg := counterConfig(1, Faults{})
	cfg.Seed = 1
	cfg.Invariants = []Invariant{{
		Name: "fewer than 5 acks",
		Check: func(h *History) error {
			if len(h.Emitted("acked")) >= 5 {
				return errors.New("5 acks")
			}
			return nil
		},
	}}
	r := run(t, cfg)
	if !r.Failed() {
		t.Fatal("expected a violation")
	}
	if r.End > time.Second {
		t.Fatalf("run continued until %v after the violation", r.End)
	}
	last := r.Trace.Events[len(r.Trace.Events)-1]
	if last.Kind != sim.KindViolation || r.Violation.Event != last.Index {
		t.Fatalf("trace should end with the violation, ends with %q", last.Kind)
	}
}

func TestNodeErrorIsAViolation(t *testing.T) {
	cfg := Config{
		Seed: 1,
		Nodes: []Node{{Name: "broken", Main: func(ctx context.Context, p *Process) error {
			time.Sleep(time.Second)
			return errors.New("disk on fire")
		}}},
		Duration: 5 * time.Second,
	}
	r := run(t, cfg)
	if !r.Failed() || !strings.Contains(r.Violation.Message, "disk on fire") {
		t.Fatalf("expected node error violation, got %v", r.Violation)
	}
}

func TestStaleIncarnationCannotActAfterCrash(t *testing.T) {
	stale := make(chan *Process, 1)
	cfg := Config{
		Seed: 1,
		Nodes: []Node{{Name: "n", Main: func(ctx context.Context, p *Process) error {
			if p.Incarnation() == 1 {
				stale <- p
			}
			<-ctx.Done()
			return nil
		}}},
		Duration: 20 * time.Second,
		Faults:   Faults{Interval: 2 * time.Second, Crashes: true, MaxDowntime: time.Second},
	}
	synctest.Test(t, func(t *testing.T) {
		done := make(chan *sim.Result)
		go func() { done <- Run(cfg) }()
		p := <-stale
		r := <-done
		if count(r, sim.KindCrash) == 0 {
			t.Skip("no crash in this seed")
		}
		if _, err := p.Listen(1); !errors.Is(err, ErrCrashed) {
			t.Errorf("Listen from a crashed incarnation: %v", err)
		}
		if err := p.Disk().Write("k", nil); !errors.Is(err, ErrCrashed) {
			t.Errorf("disk write from a crashed incarnation: %v", err)
		}
	})
}

func TestPartitionsAreInjectedAndHealed(t *testing.T) {
	cfg := counterConfig(1, Faults{Interval: time.Second, Partitions: true, MaxDowntime: time.Second})
	cfg.Seed = 3
	r := run(t, cfg)
	if count(r, sim.KindPartition) == 0 || count(r, sim.KindHeal) == 0 {
		t.Fatalf("partitions=%d heals=%d", count(r, sim.KindPartition), count(r, sim.KindHeal))
	}
}

func TestSameSeedSameTrace(t *testing.T) {
	cfg := counterConfig(3, Faults{Interval: time.Second, Crashes: true, Partitions: true, MaxDowntime: time.Second})
	for seed := uint64(1); seed <= 20; seed++ {
		cfg.Seed = seed
		a, b := run(t, cfg), run(t, cfg)
		if a.Fingerprint() != b.Fingerprint() {
			t.Fatalf("seed %d: two runs produced different traces", seed)
		}
	}
}

func TestReproductionReport(t *testing.T) {
	cfg := counterConfig(1000, Faults{Interval: 2 * time.Second, Crashes: true, MaxDowntime: time.Second})
	failures := Search(t, cfg, Options{})
	if len(failures) == 0 {
		t.Fatal("expected failures with fsync disabled")
	}
	rep := measure(t, cfg, failures[0].Result, 3)
	if rep.Exact != 3 || rep.SameViolation != 3 {
		t.Fatalf("deterministic failure reproduced %d/%d exactly, %d/%d same violation", rep.Exact, rep.Replays, rep.SameViolation, rep.Replays)
	}
	if !strings.Contains(rep.String(), "exactly, 3 of 3 replays") {
		t.Fatalf("unexpected report: %s", rep)
	}
}
