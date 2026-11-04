package sim

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// pinger sends pings to random peers on a random timer and answers pongs.
type pinger struct {
	Sent, Received int
}

type Ping struct{ N int }
type Pong struct{ N int }

func (p *pinger) Start(env *Env) { env.SetTimer("tick", 10*time.Millisecond) }

func (p *pinger) Timer(env *Env, name string) {
	peers := env.Nodes()
	to := peers[env.Rand().IntN(len(peers))]
	if to != env.ID() {
		p.Sent++
		env.Send(to, Ping{N: p.Sent})
	}
	env.SetTimer("tick", time.Duration(10+env.Rand().IntN(90))*time.Millisecond)
}

func (p *pinger) Receive(env *Env, from NodeID, msg Message) {
	p.Received++
	if ping, ok := msg.(Ping); ok {
		env.Send(from, Pong{N: ping.N})
	}
}

func (p *pinger) Snapshot() any { return p }

func chaosConfig(seed uint64) Config {
	return Config{
		Seed:     seed,
		Nodes:    5,
		NewNode:  func(NodeID) Node { return &pinger{} },
		Duration: 20 * time.Second,
		Network: NetworkConfig{
			MinLatency: time.Millisecond,
			MaxLatency: 50 * time.Millisecond,
			DropRate:   0.05,
			DupRate:    0.05,
		},
		Faults: FaultConfig{
			Interval:   500 * time.Millisecond,
			Partitions: true,
			Crashes:    true,
			Pauses:     true,
			ClockSkew:  true,
			MaxPause:   time.Second,
		},
	}
}

func count(tr *Trace, kind string) int {
	n := 0
	for _, e := range tr.Events {
		if e.Kind == kind {
			n++
		}
	}
	return n
}

func TestSameSeedSameTrace(t *testing.T) {
	for seed := uint64(1); seed <= 20; seed++ {
		a, b := Run(chaosConfig(seed)), Run(chaosConfig(seed))
		if a.Fingerprint() != b.Fingerprint() {
			t.Fatalf("seed %d: two runs produced different traces", seed)
		}
	}
}

func TestDifferentSeedsDifferentTraces(t *testing.T) {
	seen := map[string]uint64{}
	for seed := uint64(1); seed <= 20; seed++ {
		fp := Run(chaosConfig(seed)).Fingerprint()
		if prev, ok := seen[fp]; ok {
			t.Fatalf("seeds %d and %d produced identical traces", prev, seed)
		}
		seen[fp] = seed
	}
}

func TestChaosRunExercisesEveryFault(t *testing.T) {
	r := Run(chaosConfig(7))
	if r.Failed() {
		t.Fatalf("unexpected violation: %v", r.Violation)
	}
	for _, kind := range []string{KindSend, KindDeliver, KindDrop, KindCrash, KindRestart, KindPause, KindResume, KindPartition, KindHeal, KindSkew} {
		if count(r.Trace, kind) == 0 {
			t.Errorf("no %q events in a 20s chaos run", kind)
		}
	}
	if r.End > 20*time.Second {
		t.Errorf("simulation ran past its duration: %v", r.End)
	}
}

func TestLatencyWithinBounds(t *testing.T) {
	cfg := chaosConfig(3)
	cfg.Faults = FaultConfig{}
	cfg.Network = NetworkConfig{MinLatency: 5 * time.Millisecond, MaxLatency: 9 * time.Millisecond}
	r := Run(cfg)
	sentAt := map[uint64]int64{}
	for _, e := range r.Trace.Events {
		switch e.Kind {
		case KindSend:
			sentAt[e.MsgID] = e.At
		case KindDeliver:
			lat := time.Duration(e.At - sentAt[e.MsgID])
			if lat < 5*time.Millisecond || lat > 9*time.Millisecond {
				t.Fatalf("message %d delivered after %v, want 5ms..9ms", e.MsgID, lat)
			}
		}
	}
}

func TestDropAndDuplicateRates(t *testing.T) {
	cfg := chaosConfig(3)
	cfg.Faults = FaultConfig{}

	cfg.Network = NetworkConfig{MaxLatency: time.Millisecond, DropRate: 1}
	if r := Run(cfg); count(r.Trace, KindDeliver) != 0 || count(r.Trace, KindDrop) != count(r.Trace, KindSend) {
		t.Errorf("DropRate=1: every send should be dropped")
	}

	cfg.Network = NetworkConfig{MaxLatency: time.Millisecond, DupRate: 1}
	r := Run(cfg)
	deliveries := map[uint64]int{}
	for _, e := range r.Trace.Events {
		if e.Kind == KindDeliver {
			deliveries[e.MsgID]++
		}
	}
	for id, n := range deliveries {
		if n != 2 {
			t.Fatalf("DupRate=1: message %d delivered %d times, want 2", id, n)
		}
	}
}

// TestFaultSemantics replays chaos runs and checks the trace obeys the rules
// of each fault: nothing happens on a crashed or paused node, and nothing
// crosses an active partition.
func TestFaultSemantics(t *testing.T) {
	for seed := uint64(1); seed <= 30; seed++ {
		r := Run(chaosConfig(seed))
		down := map[NodeID]bool{}
		paused := map[NodeID]bool{}
		group := map[NodeID]int(nil)
		for _, e := range r.Trace.Events {
			switch e.Kind {
			case KindCrash:
				down[e.Node] = true
				paused[e.Node] = false
			case KindRestart:
				down[e.Node] = false
			case KindPause:
				paused[e.Node] = true
			case KindResume:
				paused[e.Node] = false
			case KindPartition:
				group = map[NodeID]int{}
				for g, ids := range e.Groups {
					for _, id := range ids {
						group[id] = g
					}
				}
			case KindHeal:
				group = nil
			case KindDeliver, KindTimer, KindSend:
				if down[e.Node] {
					t.Fatalf("seed %d event %d: %s on crashed node %d", seed, e.Index, e.Kind, e.Node)
				}
				if paused[e.Node] {
					t.Fatalf("seed %d event %d: %s on paused node %d", seed, e.Index, e.Kind, e.Node)
				}
				if e.Kind == KindDeliver && group != nil && group[e.Node] != group[e.Peer] {
					t.Fatalf("seed %d event %d: message crossed a partition", seed, e.Index)
				}
			}
		}
	}
}

func TestInvariantViolationStopsRun(t *testing.T) {
	cfg := chaosConfig(1)
	cfg.Invariants = []Invariant{{
		Name: "fewer than 10 sends",
		Check: func(w *World) error {
			n := 0
			for _, id := range w.NodeIDs() {
				if p, ok := w.Node(id).(*pinger); ok {
					n += p.Sent
				}
			}
			if n >= 10 {
				return errors.New("too many sends")
			}
			return nil
		},
	}}
	r := Run(cfg)
	if !r.Failed() || r.Violation.Invariant != "fewer than 10 sends" {
		t.Fatalf("expected a violation, got %+v", r.Violation)
	}
	last := r.Trace.Events[len(r.Trace.Events)-1]
	if last.Kind != KindViolation || last.Index != r.Violation.Event {
		t.Fatalf("trace should end with the violation event, ends with %q", last.Kind)
	}
}

type panicker struct{}

func (panicker) Start(env *Env)                           { env.SetTimer("boom", time.Second) }
func (panicker) Receive(env *Env, from NodeID, m Message) {}
func (panicker) Timer(env *Env, name string)              { var m map[string]int; m["x"] = 1 }

func TestNodePanicIsAViolation(t *testing.T) {
	r := Run(Config{Nodes: 1, NewNode: func(NodeID) Node { return panicker{} }})
	if !r.Failed() || r.Violation.Invariant != "node panic" {
		t.Fatalf("expected a node panic violation, got %+v", r.Violation)
	}
	if !strings.Contains(r.Violation.Message, "nil map") {
		t.Errorf("violation should carry the panic message, got %q", r.Violation.Message)
	}
	if r.End != time.Second {
		t.Errorf("panic happened at %v, want 1s", r.End)
	}
}

func TestProtectedNodesAreNeverCrashedOrPaused(t *testing.T) {
	cfg := chaosConfig(5)
	cfg.Faults.Protected = []NodeID{0}
	for seed := uint64(1); seed <= 20; seed++ {
		cfg.Seed = seed
		for _, e := range Run(cfg).Trace.Events {
			if e.Node == 0 && (e.Kind == KindCrash || e.Kind == KindPause || e.Kind == KindSkew) {
				t.Fatalf("seed %d: protected node received fault %q", seed, e.Kind)
			}
		}
	}
}

func TestCrashLosesUnsyncedWrites(t *testing.T) {
	cfg := Config{
		Seed:  1,
		Nodes: 2,
		NewNode: func(NodeID) Node {
			return &writer{}
		},
		Duration: 30 * time.Second,
		Faults:   FaultConfig{Interval: time.Second, Crashes: true},
	}
	sawLoss := false
	for seed := uint64(1); seed <= 10 && !sawLoss; seed++ {
		cfg.Seed = seed
		for _, e := range Run(cfg).Trace.Events {
			if e.Kind == KindCrash && !strings.HasPrefix(e.Detail, "lost 0 ") {
				sawLoss = true
			}
		}
	}
	if !sawLoss {
		t.Fatal("no crash ever lost an unsynced write")
	}
}

// writer writes to disk every 10ms but only syncs every 100ms.
type writer struct{ n int }

func (w *writer) Start(env *Env)                           { env.SetTimer("write", 10*time.Millisecond) }
func (w *writer) Receive(env *Env, from NodeID, m Message) {}
func (w *writer) Timer(env *Env, name string) {
	w.n++
	env.Disk().Write("counter", []byte{byte(w.n)})
	if w.n%10 == 0 {
		env.Disk().Sync()
	}
	env.SetTimer("write", 10*time.Millisecond)
}

func BenchmarkChaosRun(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Run(chaosConfig(uint64(i)))
	}
}

type namedMsg struct{ Kind string }

func (m namedMsg) SimName() string { return m.Kind }

type namer struct{}

func (namer) Start(env *Env) {
	if env.ID() == 0 {
		env.Send(1, namedMsg{Kind: "MsgVote"})
	}
}
func (namer) Receive(env *Env, from NodeID, m Message) {}
func (namer) Timer(env *Env, name string)              {}

func TestMessagesCanNameThemselves(t *testing.T) {
	r := Run(Config{Nodes: 2, NewNode: func(NodeID) Node { return namer{} }, Duration: time.Second})
	for _, e := range r.Trace.Events {
		if e.Kind == KindSend && e.MsgType != "MsgVote" {
			t.Fatalf("send recorded as %q, want MsgVote", e.MsgType)
		}
	}
}

// A panic must not make the trace nondeterministic: stack traces contain
// pointer values, goroutine IDs, and the frames of whoever called Run.
func TestNodePanicIsDeterministic(t *testing.T) {
	cfg := Config{Nodes: 1, NewNode: func(NodeID) Node { return panicker{} }}
	direct := Run(cfg)
	viaGoroutine := make(chan *Result)
	go func() {
		func() { viaGoroutine <- Run(cfg) }() // a different call path
	}()
	other := <-viaGoroutine
	if direct.Fingerprint() != other.Fingerprint() {
		t.Fatalf("panic traces differ:\n%s\n---\n%s", direct.Violation.Message, other.Violation.Message)
	}
	msg := direct.Violation.Message
	if !strings.Contains(msg, "panicker.Timer") {
		t.Errorf("panic report should include the node's frame:\n%s", msg)
	}
	if strings.Contains(msg, "testing.tRunner") || strings.Contains(msg, "runtime/debug") {
		t.Errorf("panic report should not include frames outside the node:\n%s", msg)
	}
}
