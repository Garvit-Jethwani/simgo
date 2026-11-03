package sim

import (
	"fmt"
	"math/rand/v2"
	"runtime/debug"
	"time"
)

// Simulator runs one simulation. Create it with Run; it is not reusable.
type Simulator struct {
	cfg   Config
	rng   *rand.Rand // network and nemesis randomness
	now   time.Duration
	seq   uint64
	queue eventQueue

	nodes     []*nodeState
	partition map[NodeID]int // node -> group; nil when no partition is active
	emissions map[string][]Emission

	msgID uint64
	trace *Trace
	steps int

	violation *Violation
}

type nodeState struct {
	id          NodeID
	name        string
	node        Node
	up          bool
	incarnation uint64
	pausedUntil time.Duration
	skew        time.Duration
	disk        *Disk
	timers      map[string]uint64 // name -> generation of the pending timer
	timerGen    uint64
	rng         *rand.Rand
	env         *Env
}

// Run executes a simulation and returns its result. It never panics because
// of node code: a panic inside a node is reported as a violation.
func Run(c Config) *Result {
	cfg := c.withDefaults()
	if cfg.Nodes <= 0 {
		panic("simgo: Config.Nodes must be positive")
	}
	if cfg.NewNode == nil {
		panic("simgo: Config.NewNode is required")
	}

	s := &Simulator{
		cfg:       cfg,
		rng:       rand.New(rand.NewPCG(cfg.Seed, 0x5157_6f00)),
		emissions: map[string][]Emission{},
		trace: &Trace{
			Seed:     cfg.Seed,
			Duration: int64(cfg.Duration),
		},
	}
	for i := 0; i < cfg.Nodes; i++ {
		id := NodeID(i)
		name := fmt.Sprintf("n%d", i)
		if cfg.NodeName != nil {
			name = cfg.NodeName(id)
		}
		ns := &nodeState{id: id, name: name, disk: newDisk()}
		ns.env = &Env{s: s, ns: ns}
		s.nodes = append(s.nodes, ns)
		s.trace.Nodes = append(s.trace.Nodes, TraceNode{ID: id, Name: name})
	}
	return s.run()
}

func (s *Simulator) run() *Result {
	for _, ns := range s.nodes {
		s.boot(ns, KindStart)
		if s.violation != nil {
			return s.result()
		}
	}
	s.scheduleNextFault()

	for s.violation == nil {
		e := s.next()
		if e == nil || e.at > s.cfg.Duration {
			break
		}
		if s.steps >= s.cfg.MaxEvents {
			s.trace.Truncated = true
			break
		}
		s.now = e.at
		s.steps++
		s.handle(e)
		if s.violation == nil {
			s.checkInvariants()
		}
	}
	return s.result()
}

func (s *Simulator) result() *Result {
	s.trace.Violation = s.violation
	return &Result{
		Seed:      s.cfg.Seed,
		Violation: s.violation,
		Trace:     s.trace,
		Steps:     s.steps,
		End:       s.now,
	}
}

func (s *Simulator) handle(e *event) {
	switch e.kind {
	case evDeliver:
		s.deliver(e)
	case evTimer:
		s.fireTimer(e)
	case evFault:
		s.applyFault(e.fault)
	case evResume:
		ns := s.nodes[e.node]
		if ns.pausedUntil == e.at && ns.incarnation == e.incarnation {
			ns.pausedUntil = 0
			s.record(TraceEvent{Kind: KindResume, Node: ns.id, Peer: -1})
		}
	}
}

// paused reports whether ns is frozen at the current instant.
func (s *Simulator) paused(ns *nodeState) bool { return ns.pausedUntil > s.now }

// defer re-queues an event for when a paused node wakes up. Events keep their
// relative order because they are re-queued in the order they are popped.
func (s *Simulator) deferUntilResume(ns *nodeState, e *event) {
	e.at = ns.pausedUntil
	s.schedule(e)
}

func (s *Simulator) boot(ns *nodeState, kind string) {
	ns.up = true
	ns.timers = map[string]uint64{}
	ns.rng = rand.New(rand.NewPCG(s.cfg.Seed^uint64(ns.id)*0x9e3779b97f4a7c15, ns.incarnation))
	ns.node = s.cfg.NewNode(ns.id)
	idx := s.record(TraceEvent{Kind: kind, Node: ns.id, Peer: -1})
	s.invoke(ns, idx, func() { ns.node.Start(ns.env) })
}

func (s *Simulator) send(from *nodeState, to NodeID, msg Message) {
	if int(to) < 0 || int(to) >= len(s.nodes) {
		s.fail("simulator", fmt.Sprintf("node %s sent to unknown node %d", from.name, to))
		return
	}
	s.msgID++
	id := s.msgID
	raw, typ := encode(msg)
	s.record(TraceEvent{Kind: KindSend, Node: from.id, Peer: to, MsgID: id, MsgType: typ, Msg: raw})

	net := s.cfg.Network
	if net.DropRate > 0 && s.rng.Float64() < net.DropRate {
		s.record(TraceEvent{Kind: KindDrop, Node: from.id, Peer: to, MsgID: id, MsgType: typ, Detail: "lost in network"})
		return
	}
	copies := 1
	if net.DupRate > 0 && s.rng.Float64() < net.DupRate {
		copies = 2
	}
	for i := 0; i < copies; i++ {
		s.schedule(&event{
			at:    s.now + s.latency(),
			kind:  evDeliver,
			node:  to,
			from:  from.id,
			msgID: id,
			msg:   msg,
		})
	}
}

func (s *Simulator) latency() time.Duration {
	lo, hi := s.cfg.Network.MinLatency, s.cfg.Network.MaxLatency
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(s.rng.Int64N(int64(hi-lo)+1))
}

func (s *Simulator) partitioned(a, b NodeID) bool {
	if s.partition == nil {
		return false
	}
	return s.partition[a] != s.partition[b]
}

func (s *Simulator) deliver(e *event) {
	ns := s.nodes[e.node]
	_, typ := encode(e.msg)
	switch {
	case !ns.up:
		s.record(TraceEvent{Kind: KindDrop, Node: e.from, Peer: e.node, MsgID: e.msgID, MsgType: typ, Detail: "receiver is down"})
		return
	case s.partitioned(e.from, e.node):
		s.record(TraceEvent{Kind: KindDrop, Node: e.from, Peer: e.node, MsgID: e.msgID, MsgType: typ, Detail: "network partition"})
		return
	case s.paused(ns):
		s.deferUntilResume(ns, e)
		return
	}
	raw, _ := encode(e.msg)
	idx := s.record(TraceEvent{Kind: KindDeliver, Node: e.node, Peer: e.from, MsgID: e.msgID, MsgType: typ, Msg: raw})
	s.invoke(ns, idx, func() { ns.node.Receive(ns.env, e.from, e.msg) })
}

func (s *Simulator) fireTimer(e *event) {
	ns := s.nodes[e.node]
	if !ns.up || ns.incarnation != e.incarnation || ns.timers[e.timer] != e.timerGen {
		return // cancelled, replaced, or the node crashed since it was set
	}
	if s.paused(ns) {
		s.deferUntilResume(ns, e)
		return
	}
	delete(ns.timers, e.timer)
	idx := s.record(TraceEvent{Kind: KindTimer, Node: ns.id, Peer: -1, Detail: e.timer})
	s.invoke(ns, idx, func() { ns.node.Timer(ns.env, e.timer) })
}

// invoke runs node code, converts panics into violations, and attaches the
// node's state snapshot to the trace event at idx.
func (s *Simulator) invoke(ns *nodeState, idx int, fn func()) {
	func() {
		defer func() {
			if r := recover(); r != nil {
				s.fail("node panic", fmt.Sprintf("node %s panicked: %v\n%s", ns.name, r, debug.Stack()))
			}
		}()
		fn()
	}()
	if snap, ok := ns.node.(Snapshotter); ok && ns.up {
		func() {
			defer func() {
				if r := recover(); r != nil {
					s.fail("node panic", fmt.Sprintf("node %s panicked in Snapshot: %v", ns.name, r))
				}
			}()
			raw, _ := encode(snap.Snapshot())
			s.trace.Events[idx].State = raw
		}()
	}
}

func (s *Simulator) checkInvariants() {
	w := &World{s: s}
	for _, inv := range s.cfg.Invariants {
		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("invariant panicked: %v", r)
				}
			}()
			err = inv.Check(w)
		}()
		if err != nil {
			s.fail(inv.Name, err.Error())
			return
		}
	}
}

func (s *Simulator) fail(name, msg string) {
	if s.violation != nil {
		return
	}
	idx := s.record(TraceEvent{Kind: KindViolation, Node: -1, Peer: -1, Detail: name + ": " + msg})
	s.violation = &Violation{Invariant: name, Message: msg, At: int64(s.now), Event: idx}
}
