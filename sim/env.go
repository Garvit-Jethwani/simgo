package sim

import (
	"fmt"
	"math/rand/v2"
	"time"
)

// Env is a node's window into the simulated world. All time, I/O and
// randomness a node uses must come from here.
type Env struct {
	s  *Simulator
	ns *nodeState
}

// ID returns this node's ID.
func (e *Env) ID() NodeID { return e.ns.id }

// Nodes returns the IDs of all nodes in the simulation, in order.
func (e *Env) Nodes() []NodeID {
	ids := make([]NodeID, len(e.s.nodes))
	for i := range ids {
		ids[i] = NodeID(i)
	}
	return ids
}

// Now returns this node's local clock. It starts at zero and advances with
// virtual time, but clock-skew faults can shift it, so different nodes may
// disagree about the current time.
func (e *Env) Now() time.Duration { return e.s.now + e.ns.skew }

// Send sends msg to another node over the simulated network. Delivery is not
// guaranteed: the message may be delayed, reordered, duplicated, or lost.
func (e *Env) Send(to NodeID, msg Message) {
	e.s.send(e.ns, to, msg)
}

// SetTimer arranges for Timer(name) to be called after the given delay.
// Setting a timer that is already pending replaces it.
func (e *Env) SetTimer(name string, after time.Duration) {
	if after < 0 {
		after = 0
	}
	e.ns.timerGen++
	gen := e.ns.timerGen
	e.ns.timers[name] = gen
	e.s.schedule(&event{
		at:          e.s.now + after,
		kind:        evTimer,
		node:        e.ns.id,
		timer:       name,
		timerGen:    gen,
		incarnation: e.ns.incarnation,
	})
}

// CancelTimer cancels a pending timer. It is a no-op if none is pending.
func (e *Env) CancelTimer(name string) { delete(e.ns.timers, name) }

// Rand returns this node's deterministic random source.
func (e *Env) Rand() *rand.Rand { return e.ns.rng }

// Disk returns this node's simulated durable storage.
func (e *Env) Disk() *Disk { return e.ns.disk }

// Logf records a log line in the trace.
func (e *Env) Logf(format string, args ...any) {
	e.s.record(TraceEvent{Kind: KindLog, Node: e.ns.id, Peer: -1, Detail: fmt.Sprintf(format, args...)})
}

// Emit records an externally visible action, such as charging a card or
// acknowledging a write to a client. Invariants inspect emissions through
// World.Emitted to decide whether the system behaved correctly.
func (e *Env) Emit(kind string, value any) {
	raw, _ := encode(value)
	e.s.record(TraceEvent{Kind: KindEmit, Node: e.ns.id, Peer: -1, Detail: kind, Msg: raw})
	e.s.emissions[kind] = append(e.s.emissions[kind], Emission{
		At: e.s.now, Node: e.ns.id, Kind: kind, Value: value,
	})
}

// Emission is an externally visible action recorded with Env.Emit.
type Emission struct {
	At    time.Duration
	Node  NodeID
	Kind  string
	Value any
}

// World gives invariants read-only access to the simulation state.
type World struct{ s *Simulator }

// Now returns the global virtual time.
func (w *World) Now() time.Duration { return w.s.now }

// NodeIDs returns all node IDs in order.
func (w *World) NodeIDs() []NodeID {
	ids := make([]NodeID, len(w.s.nodes))
	for i := range ids {
		ids[i] = NodeID(i)
	}
	return ids
}

// Node returns the live node with the given ID, or nil if it is crashed.
func (w *World) Node(id NodeID) Node {
	ns := w.s.nodes[id]
	if !ns.up {
		return nil
	}
	return ns.node
}

// IsUp reports whether a node is running (it may still be paused).
func (w *World) IsUp(id NodeID) bool { return w.s.nodes[id].up }

// Disk returns a node's disk, which survives crashes.
func (w *World) Disk(id NodeID) *Disk { return w.s.nodes[id].disk }

// Emitted returns all emissions of the given kind so far, in order.
func (w *World) Emitted(kind string) []Emission { return w.s.emissions[kind] }
