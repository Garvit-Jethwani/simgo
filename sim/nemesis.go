package sim

import (
	"fmt"
	"slices"
	"time"
)

type faultKind int

const (
	faultPartition faultKind = iota
	faultHeal
	faultCrash
	faultRestart
	faultPause
	faultSkew
	faultRandom // pick one of the enabled faults at random
)

type fault struct {
	kind faultKind
	node NodeID
}

// scheduleNextFault queues the next random fault, if fault injection is on.
func (s *Simulator) scheduleNextFault() {
	iv := s.cfg.Faults.Interval
	if iv <= 0 || len(s.enabledFaults()) == 0 {
		return
	}
	// Uniform in [iv/2, 3iv/2]: mean iv, never two faults at the same instant.
	delay := iv/2 + time.Duration(s.rng.Int64N(int64(iv)+1))
	s.schedule(&event{at: s.now + delay, kind: evFault, fault: &fault{kind: faultRandom}})
}

func (s *Simulator) enabledFaults() []faultKind {
	f := s.cfg.Faults
	var kinds []faultKind
	if f.Partitions && len(s.nodes) > 1 {
		kinds = append(kinds, faultPartition)
	}
	if f.Crashes {
		kinds = append(kinds, faultCrash)
	}
	if f.Pauses {
		kinds = append(kinds, faultPause)
	}
	if f.ClockSkew {
		kinds = append(kinds, faultSkew)
	}
	return kinds
}

func (s *Simulator) applyFault(f *fault) {
	switch f.kind {
	case faultRandom:
		kinds := s.enabledFaults()
		kind := kinds[s.rng.IntN(len(kinds))]
		if !s.injectRandom(kind) {
			// The chosen fault had no valid target (for example, every
			// crashable node is already down). Try the others in order.
			for _, k := range kinds {
				if k != kind && s.injectRandom(k) {
					break
				}
			}
		}
		s.scheduleNextFault()
	case faultHeal:
		if s.partition != nil {
			s.partition = nil
			s.record(TraceEvent{Kind: KindHeal, Node: -1, Peer: -1})
		}
	case faultRestart:
		ns := s.nodes[f.node]
		if !ns.up {
			s.boot(ns, KindRestart)
		}
	}
}

// injectRandom applies one fault of the given kind to a random target.
// It returns false if there was no valid target.
func (s *Simulator) injectRandom(kind faultKind) bool {
	f := s.cfg.Faults
	switch kind {
	case faultPartition:
		if s.partition != nil {
			return false
		}
		ids := make([]NodeID, len(s.nodes))
		for i := range ids {
			ids[i] = NodeID(i)
		}
		s.rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
		cut := 1 + s.rng.IntN(len(ids)-1)
		a, b := slices.Clone(ids[:cut]), slices.Clone(ids[cut:])
		slices.Sort(a)
		slices.Sort(b)
		s.partition = map[NodeID]int{}
		for _, id := range b {
			s.partition[id] = 1
		}
		for _, id := range a {
			s.partition[id] = 0
		}
		s.record(TraceEvent{Kind: KindPartition, Node: -1, Peer: -1, Groups: [][]NodeID{a, b}})
		s.schedule(&event{at: s.now + s.downtime(), kind: evFault, fault: &fault{kind: faultHeal}})
		return true

	case faultCrash:
		ns := s.pick(func(ns *nodeState) bool { return ns.up && !s.protected(ns.id) })
		if ns == nil {
			return false
		}
		ns.up = false
		ns.incarnation++
		ns.pausedUntil = 0
		ns.timers = nil
		lost := ns.disk.crash()
		ns.node = nil
		s.record(TraceEvent{Kind: KindCrash, Node: ns.id, Peer: -1, Detail: fmt.Sprintf("lost %d unsynced disk writes", lost)})
		s.schedule(&event{at: s.now + s.downtime(), kind: evFault, fault: &fault{kind: faultRestart, node: ns.id}})
		return true

	case faultPause:
		ns := s.pick(func(ns *nodeState) bool { return ns.up && !s.paused(ns) && !s.protected(ns.id) })
		if ns == nil {
			return false
		}
		d := f.MaxPause/10 + time.Duration(s.rng.Int64N(int64(f.MaxPause-f.MaxPause/10)+1))
		ns.pausedUntil = s.now + d
		s.record(TraceEvent{Kind: KindPause, Node: ns.id, Peer: -1, Detail: fmt.Sprintf("frozen for %v", d)})
		s.schedule(&event{at: ns.pausedUntil, kind: evResume, node: ns.id, incarnation: ns.incarnation})
		return true

	case faultSkew:
		ns := s.pick(func(ns *nodeState) bool { return ns.up && !s.protected(ns.id) })
		if ns == nil {
			return false
		}
		delta := time.Duration(s.rng.Int64N(2*int64(f.MaxSkew)+1)) - f.MaxSkew
		ns.skew += delta
		s.record(TraceEvent{Kind: KindSkew, Node: ns.id, Peer: -1, Detail: fmt.Sprintf("clock moved by %v (now off by %v)", delta, ns.skew)})
		return true
	}
	return false
}

func (s *Simulator) downtime() time.Duration {
	max := s.cfg.Faults.MaxDowntime
	min := max / 5
	return min + time.Duration(s.rng.Int64N(int64(max-min)+1))
}

func (s *Simulator) protected(id NodeID) bool {
	return slices.Contains(s.cfg.Faults.Protected, id)
}

// pick returns a random node satisfying ok, or nil.
func (s *Simulator) pick(ok func(*nodeState) bool) *nodeState {
	var candidates []*nodeState
	for _, ns := range s.nodes {
		if ok(ns) {
			candidates = append(candidates, ns)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	return candidates[s.rng.IntN(len(candidates))]
}
