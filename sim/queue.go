package sim

import (
	"container/heap"
	"time"
)

type eventKind int

const (
	evDeliver eventKind = iota
	evTimer
	evFault
	evResume
)

// event is something scheduled to happen at a point in virtual time.
type event struct {
	at  time.Duration
	seq uint64 // tie-breaker: events at the same instant run in scheduling order

	kind eventKind
	node NodeID

	// evDeliver
	from  NodeID
	msgID uint64
	msg   Message

	// evTimer
	timer    string
	timerGen uint64

	// evFault
	fault *fault

	// incarnation of the target node when the event was scheduled. Events
	// scheduled before a crash are discarded after it.
	incarnation uint64
}

type eventQueue []*event

func (q eventQueue) Len() int { return len(q) }
func (q eventQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *eventQueue) Push(x any)   { *q = append(*q, x.(*event)) }
func (q *eventQueue) Pop() any {
	old := *q
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*q = old[:n-1]
	return e
}

func (s *Simulator) schedule(e *event) {
	s.seq++
	e.seq = s.seq
	heap.Push(&s.queue, e)
}

func (s *Simulator) next() *event {
	if s.queue.Len() == 0 {
		return nil
	}
	return heap.Pop(&s.queue).(*event)
}
