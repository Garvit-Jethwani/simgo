package sim

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

// Trace is the complete, replayable record of one simulation run. It is what
// the time-travel viewer renders.
type Trace struct {
	Seed      uint64       `json:"seed"`
	Duration  int64        `json:"duration"` // configured virtual run length, ns
	Nodes     []TraceNode  `json:"nodes"`
	Events    []TraceEvent `json:"events"`
	Violation *Violation   `json:"violation,omitempty"`
	Truncated bool         `json:"truncated,omitempty"`
}

// TraceNode describes a node in the trace.
type TraceNode struct {
	ID   NodeID `json:"id"`
	Name string `json:"name"`
}

// Event kinds recorded in a trace.
const (
	KindStart     = "start"
	KindSend      = "send"
	KindDeliver   = "deliver"
	KindDrop      = "drop"
	KindTimer     = "timer"
	KindCrash     = "crash"
	KindRestart   = "restart"
	KindPause     = "pause"
	KindResume    = "resume"
	KindPartition = "partition"
	KindHeal      = "heal"
	KindSkew      = "skew"
	KindLog       = "log"
	KindEmit      = "emit"
	KindViolation = "violation"
)

// TraceEvent is one step in a trace.
type TraceEvent struct {
	Index   int             `json:"i"`
	At      int64           `json:"t"`           // global virtual time, ns
	Kind    string          `json:"k"`           // one of the Kind* constants
	Node    NodeID          `json:"n"`           // node the event happened on, -1 for global events
	Peer    NodeID          `json:"p"`           // other node for send/deliver/drop, -1 otherwise
	MsgID   uint64          `json:"m,omitempty"` // links send, deliver and drop events
	MsgType string          `json:"mt,omitempty"`
	Msg     json.RawMessage `json:"msg,omitempty"`
	Detail  string          `json:"d,omitempty"`
	Groups  [][]NodeID      `json:"g,omitempty"` // partition groups
	State   json.RawMessage `json:"s,omitempty"` // node snapshot after handling this event
}

// Violation describes a broken invariant (or a node panic).
type Violation struct {
	Invariant string `json:"invariant"`
	Message   string `json:"message"`
	At        int64  `json:"at"`    // virtual time, ns
	Event     int    `json:"event"` // index of the violation event in the trace
}

func (v *Violation) Error() string {
	return fmt.Sprintf("invariant %q violated at %v: %s", v.Invariant, time.Duration(v.At), v.Message)
}

// Result is the outcome of a simulation run.
type Result struct {
	Seed      uint64
	Violation *Violation
	Trace     *Trace
	Steps     int
	End       time.Duration
}

// Failed reports whether an invariant was violated.
func (r *Result) Failed() bool { return r.Violation != nil }

// Fingerprint is a hash of the full trace. Two runs with the same seed and
// the same code must have the same fingerprint; if they don't, something in
// the code under test is nondeterministic.
func (r *Result) Fingerprint() string {
	b, err := json.Marshal(r.Trace)
	if err != nil {
		panic(fmt.Sprintf("simgo: marshal trace: %v", err))
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func (s *Simulator) record(ev TraceEvent) int {
	ev.Index = len(s.trace.Events)
	ev.At = int64(s.now)
	s.trace.Events = append(s.trace.Events, ev)
	return ev.Index
}

func encode(v any) (json.RawMessage, string) {
	if v == nil {
		return nil, ""
	}
	typ := reflect.TypeOf(v)
	name := typ.Name()
	if typ.Kind() == reflect.Pointer {
		name = typ.Elem().Name()
	}
	if n, ok := v.(Named); ok {
		name = n.SimName()
	}
	b, err := json.Marshal(v)
	if err != nil {
		b, _ = json.Marshal(fmt.Sprintf("%+v", v))
	}
	return b, name
}
