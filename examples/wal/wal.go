// Package wal is a key-value server with a write-ahead log and group commit:
// the smallest example of the "acknowledged but not durable" bug.
//
// Clients send writes. The server appends each write to its log on disk and
// fsyncs every 10ms, so that one expensive fsync covers many writes (group
// commit). The only question is when the client hears "OK":
//
//   - AckThenSync (bug): acknowledge as soon as the write is in the log,
//     fsync later. A crash in between loses writes the client was told are
//     safe.
//   - SyncThenAck (fix): hold acknowledgements until the fsync that covers
//     them has completed. Same throughput, no lost writes.
package wal

import (
	"fmt"
	"strings"
	"time"

	"github.com/Garvit-Jethwani/simgo/sim"
)

// Mode selects when writes are acknowledged.
type Mode int

const (
	SyncThenAck Mode = iota
	AckThenSync
)

const (
	Server     = sim.NodeID(0)
	flushEvery = 10 * time.Millisecond
	logPrefix  = "wal/"

	EmitAck       = "ack"
	EmitRecovered = "recovered"
)

type (
	Put struct{ Key string }
	OK  struct{ Key string }
)

// Recovered is emitted when the server rebuilds its state from the log.
type Recovered struct{ Keys []string }

// ------------------------------------------------------------------ server

// KV is the server.
type KV struct {
	Mode    Mode
	Entries int      // log entries written so far
	Waiting []string // writes waiting for the next fsync (SyncThenAck)
	waiting []sim.NodeID
}

func (s *KV) Start(env *sim.Env) {
	var keys []string
	for _, k := range env.Disk().Keys() {
		if strings.HasPrefix(k, logPrefix) {
			v, _ := env.Disk().Read(k)
			keys = append(keys, string(v))
		}
	}
	s.Entries = len(keys)
	env.Emit(EmitRecovered, Recovered{Keys: keys})
	env.SetTimer("flush", flushEvery)
}

func (s *KV) Receive(env *sim.Env, from sim.NodeID, msg sim.Message) {
	put, ok := msg.(Put)
	if !ok {
		return
	}
	s.Entries++
	env.Disk().Write(fmt.Sprintf("%s%010d", logPrefix, s.Entries), []byte(put.Key))

	if s.Mode == AckThenSync {
		// BUG: the write is only in the page cache. It is not durable yet.
		env.Emit(EmitAck, put.Key)
		env.Send(from, OK{Key: put.Key})
		return
	}
	s.Waiting = append(s.Waiting, put.Key)
	s.waiting = append(s.waiting, from)
}

func (s *KV) Timer(env *sim.Env, name string) {
	env.Disk().Sync() // group commit: one fsync for every write since the last one
	for i, key := range s.Waiting {
		env.Emit(EmitAck, key)
		env.Send(s.waiting[i], OK{Key: key})
	}
	s.Waiting, s.waiting = nil, nil
	env.SetTimer("flush", flushEvery)
}

func (s *KV) Snapshot() any { return s }

// ------------------------------------------------------------------ client

// Client writes a new key every few milliseconds.
type Client struct{ Sent, Acked int }

func (c *Client) Start(env *sim.Env) { env.SetTimer("write", time.Millisecond) }

func (c *Client) Timer(env *sim.Env, name string) {
	c.Sent++
	env.Send(Server, Put{Key: fmt.Sprintf("c%d-%d", env.ID(), c.Sent)})
	env.SetTimer("write", time.Duration(2+env.Rand().IntN(8))*time.Millisecond)
}

func (c *Client) Receive(env *sim.Env, from sim.NodeID, msg sim.Message) {
	if _, ok := msg.(OK); ok {
		c.Acked++
	}
}

func (c *Client) Snapshot() any { return c }

// ------------------------------------------------------------------ config

// AcknowledgedWritesSurvive: after every recovery, the server must still have
// every write it acknowledged before the crash.
var AcknowledgedWritesSurvive = sim.Invariant{
	Name: "acknowledged writes survive crashes",
	Check: func(w *sim.World) error {
		// Earlier recoveries were checked when they happened, so only the
		// latest one needs checking.
		recoveries := w.Emitted(EmitRecovered)
		last := recoveries[len(recoveries)-1]
		if last.At != w.Now() {
			return nil
		}
		recovered := map[string]bool{}
		for _, k := range last.Value.(Recovered).Keys {
			recovered[k] = true
		}
		for _, a := range w.Emitted(EmitAck) {
			if a.At >= last.At {
				break
			}
			if key := a.Value.(string); !recovered[key] {
				return fmt.Errorf("write %q was acknowledged at %v but is missing after the restart at %v",
					key, a.At, last.At)
			}
		}
		return nil
	},
}

// Config returns a simulation of one server and two clients, with server
// crashes.
func Config(mode Mode) sim.Config {
	return sim.Config{
		Nodes: 3,
		NewNode: func(id sim.NodeID) sim.Node {
			if id == Server {
				return &KV{Mode: mode}
			}
			return &Client{}
		},
		NodeName: func(id sim.NodeID) string {
			if id == Server {
				return "server"
			}
			return fmt.Sprintf("client-%d", id)
		},
		Duration: 5 * time.Second,
		Network:  sim.NetworkConfig{MinLatency: 100 * time.Microsecond, MaxLatency: time.Millisecond},
		Faults: sim.FaultConfig{
			Interval:    time.Second,
			Crashes:     true,
			MaxDowntime: 200 * time.Millisecond,
			Protected:   []sim.NodeID{1, 2}, // clients are just load generators
		},
		Invariants: []sim.Invariant{AcknowledgedWritesSurvive},
	}
}
