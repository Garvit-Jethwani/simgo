// Package sim is a deterministic simulator for distributed systems.
//
// A simulation runs N nodes inside a single goroutine. The simulator owns
// everything that is normally nondeterministic: the clock, the network, the
// disk, and every source of randomness. Given the same seed and the same node
// code, a simulation produces exactly the same sequence of events, so any bug
// it finds can be replayed step by step.
//
// Nodes are written as event handlers (see Node). They must not start
// goroutines, read the wall clock, or use global randomness; everything they
// need is provided through *Env.
package sim

import "time"

// NodeID identifies a node in a simulation. IDs are dense: 0..Nodes-1.
type NodeID int

// Message is any value sent between nodes. Messages should be plain,
// JSON-serializable structs so they can be recorded in the trace.
type Message any

// Named is optionally implemented by messages and emitted values that want a
// more descriptive name in traces than their Go type name (for example, a
// wrapper type that carries several protocol message kinds).
type Named interface {
	SimName() string
}

// Node is the code under test. The simulator calls these methods one at a
// time; a node never runs concurrently with itself or with other nodes.
type Node interface {
	// Start is called when the node boots: at the beginning of the
	// simulation and again after every restart. Volatile state from before
	// a crash is gone; durable state must be recovered from env.Disk().
	Start(env *Env)

	// Receive is called when a message from another node is delivered.
	Receive(env *Env, from NodeID, msg Message)

	// Timer is called when a timer set with env.SetTimer fires.
	Timer(env *Env, name string)
}

// Snapshotter is optionally implemented by nodes that want their state shown
// in the time-travel viewer. Snapshot should return a JSON-serializable value.
// It is called after every event the node handles.
type Snapshotter interface {
	Snapshot() any
}

// Invariant is a property that must hold after every simulation step.
// Returning a non-nil error stops the simulation and reports a violation.
type Invariant struct {
	Name  string
	Check func(w *World) error
}

// NetworkConfig describes the simulated network.
type NetworkConfig struct {
	// MinLatency and MaxLatency bound the one-way delivery delay. Each
	// message gets a latency drawn uniformly from this range, so messages
	// between the same two nodes can be reordered.
	MinLatency time.Duration
	MaxLatency time.Duration

	// DropRate is the probability that a message is silently lost.
	DropRate float64

	// DupRate is the probability that a message is delivered twice.
	DupRate float64
}

// FaultConfig controls the "nemesis": the component that injects faults at
// random times during a run.
type FaultConfig struct {
	// Interval is the mean time between injected faults. Zero disables
	// random fault injection.
	Interval time.Duration

	Partitions bool // split the network into two groups, heal later
	Crashes    bool // crash a node (losing unsynced disk writes), restart later
	Pauses     bool // freeze a node (GC pause, VM stall), resume later
	ClockSkew  bool // shift a node's local clock

	// MaxPause and MaxSkew bound the size of pauses and clock jumps.
	MaxPause time.Duration
	MaxSkew  time.Duration

	// MaxDowntime bounds how long partitions and crashes last.
	MaxDowntime time.Duration

	// Protected nodes are never crashed or paused (they can still be
	// partitioned). Useful for nodes that model external infrastructure.
	Protected []NodeID
}

// Config describes one simulation run.
type Config struct {
	Seed     uint64
	Nodes    int
	NewNode  func(id NodeID) Node
	NodeName func(id NodeID) string // optional, used in traces

	// Duration is how much virtual time to simulate.
	Duration time.Duration

	Network    NetworkConfig
	Faults     FaultConfig
	Invariants []Invariant

	// MaxEvents is a safety limit on the number of processed events.
	// Zero means 1,000,000.
	MaxEvents int
}

func (c *Config) withDefaults() Config {
	cfg := *c
	if cfg.Duration == 0 {
		cfg.Duration = time.Minute
	}
	if cfg.Network.MaxLatency == 0 {
		cfg.Network.MinLatency = time.Millisecond
		cfg.Network.MaxLatency = 10 * time.Millisecond
	}
	if cfg.Network.MaxLatency < cfg.Network.MinLatency {
		cfg.Network.MaxLatency = cfg.Network.MinLatency
	}
	if cfg.Faults.MaxPause == 0 {
		cfg.Faults.MaxPause = 5 * time.Second
	}
	if cfg.Faults.MaxSkew == 0 {
		cfg.Faults.MaxSkew = 2 * time.Second
	}
	if cfg.Faults.MaxDowntime == 0 {
		cfg.Faults.MaxDowntime = 5 * time.Second
	}
	if cfg.MaxEvents == 0 {
		cfg.MaxEvents = 1_000_000
	}
	return cfg
}
