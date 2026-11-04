// Package simproc is SimGo's EXPERIMENTAL goroutine mode: it runs ordinary,
// goroutine-based Go programs as nodes of a simulated cluster, injects
// crashes and partitions, and checks invariants.
//
// A node is a function that runs until its context is cancelled:
//
//	func server(ctx context.Context, p *simproc.Process) error {
//		ln, err := p.Listen(80)
//		if err != nil {
//			return err
//		}
//		go http.Serve(ln, handler(p))
//		<-ctx.Done()
//		return nil
//	}
//
// Everything the node touches that SimGo must control comes from *Process:
// the network (Dial, Listen), the disk, and observations (Emit). Time is Go's
// own time package: runs execute inside a testing/synctest bubble, where the
// clock is fake and only advances when every goroutine is blocked.
//
// Crashing a node cancels its context, closes its network connections and
// discards unsynced disk writes. Goroutines cannot be killed from outside, so
// node code must return when its context is cancelled or its connections
// fail. After a crash, the old incarnation's Process stops working (writes
// are ignored, dials fail), so stragglers cannot affect the new incarnation.
// Restarting calls the node function again with a fresh context.
//
// Runs are reproducible except when two goroutines of the same process wake
// at the same virtual instant and race; see docs/design/phase4-goroutine-mode.md.
// Explore therefore re-runs every failing seed and reports how often it
// reproduces.
package simproc

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/Garvit-Jethwani/simgo/sim"
	"github.com/Garvit-Jethwani/simgo/x/simnet"
)

// Main is a node's program. It must return soon after ctx is cancelled.
// Returning a non-nil error while the node is alive is a violation.
type Main func(ctx context.Context, p *Process) error

// Node describes one node of the cluster.
type Node struct {
	Name string
	Main Main
	// Protected nodes are never crashed (they can still be partitioned).
	Protected bool
}

// Faults controls fault injection.
type Faults struct {
	// Interval is the mean time between faults. Zero disables injection.
	Interval   time.Duration
	Crashes    bool // crash a node, restart it after a delay
	Partitions bool // split the cluster in two, heal after a delay
	// MaxDowntime bounds how long crashes and partitions last. Default 3s.
	MaxDowntime time.Duration
}

// Invariant is checked after every emission and at the end of the run.
type Invariant struct {
	Name  string
	Check func(h *History) error
}

// Config describes a run.
type Config struct {
	Seed     uint64
	Nodes    []Node
	Duration time.Duration // default 30s
	// MinLatency and MaxLatency bound network latency (default 1ms..10ms).
	MinLatency, MaxLatency time.Duration
	Faults                 Faults
	Invariants             []Invariant
}

// History gives invariants read-only access to what nodes emitted.
type History struct{ c *cluster }

// Now returns the virtual time since the run started.
func (h *History) Now() time.Duration { return h.c.nw.Now() }

// Emitted returns all emissions of a kind so far, in emission order.
func (h *History) Emitted(kind string) []sim.Emission { return h.c.emissions[kind] }

// IsUp reports whether the named node is running.
func (h *History) IsUp(name string) bool {
	n, ok := h.c.byName[name]
	return ok && n.up
}

// --------------------------------------------------------------- process

// Process is one incarnation of a node: its handle on the simulated world.
// It is safe for concurrent use by the node's goroutines.
type Process struct {
	c           *cluster
	n           *nodeState
	incarnation int
	ctx         context.Context
}

// ErrCrashed is returned by Process operations after the incarnation crashed.
var ErrCrashed = errors.New("simproc: process crashed")

func (p *Process) alive() bool {
	p.c.mu.Lock()
	defer p.c.mu.Unlock()
	return p.n.up && p.n.incarnation == p.incarnation
}

// Name returns the node name.
func (p *Process) Name() string { return p.n.name }

// Incarnation counts restarts: 1 for the first run, 2 after one restart...
func (p *Process) Incarnation() int { return p.incarnation }

// Seed returns a seed derived from the run seed, the node and the
// incarnation, for code that needs its own deterministic randomness.
func (p *Process) Seed() uint64 {
	return p.c.cfg.Seed ^ uint64(p.n.index+1)*0x9e3779b97f4a7c15 ^ uint64(p.incarnation)<<48
}

// Rand returns a new random source seeded with Seed. Use one per goroutine.
func (p *Process) Rand() *rand.Rand { return rand.New(rand.NewPCG(p.Seed(), 0x51)) }

// Listen opens a listener on this node.
func (p *Process) Listen(port int) (net.Listener, error) {
	if !p.alive() {
		return nil, ErrCrashed
	}
	return p.n.host.Listen(port)
}

// Dial connects from this node to "node:port".
func (p *Process) Dial(ctx context.Context, addr string) (net.Conn, error) {
	if !p.alive() {
		return nil, ErrCrashed
	}
	return p.n.host.Dial(ctx, addr)
}

// DialContext has the signature of net.Dialer.DialContext, for use as
// http.Transport.DialContext. The network argument is ignored.
func (p *Process) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return p.Dial(ctx, addr)
}

// Disk returns this node's disk. It survives crashes; unsynced writes do not.
func (p *Process) Disk() *Disk { return &Disk{p: p} }

// Emit records an externally visible action, such as acknowledging a write
// to a client. Invariants are checked against emissions.
func (p *Process) Emit(kind string, value any) {
	if !p.alive() {
		return
	}
	p.c.emit(p.n, kind, value)
}

// Logf records a log line in the trace.
func (p *Process) Logf(format string, args ...any) {
	if !p.alive() {
		return
	}
	p.c.event(evt{kind: sim.KindLog, node: p.n.name, detail: fmt.Sprintf(format, args...)})
}

// Disk is a node's simulated durable storage, as seen by one incarnation.
// After the incarnation crashes, writes are ignored and reads fail.
type Disk struct{ p *Process }

func (d *Disk) disk() *sim.Disk { return d.p.n.disk }

// Write buffers a write. It is lost on crash unless Sync is called.
func (d *Disk) Write(key string, value []byte) error {
	if !d.p.alive() {
		return ErrCrashed
	}
	d.disk().Write(key, value)
	return nil
}

// Delete buffers a delete. It is lost on crash unless Sync is called.
func (d *Disk) Delete(key string) error {
	if !d.p.alive() {
		return ErrCrashed
	}
	d.disk().Delete(key)
	return nil
}

// Sync makes all buffered writes durable.
func (d *Disk) Sync() error {
	if !d.p.alive() {
		return ErrCrashed
	}
	d.disk().Sync()
	return nil
}

// Read returns the current value of key.
func (d *Disk) Read(key string) ([]byte, bool, error) {
	if !d.p.alive() {
		return nil, false, ErrCrashed
	}
	v, ok := d.disk().Read(key)
	return v, ok, nil
}

// Keys returns all live keys in sorted order.
func (d *Disk) Keys() ([]string, error) {
	if !d.p.alive() {
		return nil, ErrCrashed
	}
	return d.disk().Keys(), nil
}

// --------------------------------------------------------------- cluster

type nodeState struct {
	index       int
	name        string
	main        Main
	protected   bool
	host        *simnet.Host
	disk        *sim.Disk
	up          bool
	incarnation int
	cancel      context.CancelFunc
}

type evt struct {
	at     time.Duration
	kind   string
	node   string
	detail string
	value  any
	groups [][]string
}

type cluster struct {
	cfg    Config
	nw     *simnet.Network
	ctx    context.Context
	stop   context.CancelFunc
	rng    *rand.Rand // nemesis randomness; used by the nemesis goroutine only
	nodes  []*nodeState
	byName map[string]*nodeState

	mu        sync.Mutex
	emissions map[string][]sim.Emission
	events    []evt
	violation *sim.Violation
	violated  chan struct{}
}

func (c *cluster) event(e evt) {
	e.at = c.nw.Now()
	c.mu.Lock()
	c.events = append(c.events, e)
	c.mu.Unlock()
}

func (c *cluster) emit(n *nodeState, kind string, value any) {
	at := c.nw.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.violation != nil {
		return
	}
	c.emissions[kind] = append(c.emissions[kind], sim.Emission{At: at, Node: sim.NodeID(n.index), Kind: kind, Value: value})
	c.events = append(c.events, evt{at: at, kind: sim.KindEmit, node: n.name, detail: kind, value: value})
	c.checkLocked()
}

// checkLocked runs every invariant. c.mu must be held.
func (c *cluster) checkLocked() {
	h := &History{c: c}
	for _, inv := range c.cfg.Invariants {
		if err := inv.Check(h); err != nil {
			c.failLocked(inv.Name, err.Error())
			return
		}
	}
}

func (c *cluster) failLocked(name, msg string) {
	if c.violation != nil {
		return
	}
	at := c.nw.Now()
	c.violation = &sim.Violation{Invariant: name, Message: msg, At: int64(at)}
	c.events = append(c.events, evt{at: at, kind: sim.KindViolation, detail: name + ": " + msg})
	close(c.violated)
}

func (c *cluster) start(n *nodeState, kind string) {
	c.mu.Lock()
	if c.ctx.Err() != nil {
		// The run is shutting down; a late restart must not start goroutines
		// that would outlive the bubble.
		c.mu.Unlock()
		return
	}
	n.up = true
	n.incarnation++
	ctx, cancel := context.WithCancel(c.ctx)
	n.cancel = cancel
	p := &Process{c: c, n: n, incarnation: n.incarnation, ctx: ctx}
	c.mu.Unlock()
	c.event(evt{kind: kind, node: n.name})
	go func() {
		err := n.main(ctx, p)
		if err != nil && ctx.Err() == nil && p.alive() {
			c.mu.Lock()
			c.failLocked("node exited", fmt.Sprintf("node %s returned an error: %v", n.name, err))
			c.mu.Unlock()
		}
	}()
}

func (c *cluster) crash(n *nodeState) {
	c.mu.Lock()
	n.up = false
	cancel := n.cancel
	c.mu.Unlock()
	cancel()
	n.host.Crash()
	lost := n.disk.Crash()
	c.event(evt{kind: sim.KindCrash, node: n.name, detail: fmt.Sprintf("lost %d unsynced disk writes", lost)})
}

func (c *cluster) restart(n *nodeState) {
	n.host.Restart()
	c.start(n, sim.KindRestart)
}

// jitter returns a seeded sub-millisecond offset, so faults never fire at the
// same instant as code timers (see the design document).
func (c *cluster) jitter() time.Duration { return time.Duration(1 + c.rng.Int64N(999_999)) }

// sleep waits for d or until the run stops. It reports whether to continue.
func (c *cluster) sleep(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-c.ctx.Done():
		return false
	}
}

func (c *cluster) nemesis() {
	f := c.cfg.Faults
	var kinds []string
	if f.Crashes {
		kinds = append(kinds, "crash")
	}
	if f.Partitions && len(c.nodes) > 1 {
		kinds = append(kinds, "partition")
	}
	if f.Interval <= 0 || len(kinds) == 0 {
		return
	}
	partitioned := false
	for {
		delay := f.Interval/2 + time.Duration(c.rng.Int64N(int64(f.Interval)+1)) + c.jitter()
		if !c.sleep(delay) {
			return
		}
		down := f.MaxDowntime/5 + time.Duration(c.rng.Int64N(int64(f.MaxDowntime-f.MaxDowntime/5)+1)) + c.jitter()
		switch kinds[c.rng.IntN(len(kinds))] {
		case "crash":
			var candidates []*nodeState
			c.mu.Lock()
			for _, n := range c.nodes {
				if n.up && !n.protected {
					candidates = append(candidates, n)
				}
			}
			c.mu.Unlock()
			if len(candidates) == 0 {
				continue
			}
			n := candidates[c.rng.IntN(len(candidates))]
			c.crash(n)
			go func() {
				if c.sleep(down) {
					c.restart(n)
				}
			}()
		case "partition":
			if partitioned {
				continue
			}
			names := make([]string, len(c.nodes))
			for i, n := range c.nodes {
				names[i] = n.name
			}
			c.rng.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })
			cut := 1 + c.rng.IntN(len(names)-1)
			a, b := append([]string(nil), names[:cut]...), append([]string(nil), names[cut:]...)
			sort.Strings(a)
			sort.Strings(b)
			c.nw.Partition(a, b)
			c.event(evt{kind: sim.KindPartition, groups: [][]string{a, b}})
			partitioned = true
			if !c.sleep(down) {
				return
			}
			c.nw.Heal()
			c.event(evt{kind: sim.KindHeal})
			partitioned = false
		}
	}
}

// Run executes one simulation. It must be called inside a synctest bubble
// (see Explore, which does that for you).
func Run(cfg Config) *sim.Result {
	if cfg.Duration == 0 {
		cfg.Duration = 30 * time.Second
	}
	if cfg.Faults.MaxDowntime == 0 {
		cfg.Faults.MaxDowntime = 3 * time.Second
	}
	if len(cfg.Nodes) == 0 {
		panic("simproc: Config.Nodes is empty")
	}
	nw := simnet.New(simnet.Config{Seed: cfg.Seed, MinLatency: cfg.MinLatency, MaxLatency: cfg.MaxLatency})
	ctx, stop := context.WithCancel(context.Background())
	c := &cluster{
		cfg:       cfg,
		nw:        nw,
		ctx:       ctx,
		stop:      stop,
		rng:       rand.New(rand.NewPCG(cfg.Seed, 0x6e656d)),
		byName:    map[string]*nodeState{},
		emissions: map[string][]sim.Emission{},
		violated:  make(chan struct{}),
	}
	for i, nd := range cfg.Nodes {
		if _, dup := c.byName[nd.Name]; dup {
			panic("simproc: duplicate node name " + nd.Name)
		}
		n := &nodeState{index: i, name: nd.Name, main: nd.Main, protected: nd.Protected, host: nw.Host(nd.Name), disk: sim.NewDisk()}
		c.nodes = append(c.nodes, n)
		c.byName[nd.Name] = n
	}
	for _, n := range c.nodes {
		c.start(n, sim.KindStart)
	}
	nemesisDone := make(chan struct{})
	go func() {
		defer close(nemesisDone)
		c.nemesis()
	}()

	timer := time.NewTimer(cfg.Duration)
	select {
	case <-timer.C:
	case <-c.violated:
		timer.Stop()
	}
	end := nw.Now()

	// Final check: invariants may depend on everything having happened.
	c.mu.Lock()
	if c.violation == nil {
		c.checkLocked()
	}
	c.mu.Unlock()

	// Shut down: cancel every node, crash the network, let goroutines exit.
	stop()
	<-nemesisDone
	nw.Close()

	return buildResult(c, end)
}
