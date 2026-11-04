// Package simnet is an EXPERIMENTAL simulated network for running ordinary,
// goroutine-based Go code under SimGo (Phase 4 spike).
//
// It must be used inside a testing/synctest bubble (Go 1.25+). Inside the
// bubble the time package uses a fake clock that only advances when every
// goroutine is blocked, so simulated latency costs no real time.
//
// Code under test gets real net.Conn and net.Listener values from a Host,
// so it needs no changes beyond being given the Host's Dial and Listen
// functions instead of the net package's:
//
//	synctest.Test(t, func(t *testing.T) {
//		nw := simnet.New(simnet.Config{Seed: 1})
//		ln, _ := nw.Host("server").Listen(7000)
//		go serve(ln)
//		conn, _ := nw.Host("client").Dial(ctx, "server:7000")
//		...
//	})
//
// Determinism is best-effort; see docs/design/phase4-goroutine-mode.md for
// what is and is not reproducible, and why.
package simnet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing/synctest"
	"time"
)

// Config describes the simulated network.
type Config struct {
	Seed       uint64
	MinLatency time.Duration // default 1ms
	MaxLatency time.Duration // default 10ms
	// DialTimeout is how long a dial waits when the destination is
	// unreachable (partitioned or crashed). Default 3s.
	DialTimeout time.Duration
}

// Network is a simulated network. Create it inside a synctest bubble.
type Network struct {
	cfg   Config
	start time.Time

	mu        sync.Mutex
	hosts     map[string]*Host
	listeners map[string]*listener // "host:port"
	cut       map[[2]string]bool   // partitioned host pairs (sorted)
	events    []Event
	pipes     []*pipe // every connection direction, for Heal
}

// New creates a network. It must be called inside a synctest bubble.
func New(cfg Config) *Network {
	if cfg.MaxLatency == 0 {
		cfg.MinLatency, cfg.MaxLatency = time.Millisecond, 10*time.Millisecond
	}
	if cfg.MaxLatency < cfg.MinLatency {
		cfg.MaxLatency = cfg.MinLatency
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 3 * time.Second
	}
	return &Network{
		cfg:       cfg,
		start:     time.Now(),
		hosts:     map[string]*Host{},
		listeners: map[string]*listener{},
		cut:       map[[2]string]bool{},
	}
}

// Event is one entry in the network log.
type Event struct {
	At   time.Duration // since the network was created
	Kind string        // dial, accept, refuse, send, deliver, fin, partition, heal, crash
	From string
	To   string
	Conn string // stable connection identity: "client#3->server:7000"
	Seq  int    // per-connection-direction sequence number
	Len  int
	Sum  uint64 // FNV hash of the payload
}

func (e Event) String() string {
	return fmt.Sprintf("%v %s %s->%s %s seq=%d len=%d sum=%x", e.At, e.Kind, e.From, e.To, e.Conn, e.Seq, e.Len, e.Sum)
}

func (n *Network) now() time.Duration { return time.Since(n.start) }

func (n *Network) record(e Event) {
	e.At = n.now()
	n.mu.Lock()
	n.events = append(n.events, e)
	n.mu.Unlock()
}

// Events returns the network log in the order events were recorded.
func (n *Network) Events() []Event {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]Event(nil), n.events...)
}

// RawFingerprint hashes the log in recording order. Two goroutines acting at
// the same virtual instant can record in either order, so this differs
// between runs whenever that happens.
func (n *Network) RawFingerprint() string { return fingerprint(n.Events()) }

// Fingerprint hashes the log in canonical order: by time, then connection,
// then sequence. Events at the same instant on different connections are
// independent, so their recording order does not matter. Two runs with the
// same Fingerprint observed the same network behaviour.
func (n *Network) Fingerprint() string {
	ev := n.Events()
	sort.SliceStable(ev, func(i, j int) bool {
		a, b := ev[i], ev[j]
		if a.At != b.At {
			return a.At < b.At
		}
		if a.Conn != b.Conn {
			return a.Conn < b.Conn
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		return a.Seq < b.Seq
	})
	return fingerprint(ev)
}

func fingerprint(ev []Event) string {
	h := sha256.New()
	for _, e := range ev {
		fmt.Fprintln(h, e.String())
	}
	return hex.EncodeToString(h.Sum(nil))
}

func pair(a, b string) [2]string {
	if a > b {
		a, b = b, a
	}
	return [2]string{a, b}
}

func (n *Network) isDown(host string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	h := n.hosts[host]
	return h != nil && h.down
}

func (n *Network) isCut(a, b string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.cut[pair(a, b)]
}

// reachable reports whether a new connection from a to b can be made.
func (n *Network) reachable(a, b string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if ha, hb := n.hosts[a], n.hosts[b]; (ha != nil && ha.down) || (hb != nil && hb.down) {
		return false
	}
	return !n.cut[pair(a, b)]
}

// Partition cuts every link between group a and group b. Data sent across
// the cut is held (like TCP retransmitting) and delivered after Heal.
func (n *Network) Partition(a, b []string) {
	n.mu.Lock()
	for _, x := range a {
		for _, y := range b {
			n.cut[pair(x, y)] = true
		}
	}
	n.mu.Unlock()
	n.record(Event{Kind: "partition", From: strings.Join(a, ","), To: strings.Join(b, ",")})
}

// Heal removes all partitions and releases held data.
func (n *Network) Heal() {
	n.mu.Lock()
	n.cut = map[[2]string]bool{}
	pipes := append([]*pipe(nil), n.pipes...)
	n.mu.Unlock()
	n.record(Event{Kind: "heal"})
	for _, p := range pipes {
		p.release()
	}
}

// Host returns the host with the given name, creating it if needed.
func (n *Network) Host(name string) *Host {
	n.mu.Lock()
	defer n.mu.Unlock()
	h, ok := n.hosts[name]
	if !ok {
		h = &Host{nw: n, name: name}
		// Each host gets its own random stream, so the order in which
		// different hosts act does not change the latencies they see.
		h.rng = rand.New(rand.NewPCG(n.cfg.Seed, hashString(name)))
		n.hosts[name] = h
	}
	return h
}

func hashString(s string) uint64 {
	f := fnv.New64a()
	f.Write([]byte(s))
	return f.Sum64()
}

// ------------------------------------------------------------------- hosts

// Host is one machine on the simulated network.
type Host struct {
	nw    *Network
	name  string
	rng   *rand.Rand
	dials int
	down  bool
	conns []*conn
	lns   []*listener
}

// Name returns the host name.
func (h *Host) Name() string { return h.name }

func (h *Host) latency() time.Duration {
	h.nw.mu.Lock()
	defer h.nw.mu.Unlock()
	lo, hi := h.nw.cfg.MinLatency, h.nw.cfg.MaxLatency
	d := lo
	if hi > lo {
		d += time.Duration(h.rng.Int64N(int64(hi - lo)))
	}
	// A seeded sub-millisecond jitter makes simultaneous deliveries on
	// different connections rare, so most reactions run one at a time.
	return d + time.Duration(h.rng.Int64N(1000))
}

// Listen opens a listener on the given port.
func (h *Host) Listen(port int) (net.Listener, error) {
	addr := h.name + ":" + strconv.Itoa(port)
	h.nw.mu.Lock()
	defer h.nw.mu.Unlock()
	if h.down {
		return nil, errors.New("simnet: host is down")
	}
	if _, ok := h.nw.listeners[addr]; ok {
		return nil, fmt.Errorf("simnet: listen %s: address already in use", addr)
	}
	ln := &listener{host: h, addr: addr, accept: make(chan *conn, 128), done: make(chan struct{})}
	h.nw.listeners[addr] = ln
	h.lns = append(h.lns, ln)
	return ln, nil
}

// Dial connects to "host:port".
func (h *Host) Dial(ctx context.Context, addr string) (net.Conn, error) {
	dst, _, ok := strings.Cut(addr, ":")
	if !ok {
		return nil, fmt.Errorf("simnet: dial %s: missing port", addr)
	}
	h.nw.mu.Lock()
	h.dials++
	id := fmt.Sprintf("%s#%d->%s", h.name, h.dials, addr)
	ln := h.nw.listeners[addr]
	h.nw.mu.Unlock()

	rtt := 2 * h.latency()
	if !h.nw.reachable(h.name, dst) {
		h.nw.record(Event{Kind: "dial-timeout", From: h.name, To: dst, Conn: id})
		select {
		case <-time.After(h.nw.cfg.DialTimeout):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("i/o timeout")}
	}
	select {
	case <-time.After(rtt): // handshake
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if ln == nil || ln.isClosed() {
		h.nw.record(Event{Kind: "refuse", From: h.name, To: dst, Conn: id})
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	}
	client, server := newConnPair(h, ln.host, id)
	h.nw.record(Event{Kind: "dial", From: h.name, To: dst, Conn: id})
	select {
	case ln.accept <- server:
	case <-ln.done:
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	}
	return client, nil
}

// Crash closes all of the host's listeners and connections, the way a
// process crash would. Code running on the host must notice (its reads and
// accepts fail) and exit; simnet cannot stop goroutines by force.
func (h *Host) Crash() {
	h.nw.mu.Lock()
	h.down = true
	conns, lns := h.conns, h.lns
	h.conns, h.lns = nil, nil
	for _, ln := range lns {
		delete(h.nw.listeners, ln.addr)
	}
	h.nw.mu.Unlock()
	h.nw.record(Event{Kind: "crash", From: h.name})
	for _, ln := range lns {
		ln.Close()
	}
	for _, c := range conns {
		c.Close()
	}
}

// Close shuts the whole network down: every host is crashed (closing all
// connections and listeners, so blocked reads and accepts return), and then
// Close waits until every other goroutine in the bubble has finished or is
// blocked. Call it, usually deferred, before the synctest bubble's function
// returns: fake time stops when that function exits, so data still in flight
// would otherwise never arrive and goroutines waiting for it would deadlock.
func (n *Network) Close() {
	n.mu.Lock()
	names := make([]string, 0, len(n.hosts))
	for name := range n.hosts {
		names = append(names, name)
	}
	n.mu.Unlock()
	sort.Strings(names)
	for _, name := range names {
		n.Host(name).Crash()
	}
	synctest.Wait()
}

// Restart brings a crashed host back up. It can listen and dial again.
func (h *Host) Restart() {
	h.nw.mu.Lock()
	h.down = false
	h.nw.mu.Unlock()
	h.nw.record(Event{Kind: "restart", From: h.name})
}

// --------------------------------------------------------------- listener

type listener struct {
	host   *Host
	addr   string
	accept chan *conn
	done   chan struct{}
	once   sync.Once
}

func (l *listener) Accept() (net.Conn, error) {
	select {
	case c := <-l.accept:
		l.host.nw.record(Event{Kind: "accept", From: c.remote.name, To: l.host.name, Conn: c.id})
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *listener) isClosed() bool {
	select {
	case <-l.done:
		return true
	default:
		return false
	}
}

func (l *listener) Close() error {
	l.once.Do(func() {
		close(l.done)
		l.host.nw.mu.Lock()
		if l.host.nw.listeners[l.addr] == l {
			delete(l.host.nw.listeners, l.addr)
		}
		l.host.nw.mu.Unlock()
	})
	return nil
}

func (l *listener) Addr() net.Addr { return addr(l.addr) }

type addr string

func (a addr) Network() string { return "simnet" }
func (a addr) String() string  { return string(a) }
