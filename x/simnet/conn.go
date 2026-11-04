package simnet

import (
	"hash/fnv"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"sync"
	"time"
)

// pipe is one direction of a connection: bytes written by one host and read
// by the other. Reads block on a sync.Cond, which synctest treats as durably
// blocking, so fake time can advance while code waits for data.
type pipe struct {
	nw       *Network
	from, to *Host
	id       string

	mu       sync.Mutex
	cond     *sync.Cond
	buf      []byte
	eof      bool // the writer closed and the FIN has arrived
	closed   bool // the reader closed its end
	seq      int
	lastAt   time.Time  // arrival time of the last segment, for FIFO order
	rng      *rand.Rand // this direction's latency stream (guarded by mu)
	held     []*segment // segments waiting for a partition to heal
	deadline time.Time
	timer    *time.Timer
}

type segment struct {
	data []byte
	fin  bool
	seq  int
}

func newPipe(nw *Network, from, to *Host, id string) *pipe {
	p := &pipe{nw: nw, from: from, to: to, id: id, rng: nw.stream(id + "|" + from.name)}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// send schedules data (or a FIN) for delivery after a seeded latency,
// preserving order on this direction.
func (p *pipe) send(data []byte, fin bool) {
	p.mu.Lock()
	lat := p.nw.latency(p.rng)
	p.seq++
	seg := &segment{data: data, fin: fin, seq: p.seq}
	at := time.Now().Add(lat)
	if !at.After(p.lastAt) {
		at = p.lastAt.Add(time.Nanosecond)
	}
	p.lastAt = at
	p.mu.Unlock()

	kind := "send"
	if fin {
		kind = "fin"
	}
	p.nw.record(Event{Kind: kind, From: p.from.name, To: p.to.name, Conn: p.id, Seq: seg.seq, Len: len(data), Sum: sum(data), Data: preview(data)})
	time.AfterFunc(time.Until(at), func() { p.arrive(seg) })
}

// arrive handles a segment reaching the receiver. Data already in flight
// still arrives if the sender has since crashed (the OS closes a crashed
// process's sockets, which is how peers see EOF). It is lost if the receiver
// is down, and held while a partition separates the two hosts.
func (p *pipe) arrive(seg *segment) {
	switch {
	case p.nw.isDown(p.to.name):
		p.nw.record(Event{Kind: "drop", From: p.from.name, To: p.to.name, Conn: p.id, Seq: seg.seq, Len: len(seg.data), Sum: sum(seg.data)})
		return
	case p.nw.isCut(p.from.name, p.to.name):
		p.mu.Lock()
		p.held = append(p.held, seg)
		p.mu.Unlock()
		return
	}
	p.mu.Lock()
	if len(p.held) > 0 {
		// Keep FIFO order: anything after a held segment waits too.
		p.held = append(p.held, seg)
		p.mu.Unlock()
		return
	}
	p.deliverLocked(seg)
	p.mu.Unlock()
}

func (p *pipe) deliverLocked(seg *segment) {
	if seg.fin {
		p.eof = true
	} else if !p.closed {
		p.buf = append(p.buf, seg.data...)
	}
	p.nw.record(Event{Kind: "deliver", From: p.from.name, To: p.to.name, Conn: p.id, Seq: seg.seq, Len: len(seg.data), Sum: sum(seg.data)})
	p.cond.Broadcast()
}

// release delivers held segments after a partition heals.
func (p *pipe) release() {
	p.mu.Lock()
	held := p.held
	p.held = nil
	lat := p.nw.latency(p.rng)
	p.mu.Unlock()
	if len(held) == 0 {
		return
	}
	time.AfterFunc(lat, func() {
		if p.nw.isDown(p.to.name) {
			for _, seg := range held {
				p.nw.record(Event{Kind: "drop", From: p.from.name, To: p.to.name, Conn: p.id, Seq: seg.seq, Len: len(seg.data), Sum: sum(seg.data)})
			}
			return
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, seg := range held {
			p.deliverLocked(seg)
		}
	})
}

func (p *pipe) read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.buf) == 0 && !p.eof && !p.closed {
		if !p.deadline.IsZero() && !time.Now().Before(p.deadline) {
			return 0, os.ErrDeadlineExceeded
		}
		p.cond.Wait()
	}
	if p.closed {
		return 0, net.ErrClosed
	}
	if len(p.buf) == 0 && p.eof {
		return 0, io.EOF
	}
	n := copy(b, p.buf)
	p.buf = p.buf[n:]
	return n, nil
}

func (p *pipe) setDeadline(t time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deadline = t
	if p.timer != nil {
		p.timer.Stop()
		p.timer = nil
	}
	if !t.IsZero() {
		p.timer = time.AfterFunc(time.Until(t), func() {
			p.mu.Lock()
			p.cond.Broadcast()
			p.mu.Unlock()
		})
	}
	p.cond.Broadcast()
}

func (p *pipe) closeReader() {
	p.mu.Lock()
	p.closed = true
	if p.timer != nil {
		p.timer.Stop()
	}
	p.cond.Broadcast()
	p.mu.Unlock()
}

func sum(b []byte) uint64 {
	if len(b) == 0 {
		return 0
	}
	f := fnv.New64a()
	f.Write(b)
	return f.Sum64()
}

// ------------------------------------------------------------------- conn

type conn struct {
	id       string
	local    *Host
	remote   *Host
	in, out  *pipe
	mu       sync.Mutex
	closed   bool
	localAd  addr
	remoteAd addr
}

func newConnPair(client, server *Host, id string) (*conn, *conn) {
	nw := client.nw
	c2s := newPipe(nw, client, server, id)
	s2c := newPipe(nw, server, client, id)
	c := &conn{id: id, local: client, remote: server, in: s2c, out: c2s, localAd: addr(client.name), remoteAd: addr(server.name)}
	s := &conn{id: id, local: server, remote: client, in: c2s, out: s2c, localAd: addr(server.name), remoteAd: addr(client.name)}
	nw.mu.Lock()
	nw.pipes = append(nw.pipes, c2s, s2c)
	client.conns = append(client.conns, c)
	server.conns = append(server.conns, s)
	nw.mu.Unlock()
	return c, s
}

func (c *conn) Read(b []byte) (int, error) { return c.in.read(b) }

func (c *conn) Write(b []byte) (int, error) {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return 0, net.ErrClosed
	}
	if len(b) == 0 {
		return 0, nil
	}
	c.out.send(append([]byte(nil), b...), false)
	return len(b), nil
}

func (c *conn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	c.out.send(nil, true) // FIN: the peer reads EOF after pending data
	c.in.closeReader()
	return nil
}

func (c *conn) LocalAddr() net.Addr  { return c.localAd }
func (c *conn) RemoteAddr() net.Addr { return c.remoteAd }

func (c *conn) SetDeadline(t time.Time) error {
	c.in.setDeadline(t)
	return nil
}
func (c *conn) SetReadDeadline(t time.Time) error {
	c.in.setDeadline(t)
	return nil
}

// SetWriteDeadline is accepted but has no effect: writes never block.
func (c *conn) SetWriteDeadline(t time.Time) error { return nil }

var _ net.Conn = (*conn)(nil)
