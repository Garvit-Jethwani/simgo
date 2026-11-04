package simproc

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Garvit-Jethwani/simgo/sim"
)

// buildResult merges the network log and the cluster's own events into a
// sim.Trace, so goroutine-mode runs use the same time-travel viewer.
//
// Events are put in canonical order (time, node, kind, connection,
// sequence): goroutines acting at the same virtual instant can record in any
// order, and that order must not change the trace or its fingerprint.
func buildResult(c *cluster, end time.Duration) *sim.Result {
	type item struct {
		at    time.Duration
		node  string
		kind  string
		conn  string
		seq   int
		order int
		ev    sim.TraceEvent
	}

	// Node table: configured nodes first, then any other hosts by name.
	index := map[string]sim.NodeID{}
	tr := &sim.Trace{Seed: c.cfg.Seed, Duration: int64(c.cfg.Duration)}
	for _, n := range c.nodes {
		index[n.name] = sim.NodeID(len(tr.Nodes))
		tr.Nodes = append(tr.Nodes, sim.TraceNode{ID: sim.NodeID(len(tr.Nodes)), Name: n.name})
	}
	netEvents := c.nw.Events()
	var extra []string
	for _, e := range netEvents {
		for _, h := range []string{e.From, e.To} {
			if _, ok := index[h]; !ok && h != "" && !strings.Contains(h, ",") {
				index[h] = -1
				extra = append(extra, h)
			}
		}
	}
	sort.Strings(extra)
	for _, h := range extra {
		index[h] = sim.NodeID(len(tr.Nodes))
		tr.Nodes = append(tr.Nodes, sim.TraceNode{ID: sim.NodeID(len(tr.Nodes)), Name: h})
	}
	id := func(name string) sim.NodeID {
		if v, ok := index[name]; ok && name != "" {
			return v
		}
		return -1
	}
	groups := func(gs [][]string) [][]sim.NodeID {
		out := make([][]sim.NodeID, len(gs))
		for i, g := range gs {
			for _, name := range g {
				out[i] = append(out[i], id(name))
			}
		}
		return out
	}
	quote := func(s string) json.RawMessage {
		b, _ := json.Marshal(s)
		return b
	}

	var items []item
	for i, e := range netEvents {
		it := item{at: e.At, node: e.From, kind: e.Kind, conn: e.Conn, seq: e.Seq, order: i}
		switch e.Kind {
		case "send", "fin":
			typ := "data"
			if e.Kind == "fin" {
				typ = "FIN"
			}
			it.ev = sim.TraceEvent{Kind: sim.KindSend, Node: id(e.From), Peer: id(e.To), MsgType: typ, Msg: quote(e.Data), Detail: fmt.Sprintf("%s seq=%d len=%d", e.Conn, e.Seq, e.Len)}
		case "deliver":
			typ := "data"
			if e.Len == 0 {
				typ = "FIN"
			}
			it.node = e.To
			it.ev = sim.TraceEvent{Kind: sim.KindDeliver, Node: id(e.To), Peer: id(e.From), MsgType: typ, Msg: quote(e.Data), Detail: fmt.Sprintf("%s seq=%d len=%d", e.Conn, e.Seq, e.Len)}
		case "drop":
			it.ev = sim.TraceEvent{Kind: sim.KindDrop, Node: id(e.From), Peer: id(e.To), MsgType: "data", Detail: "receiver is down"}
		case "dial", "refuse", "dial-timeout", "accept":
			node := e.From
			if e.Kind == "accept" {
				node = e.To
			}
			it.node = node
			it.ev = sim.TraceEvent{Kind: sim.KindLog, Node: id(node), Peer: -1, Detail: fmt.Sprintf("%s %s", e.Kind, e.Conn)}
		default:
			// partition, heal, crash and restart are recorded by the
			// cluster with more detail.
			continue
		}
		items = append(items, it)
	}

	c.mu.Lock()
	clusterEvents := append([]evt(nil), c.events...)
	violation := c.violation
	c.mu.Unlock()
	for i, e := range clusterEvents {
		it := item{at: e.at, node: e.node, kind: e.kind, order: len(netEvents) + i}
		te := sim.TraceEvent{Kind: e.kind, Node: id(e.node), Peer: -1, Detail: e.detail}
		if e.value != nil {
			raw, _ := json.Marshal(e.value)
			te.Msg = raw
		}
		if e.groups != nil {
			te.Groups = groups(e.groups)
		}
		it.ev = te
		items = append(items, it)
	}

	// Canonical order. The violation always comes last at its instant.
	rank := func(k string) int {
		if k == sim.KindViolation {
			return 1
		}
		return 0
	}
	sort.SliceStable(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if a.at != b.at {
			return a.at < b.at
		}
		if rank(a.kind) != rank(b.kind) {
			return rank(a.kind) < rank(b.kind)
		}
		if a.node != b.node {
			return a.node < b.node
		}
		if a.kind != b.kind {
			return a.kind < b.kind
		}
		if a.conn != b.conn {
			return a.conn < b.conn
		}
		if a.seq != b.seq {
			return a.seq < b.seq
		}
		return a.ev.Detail < b.ev.Detail
	})

	// Message IDs link sends to deliveries: (connection, direction, seq).
	msgIDs := map[string]uint64{}
	key := func(e simnetKey) string { return fmt.Sprintf("%s|%s|%d", e.conn, e.from, e.seq) }
	for i := range items {
		it := &items[i]
		ev := &it.ev
		ev.Index = i
		ev.At = int64(it.at)
		switch ev.Kind {
		case sim.KindSend, sim.KindDeliver, sim.KindDrop:
			from := it.node
			if ev.Kind == sim.KindDeliver {
				from = tr.Nodes[ev.Peer].Name
			}
			k := key(simnetKey{conn: it.conn, from: from, seq: it.seq})
			if _, ok := msgIDs[k]; !ok {
				msgIDs[k] = uint64(len(msgIDs) + 1)
			}
			ev.MsgID = msgIDs[k]
		case sim.KindViolation:
			if violation != nil {
				violation.Event = i
			}
		}
		tr.Events = append(tr.Events, *ev)
	}
	tr.Violation = violation
	return &sim.Result{Seed: c.cfg.Seed, Violation: violation, Trace: tr, Steps: len(tr.Events), End: end}
}

type simnetKey struct {
	conn string
	from string
	seq  int
}
