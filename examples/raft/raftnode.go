// Package raftsim runs the real etcd raft library (go.etcd.io/raft/v3)
// inside SimGo.
//
// etcd raft is designed for this: RawNode is a deterministic state machine
// driven by Tick, Step and Ready. The application is responsible for
// persisting state, sending messages and applying committed entries, and the
// library's documentation is explicit about the contract: HardState and
// entries must be durably stored before messages from the same Ready are
// sent.
//
// This package wraps RawNode as a sim.Node and offers two storage modes:
//
//   - SyncEveryReady: the contract is followed. Every Ready is written to
//     disk and fsynced before its messages go out.
//   - BatchedFsync: a common "optimisation". Writes go to disk immediately
//     but are fsynced on a 50ms timer, to batch the expensive fsyncs. A
//     crash in between loses votes and log entries the node already told
//     its peers about.
//
// One more detail: etcd raft randomizes election timeouts with crypto/rand,
// which no seed can control. With DriveElections set, raft's own election
// timer is effectively disabled and elections are started by a seeded SimGo
// timer through the public Campaign API, which keeps runs reproducible.
package raftsim

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/Garvit-Jethwani/simgo/sim"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Storage selects how a node persists raft state.
type Storage int

const (
	SyncEveryReady Storage = iota // correct: fsync before sending
	BatchedFsync                  // bug: fsync on a timer
)

func (s Storage) String() string {
	if s == BatchedFsync {
		return "batched-fsync"
	}
	return "sync-every-ready"
}

// Options configures the simulated cluster.
type Options struct {
	Nodes   int
	Storage Storage
	// DriveElections starts elections from a seeded SimGo timer. Without it,
	// raft uses its own crypto/rand election timeouts and runs are not
	// reproducible (SimGo detects this).
	DriveElections bool
}

const (
	tickEvery      = 100 * time.Millisecond
	fsyncEvery     = 50 * time.Millisecond
	proposeEvery   = 300 * time.Millisecond
	electionMin    = 1000 * time.Millisecond
	electionJitter = 1000 * time.Millisecond

	hardStateKey = "hardstate"
	entryPrefix  = "entry/"
)

// Emitted event kinds checked by the invariants.
const (
	EmitLeader = "became-leader"
	EmitApply  = "applied"
)

// Leader is emitted when a node becomes leader.
type Leader struct{ Term uint64 }

// Applied is emitted when a node applies a committed entry.
type Applied struct {
	Index uint64
	Term  uint64
	Data  string
}

// Msg carries a raft message over the simulated network. The summary fields
// are what the trace shows; the raw encoded message is what the receiver
// steps into raft.
type Msg struct {
	Type    string
	Term    uint64
	LogTerm uint64 `json:",omitempty"`
	Index   uint64 `json:",omitempty"`
	Commit  uint64 `json:",omitempty"`
	Entries int    `json:",omitempty"`
	Reject  bool   `json:",omitempty"`
	raw     []byte
}

func (m Msg) SimName() string { return m.Type }

// Node is one raft member.
type Node struct {
	opt     Options
	id      uint64
	rn      *raft.RawNode
	storage *raft.MemoryStorage
	unsync  bool // writes since the last fsync (BatchedFsync)
	nextVal int
}

var discard = &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)}

func toRaftID(id sim.NodeID) uint64 { return uint64(id) + 1 }
func toSimID(id uint64) sim.NodeID  { return sim.NodeID(id - 1) }

// Start boots the node, recovering raft state from disk after a restart.
func (n *Node) Start(env *sim.Env) {
	n.id = toRaftID(env.ID())
	n.storage = raft.NewMemoryStorage()

	// Every node starts from the same snapshot containing the membership.
	voters := make([]uint64, n.opt.Nodes)
	for i := range voters {
		voters[i] = uint64(i) + 1
	}
	if err := n.storage.ApplySnapshot(pb.Snapshot{Metadata: pb.SnapshotMetadata{
		Index: 1, Term: 1, ConfState: pb.ConfState{Voters: voters},
	}}); err != nil {
		panic(err)
	}
	n.recover(env.Disk())

	electionTick := 10
	if n.opt.DriveElections {
		electionTick = 1 << 30 // raft's own election timer never fires
	}
	rn, err := raft.NewRawNode(&raft.Config{
		ID:              n.id,
		ElectionTick:    electionTick,
		HeartbeatTick:   1,
		Storage:         n.storage,
		Applied:         1,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 256,
		Logger:          discard,
	})
	if err != nil {
		panic(err)
	}
	n.rn = rn

	env.SetTimer("tick", tickEvery)
	env.SetTimer("propose", proposeEvery)
	if n.opt.DriveElections {
		n.resetElection(env)
	}
	if n.opt.Storage == BatchedFsync {
		env.SetTimer("fsync", fsyncEvery)
	}
}

func (n *Node) recover(d *sim.Disk) {
	if b, ok := d.Read(hardStateKey); ok {
		var hs pb.HardState
		if err := hs.Unmarshal(b); err != nil {
			panic(err)
		}
		if err := n.storage.SetHardState(hs); err != nil {
			panic(err)
		}
	}
	var ents []pb.Entry
	for _, k := range d.Keys() {
		if !strings.HasPrefix(k, entryPrefix) {
			continue
		}
		b, _ := d.Read(k)
		var e pb.Entry
		if err := e.Unmarshal(b); err != nil {
			panic(err)
		}
		ents = append(ents, e)
	}
	if len(ents) > 0 {
		if err := n.storage.Append(ents); err != nil {
			panic(err)
		}
	}
}

func (n *Node) resetElection(env *sim.Env) {
	jitter := time.Duration(env.Rand().Int64N(int64(electionJitter)))
	env.SetTimer("election", electionMin+jitter)
}

func (n *Node) Timer(env *sim.Env, name string) {
	switch name {
	case "tick":
		n.rn.Tick()
		env.SetTimer("tick", tickEvery)
	case "election":
		if n.rn.BasicStatus().RaftState != raft.StateLeader {
			_ = n.rn.Campaign()
		}
		n.resetElection(env)
	case "propose":
		if n.rn.BasicStatus().RaftState == raft.StateLeader {
			n.nextVal++
			val := fmt.Sprintf("n%d-v%d@%dms", n.id, n.nextVal, env.Now().Milliseconds())
			_ = n.rn.Propose([]byte(val))
		}
		env.SetTimer("propose", proposeEvery)
	case "fsync":
		if n.unsync {
			env.Disk().Sync()
			n.unsync = false
		}
		env.SetTimer("fsync", fsyncEvery)
	}
	n.handleReady(env)
}

func (n *Node) Receive(env *sim.Env, from sim.NodeID, msg sim.Message) {
	m, ok := msg.(Msg)
	if !ok {
		return
	}
	var rm pb.Message
	if err := rm.Unmarshal(m.raw); err != nil {
		panic(err)
	}
	// Hearing from the current leader postpones our next election.
	if n.opt.DriveElections && rm.From == n.rn.BasicStatus().Lead &&
		(rm.Type == pb.MsgApp || rm.Type == pb.MsgHeartbeat || rm.Type == pb.MsgSnap) {
		n.resetElection(env)
	}
	_ = n.rn.Step(rm)
	n.handleReady(env)
}

// handleReady persists, sends and applies, following the RawNode contract
// (except for when fsync happens in BatchedFsync mode).
func (n *Node) handleReady(env *sim.Env) {
	for n.rn.HasReady() {
		rd := n.rn.Ready()

		n.persist(env.Disk(), rd.HardState, rd.Entries)
		if !raft.IsEmptyHardState(rd.HardState) {
			if err := n.storage.SetHardState(rd.HardState); err != nil {
				panic(err)
			}
		}
		if len(rd.Entries) > 0 {
			if err := n.storage.Append(rd.Entries); err != nil {
				panic(err)
			}
		}

		if rd.SoftState != nil && rd.SoftState.RaftState == raft.StateLeader {
			env.Emit(EmitLeader, Leader{Term: n.rn.BasicStatus().Term})
		}

		for _, m := range rd.Messages {
			raw, err := m.Marshal()
			if err != nil {
				panic(err)
			}
			env.Send(toSimID(m.To), Msg{
				Type: m.Type.String(), Term: m.Term, LogTerm: m.LogTerm, Index: m.Index,
				Commit: m.Commit, Entries: len(m.Entries), Reject: m.Reject, raw: raw,
			})
		}

		for _, e := range rd.CommittedEntries {
			if e.Type == pb.EntryNormal && len(e.Data) > 0 {
				env.Emit(EmitApply, Applied{Index: e.Index, Term: e.Term, Data: string(e.Data)})
			}
		}
		n.rn.Advance(rd)
	}
}

func entryKey(index uint64) string { return entryPrefix + fmt.Sprintf("%020d", index) }

func (n *Node) persist(d *sim.Disk, hs pb.HardState, ents []pb.Entry) {
	if raft.IsEmptyHardState(hs) && len(ents) == 0 {
		return
	}
	if !raft.IsEmptyHardState(hs) {
		b, err := hs.Marshal()
		if err != nil {
			panic(err)
		}
		d.Write(hardStateKey, b)
	}
	if len(ents) > 0 {
		// New entries replace everything from their first index onward.
		first := ents[0].Index
		for _, k := range d.Keys() {
			if !strings.HasPrefix(k, entryPrefix) {
				continue
			}
			if idx, _ := strconv.ParseUint(strings.TrimPrefix(k, entryPrefix), 10, 64); idx >= first {
				d.Delete(k)
			}
		}
		for _, e := range ents {
			b, err := e.Marshal()
			if err != nil {
				panic(err)
			}
			d.Write(entryKey(e.Index), b)
		}
	}
	if n.opt.Storage == SyncEveryReady {
		d.Sync()
	} else {
		n.unsync = true // fsynced later by the "fsync" timer
	}
}

// Snapshot shows the node's raft state in the time-travel viewer.
func (n *Node) Snapshot() any {
	st := n.rn.Status()
	return struct {
		State     string
		Term      uint64
		Vote      uint64
		Lead      uint64
		Commit    uint64
		Applied   uint64
		LastIndex uint64
	}{
		State: st.RaftState.String(), Term: st.Term, Vote: st.Vote, Lead: st.Lead,
		Commit: st.Commit, Applied: st.Applied, LastIndex: n.lastIndex(),
	}
}

func (n *Node) lastIndex() uint64 {
	i, _ := n.storage.LastIndex()
	return i
}

// ---------------------------------------------------------------- invariants

// ElectionSafety: at most one leader per term (Raft paper, Figure 3).
var ElectionSafety = sim.Invariant{
	Name: "election safety: at most one leader per term",
	Check: func(w *sim.World) error {
		leaders := map[uint64]sim.NodeID{}
		for _, e := range w.Emitted(EmitLeader) {
			term := e.Value.(Leader).Term
			if prev, ok := leaders[term]; ok && prev != e.Node {
				return fmt.Errorf("term %d has two leaders: n%d and n%d", term, prev+1, e.Node+1)
			}
			leaders[term] = e.Node
		}
		return nil
	},
}

// StateMachineSafety: no two nodes ever apply different entries at the same
// log index (Raft paper, Figure 3). A node re-applying after a restart must
// apply the same entries again.
var StateMachineSafety = sim.Invariant{
	Name: "state machine safety: same entry applied at each index",
	Check: func(w *sim.World) error {
		at := map[uint64]sim.Emission{}
		for _, e := range w.Emitted(EmitApply) {
			a := e.Value.(Applied)
			if prev, ok := at[a.Index]; ok {
				if p := prev.Value.(Applied); p.Data != a.Data {
					return fmt.Errorf("index %d: n%d applied %q (term %d) but n%d applied %q (term %d)",
						a.Index, prev.Node+1, p.Data, p.Term, e.Node+1, a.Data, a.Term)
				}
				continue
			}
			at[a.Index] = e
		}
		return nil
	},
}

// Config returns a SimGo configuration for a raft cluster under crashes,
// pauses, partitions and message loss.
func Config(opt Options) sim.Config {
	if opt.Nodes == 0 {
		opt.Nodes = 3
	}
	return sim.Config{
		Nodes:    opt.Nodes,
		NewNode:  func(sim.NodeID) sim.Node { return &Node{opt: opt} },
		NodeName: func(id sim.NodeID) string { return fmt.Sprintf("raft-%d", id+1) },
		Duration: 30 * time.Second,
		Network: sim.NetworkConfig{
			MinLatency: time.Millisecond,
			MaxLatency: 10 * time.Millisecond,
			DropRate:   0.02,
		},
		Faults: sim.FaultConfig{
			Interval:    time.Second,
			Crashes:     true,
			Pauses:      true,
			Partitions:  true,
			MaxPause:    3 * time.Second,
			MaxDowntime: 3 * time.Second,
		},
		Invariants: []sim.Invariant{ElectionSafety, StateMachineSafety},
	}
}

// committed counts distinct applied indices, to check the cluster makes
// progress.
func committed(r *sim.Result) int {
	seen := map[uint64]bool{}
	for _, e := range r.Trace.Events {
		if e.Kind == sim.KindEmit && e.Detail == EmitApply {
			var a Applied
			if json.Unmarshal(e.Msg, &a) == nil {
				seen[a.Index] = true
			}
		}
	}
	return len(seen)
}
