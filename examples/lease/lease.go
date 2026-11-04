// Package lease is a payment job runner that uses a lease for leader
// election, the way many real systems do with a database row.
//
// Node 0 is the database. Nodes 1..N are workers. A worker that holds the
// lease is the leader: it takes the next pending job, charges the customer
// (which takes a while, like calling a payment API), then marks the job done.
//
// The naive worker trusts its own view of the lease. If it is paused (a GC
// pause, a VM stall) between deciding to charge and actually charging, the
// lease expires, another worker takes over and charges the same job, and the
// customer is charged twice. The fenced worker sends every charge to the
// database with its lease epoch (a fencing token); the database rejects
// charges from stale leaders.
package lease

import (
	"fmt"
	"time"

	"github.com/Garvit-Jethwani/simgo/sim"
)

const (
	DB = sim.NodeID(0)

	LeaseTTL     = 2 * time.Second
	TickEvery    = 500 * time.Millisecond
	ChargeTakes  = 300 * time.Millisecond // time to call the payment provider
	DefaultJobs  = 40
	ChargedEvent = "charge"
)

// Messages.
type (
	Acquire  struct{}
	Granted  struct{ Epoch int }
	Denied   struct{ Holder sim.NodeID }
	NextJob  struct{}
	Job      struct{ ID int }
	MarkDone struct{ ID int }

	// Fenced mode only.
	ChargeReq    struct{ ID, Epoch int }
	ChargeResult struct {
		ID int
		OK bool
	}
)

// Charge is the externally visible side effect: money moved.
type Charge struct {
	Job int
	By  string
}

// ---------------------------------------------------------------- database

// Database owns the lease and the job table. It is a protected node: it
// never crashes or pauses, like a managed database.
type Database struct {
	Jobs        int
	Holder      sim.NodeID
	Epoch       int
	ExpiresAtMs int64
	Done        map[int]bool
}

func (d *Database) Start(env *sim.Env)              {}
func (d *Database) Timer(env *sim.Env, name string) {}

func (d *Database) leaseValid(env *sim.Env) bool {
	return d.Holder != 0 && env.Now().Milliseconds() < d.ExpiresAtMs
}

func (d *Database) Receive(env *sim.Env, from sim.NodeID, msg sim.Message) {
	switch m := msg.(type) {
	case Acquire:
		if d.leaseValid(env) && d.Holder != from {
			env.Send(from, Denied{Holder: d.Holder})
			return
		}
		if d.Holder != from || !d.leaseValid(env) {
			d.Epoch++ // new lease term
		}
		d.Holder = from
		d.ExpiresAtMs = (env.Now() + LeaseTTL).Milliseconds()
		env.Send(from, Granted{Epoch: d.Epoch})

	case NextJob:
		for id := 1; id <= d.Jobs; id++ {
			if !d.Done[id] {
				env.Send(from, Job{ID: id})
				return
			}
		}

	case MarkDone:
		d.Done[m.ID] = true

	case ChargeReq:
		// Fencing: only the current lease term may charge, and only once.
		ok := m.Epoch == d.Epoch && !d.Done[m.ID]
		if ok {
			env.Emit(ChargedEvent, Charge{Job: m.ID, By: fmt.Sprintf("n%d", from)})
			d.Done[m.ID] = true
		} else {
			env.Logf("rejected charge for job %d from epoch %d (current epoch %d)", m.ID, m.Epoch, d.Epoch)
		}
		env.Send(from, ChargeResult{ID: m.ID, OK: ok})
	}
}

func (d *Database) Snapshot() any { return d }

// ------------------------------------------------------------------ worker

// Worker is a job runner. Its state is volatile: a restart forgets the lease.
type Worker struct {
	Fenced bool

	Leader       bool
	Epoch        int
	LeaseUntilMs int64 // by this worker's local clock
	CurrentJob   int
}

func (w *Worker) Start(env *sim.Env) {
	env.SetTimer("tick", TickEvery)
}

func (w *Worker) holdsLease(env *sim.Env) bool {
	return w.Leader && env.Now().Milliseconds() < w.LeaseUntilMs
}

func (w *Worker) Timer(env *sim.Env, name string) {
	switch name {
	case "tick":
		env.Send(DB, Acquire{}) // acquire or renew
		if w.holdsLease(env) && w.CurrentJob == 0 {
			env.Send(DB, NextJob{})
		}
		env.SetTimer("tick", TickEvery)

	case "charge":
		job := w.CurrentJob
		if w.Fenced {
			env.Send(DB, ChargeReq{ID: job, Epoch: w.Epoch})
			return
		}
		// BUG: the lease was checked before the slow charge started, not
		// now. If this worker was paused in between, another worker may
		// already own the lease and have charged this job.
		env.Emit(ChargedEvent, Charge{Job: job, By: fmt.Sprintf("n%d", env.ID())})
		env.Send(DB, MarkDone{ID: job})
		w.CurrentJob = 0
	}
}

func (w *Worker) Receive(env *sim.Env, from sim.NodeID, msg sim.Message) {
	switch m := msg.(type) {
	case Granted:
		w.Leader = true
		w.Epoch = m.Epoch
		// The lease started at the DB slightly before this message arrived,
		// so measuring from now is already optimistic.
		w.LeaseUntilMs = (env.Now() + LeaseTTL).Milliseconds()
	case Denied:
		w.Leader = false
	case Job:
		if w.holdsLease(env) && w.CurrentJob == 0 {
			w.CurrentJob = m.ID
			env.SetTimer("charge", ChargeTakes)
		}
	case ChargeResult:
		if m.ID == w.CurrentJob {
			w.CurrentJob = 0
		}
	}
}

func (w *Worker) Snapshot() any { return w }

// ------------------------------------------------------------------ config

// Config returns a simulation of one database and the given number of
// workers under pauses, crashes, partitions and clock skew.
func Config(workers int, fenced bool) sim.Config {
	return sim.Config{
		Nodes: workers + 1,
		NewNode: func(id sim.NodeID) sim.Node {
			if id == DB {
				return &Database{Jobs: DefaultJobs, Done: map[int]bool{}}
			}
			return &Worker{Fenced: fenced}
		},
		NodeName: func(id sim.NodeID) string {
			if id == DB {
				return "db"
			}
			return fmt.Sprintf("worker-%d", id)
		},
		Duration: time.Minute,
		Network: sim.NetworkConfig{
			MinLatency: time.Millisecond,
			MaxLatency: 20 * time.Millisecond,
			DropRate:   0.01,
		},
		Faults: sim.FaultConfig{
			Interval:    2 * time.Second,
			Pauses:      true,
			Crashes:     true,
			Partitions:  true,
			ClockSkew:   true,
			MaxPause:    5 * time.Second,
			MaxSkew:     time.Second,
			MaxDowntime: 4 * time.Second,
			Protected:   []sim.NodeID{DB},
		},
		Invariants: []sim.Invariant{NoDoubleCharge},
	}
}

// NoDoubleCharge fails if any job has been charged more than once.
var NoDoubleCharge = sim.Invariant{
	Name: "each job is charged at most once",
	Check: func(w *sim.World) error {
		seen := map[int]string{}
		for _, e := range w.Emitted(ChargedEvent) {
			c := e.Value.(Charge)
			if first, dup := seen[c.Job]; dup {
				if first == c.By {
					// The same worker charged again: its "done" was lost.
					return fmt.Errorf("job %d charged twice by the same worker %s", c.Job, c.By)
				}
				// Two workers both believed they were the leader.
				return fmt.Errorf("job %d charged by two workers: first %s, then %s", c.Job, first, c.By)
			}
			seen[c.Job] = c.By
		}
		return nil
	},
}
