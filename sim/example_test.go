package sim_test

import (
	"errors"
	"fmt"
	"time"

	"github.com/Garvit-Jethwani/simgo/sim"
)

// ticker counts timer ticks, one per simulated second.
type ticker struct{ Ticks int }

func (t *ticker) Start(env *sim.Env)                                   { env.SetTimer("tick", time.Second) }
func (t *ticker) Receive(env *sim.Env, from sim.NodeID, m sim.Message) {}
func (t *ticker) Timer(env *sim.Env, name string) {
	t.Ticks++
	env.SetTimer("tick", time.Second)
}

// A simulation runs in virtual time: a minute of simulated time takes
// microseconds, and an invariant is checked after every step.
func Example() {
	r := sim.Run(sim.Config{
		Seed:     1,
		Nodes:    1,
		NewNode:  func(sim.NodeID) sim.Node { return &ticker{} },
		Duration: time.Minute,
		Invariants: []sim.Invariant{{
			Name: "fewer than 5 ticks",
			Check: func(w *sim.World) error {
				if w.Node(0).(*ticker).Ticks >= 5 {
					return errors.New("ticked 5 times")
				}
				return nil
			},
		}},
	})
	fmt.Println(r.Violation)
	// Output: invariant "fewer than 5 ticks" violated at 5s: ticked 5 times
}

// The same seed always produces the same trace, so a failure can be replayed
// exactly.
func ExampleResult_Fingerprint() {
	cfg := sim.Config{
		Seed:     42,
		Nodes:    3,
		NewNode:  func(sim.NodeID) sim.Node { return &ticker{} },
		Duration: 10 * time.Second,
		Faults:   sim.FaultConfig{Interval: time.Second, Crashes: true, Partitions: true},
	}
	a, b := sim.Run(cfg), sim.Run(cfg)
	fmt.Println(a.Fingerprint() == b.Fingerprint())
	// Output: true
}

// Disk writes survive a crash only after Sync.
func ExampleDisk() {
	d := sim.NewDisk()
	d.Write("a", []byte("synced"))
	d.Sync()
	d.Write("b", []byte("not synced"))
	lost := d.Crash()
	_, hasA := d.Read("a")
	_, hasB := d.Read("b")
	fmt.Println(lost, hasA, hasB)
	// Output: 1 true false
}
