package viewer_test

import (
	"log"
	"time"

	"github.com/Garvit-Jethwani/simgo/sim"
	"github.com/Garvit-Jethwani/simgo/viewer"
)

type idle struct{}

func (idle) Start(*sim.Env)                            {}
func (idle) Receive(*sim.Env, sim.NodeID, sim.Message) {}
func (idle) Timer(*sim.Env, string)                    {}

// Write any run's trace as a self-contained HTML time-travel debugger.
func ExampleWriteFile() {
	r := sim.Run(sim.Config{Seed: 1, Nodes: 2, NewNode: func(sim.NodeID) sim.Node { return idle{} }, Duration: time.Second})
	if err := viewer.WriteFile("/tmp/simgo/example.html", r.Trace, viewer.Options{Title: "example"}); err != nil {
		log.Fatal(err)
	}
}
