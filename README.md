# SimGo

**Deterministic simulation testing for distributed systems in Go, with a
time-travel debugger for every bug it finds.**

SimGo runs your whole cluster inside one `go test` process. It controls the
clock, the network, the disks and every random number, then throws crashes,
partitions, process pauses, lost packets and clock skew at your code
thousands of times per minute. When something breaks, you get:

- the **seed** that reproduces the exact same run, every time, and
- a **time-travel debugger**: a single HTML file showing every node, every
  message and every fault on a timeline you can scrub back and forth, with
  each node's state at every step.

Rust has [MadSim](https://github.com/madsim-rs/madsim) and
[Turmoil](https://github.com/tokio-rs/turmoil). SimGo aims to bring the same
technique to Go.

> Status: early development. See the [roadmap](docs/ROADMAP.md).

## Quick look

```go
func TestNoDoubleCharge(t *testing.T) {
    cfg := sim.Config{
        Nodes:   4,
        NewNode: newNode,
        Faults:  sim.FaultConfig{Interval: 2 * time.Second, Pauses: true, Crashes: true},
        Invariants: []sim.Invariant{{
            Name:  "each job is charged at most once",
            Check: noDoubleCharge,
        }},
    }
    simtest.Explore(t, cfg, simtest.Options{Seeds: 500})
}
```

```
--- FAIL: TestNoDoubleCharge
    seed 4182: invariant "each job is charged at most once" violated at 31.2s: job 7 charged twice
    trace:  /tmp/simgo/TestNoDoubleCharge-4182.html
    replay: SIMGO_SEED=4182 go test -run TestNoDoubleCharge ./examples/lease
```

## How it works

Read the [architecture document](docs/ARCHITECTURE.md) and the
[design decisions](docs/adr).

## Requirements

Go 1.25 or newer.

## License

Apache 2.0
