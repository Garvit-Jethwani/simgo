# SimGo

**Deterministic simulation testing for distributed systems in Go, with a
time-travel debugger for every bug it finds.**

SimGo runs your whole cluster inside one `go test` process. It controls the
clock, the network, the disks and every random number, then throws crashes,
partitions, process pauses, lost packets and clock skew at your code
thousands of times per minute. When something breaks, you get:

- the **seed** that reproduces exactly the same run, every time, and
- a **time-travel debugger**: one self-contained HTML file showing every
  node, message and fault on a timeline you can scrub back and forth, with
  each node's state at every step.

![SimGo time-travel debugger showing a double-charge bug](docs/images/viewer.png)

Rust has [MadSim](https://github.com/madsim-rs/madsim) and
[Turmoil](https://github.com/tokio-rs/turmoil). SimGo brings the same
technique to Go.

> Status: early development. See the [roadmap](docs/ROADMAP.md).

## What it has found so far

Three examples, each with a buggy and a fixed version. The buggy ones look
reasonable and pass ordinary tests.

| Example | Bug | SimGo result |
|---------|-----|--------------|
| [`examples/raft`](examples/raft): the real [etcd raft](https://github.com/etcd-io/raft) library | fsync batched on a 50ms timer instead of before sending (breaks raft's storage contract) | **Two leaders in the same term** (seed 404) and **different data committed at the same log index** (seed 1251), out of 3,000 seeds. In 376 more seeds raft's own corruption check fires. Correct storage passes 300 seeds. |
| [`examples/lease`](examples/lease): lease-based payment runner | Leader keeps charging after its lease expired (pause, partition, clock skew); lost "done" acknowledgement | A customer charged twice in 175 of 200 seeds, from two different bugs. The fenced version passes 500 seeds. |
| [`examples/wal`](examples/wal): key-value server with group commit | Write acknowledged before the fsync that covers it | Acknowledged writes lost on crash in 100 of 100 seeds. Acking after fsync passes 300 seeds with the same throughput. |

### etcd raft, under SimGo

etcd raft is a deterministic state machine (`Tick`, `Step`, `Ready`), so it
runs unmodified inside SimGo. Two things came out of it:

- **SimGo's determinism check caught raft's own randomness.** raft draws
  election timeouts from `crypto/rand`, so the same seed gave different runs.
  The example starts elections from a seeded SimGo timer through raft's
  public `Campaign` API instead, and every seed then replays exactly.
- **One "optimisation" breaks Raft's safety.** Batching fsyncs on a timer lets
  a crash erase a vote the node already cast, or log entries it already
  acknowledged. SimGo turns that into concrete, replayable runs:

```
seed 404: invariant "election safety: at most one leader per term" violated at 13.98s:
    term 7 has two leaders: n3 and n2
seed 1251: invariant "state machine safety: same entry applied at each index" violated at 17.50s:
    index 6: n3 applied "n3-v1@14992ms" (term 5) but n1 applied "n1-v2@14014ms" (term 4)
```

The raft example is a separate Go module, so SimGo itself has no
dependencies.

### The double-charge example

```
$ go test -v ./examples/lease
=== RUN   TestNaiveWorkerDoubleCharges
    found the bug in 175 of 200 seeds; first: seed 1
    invariant "each job is charged at most once" violated at 22.94s: job 28 charged twice: first by n3, again by n3
    trace: /tmp/simgo/TestNaiveWorkerDoubleCharges-seed-1.html
--- PASS: TestNaiveWorkerDoubleCharges (0.17s)
=== RUN   TestFencedWorkerNeverDoubleCharges
    simgo: 500 seeds explored, no violations
--- PASS: TestFencedWorkerNeverDoubleCharges (2.00s)
```

## Writing a SimGo test

Nodes are event handlers. Everything they touch (time, network, disk,
randomness) comes from `*sim.Env`, which is what makes runs reproducible.

```go
type Worker struct{ /* state */ }

func (w *Worker) Start(env *sim.Env)                                      { env.SetTimer("tick", time.Second) }
func (w *Worker) Timer(env *sim.Env, name string)                         { env.Send(dbNode, Acquire{}) }
func (w *Worker) Receive(env *sim.Env, from sim.NodeID, msg sim.Message)  { /* ... */ }
```

Describe the cluster, the faults, and what must always be true:

```go
func TestNoDoubleCharge(t *testing.T) {
    cfg := sim.Config{
        Nodes:   4,
        NewNode: newNode,
        Network: sim.NetworkConfig{MaxLatency: 20 * time.Millisecond, DropRate: 0.01},
        Faults: sim.FaultConfig{
            Interval: 2 * time.Second,
            Pauses: true, Crashes: true, Partitions: true, ClockSkew: true,
            Protected: []sim.NodeID{0}, // the database never crashes
        },
        Invariants: []sim.Invariant{{
            Name:  "each job is charged at most once",
            Check: noDoubleCharge, // inspects w.Emitted("charge")
        }},
    }
    simtest.Explore(t, cfg, simtest.Options{Seeds: 500})
}
```

When a seed fails, the test output tells you exactly how to look at it and
replay it:

```
seed 4182: invariant "each job is charged at most once" violated at 31.2s: ...
    trace:  /tmp/simgo/TestNoDoubleCharge-seed-4182.html
    replay: SIMGO_SEED=4182 go test -run '^TestNoDoubleCharge$' ./yourpkg
```

| Variable | Effect |
|----------|--------|
| `SIMGO_SEED=n` | Run only seed `n` and always write its trace |
| `SIMGO_SEEDS=n` | Explore `n` seeds instead of the test's default |
| `SIMGO_OUT=dir` | Write traces to `dir` (default `$TMPDIR/simgo`) |

## What SimGo simulates

| Fault | Effect |
|-------|--------|
| Network | Latency and reordering, message loss, duplication |
| Partitions | Nodes split into groups; in-flight messages across the split are lost |
| Crashes | Volatile state and timers are lost, **unsynced disk writes are lost**, the node restarts later |
| Pauses | A node freezes (GC pause, VM stall) while the rest of the cluster moves on |
| Clock skew | A node's local clock jumps forward or backward |

A 20-second simulation of 5 nodes under all of these faults runs in about
3 ms, around 7,000 times faster than real time.

## How it works

Read the [architecture document](docs/ARCHITECTURE.md) and the
[design decisions](docs/adr).

## Requirements

Go 1.25 or newer.

## License

[Apache 2.0](LICENSE)
