# Two Raft leaders in one term, and other bugs a simulator found in an afternoon

*Draft blog post for the SimGo v0.1 launch.*

Distributed systems fail in the gaps between events: a process pauses for a
garbage collection right after deciding something, a packet arrives after a
lease expired, a crash lands between a write and its fsync. These bugs are
rare in production, nearly impossible to reproduce, and invisible to unit
tests, which only check the orderings someone thought of.

FoundationDB and TigerBeetle are famous for testing differently: run the
whole cluster in one process, replace the clock, network and disk with
simulated ones driven by a single random seed, and inject faults thousands
of times faster than reality. When something breaks, the seed replays the
exact same run. Rust has libraries for this (MadSim, Turmoil). Go mostly
did not, so I built one: **SimGo**.

## What it looks like

A SimGo test reads like any Go test:

```go
func TestNoDoubleCharge(t *testing.T) {
    simtest.Explore(t, sim.Config{
        Nodes:   4,
        NewNode: newNode,
        Faults: sim.FaultConfig{
            Interval: 2 * time.Second,
            Pauses: true, Crashes: true, Partitions: true, ClockSkew: true,
        },
        Invariants: []sim.Invariant{noDoubleCharge},
    }, simtest.Options{Seeds: 500})
}
```

A minute of simulated time runs in milliseconds. When an invariant breaks,
the test prints the seed, a replay command, and the path to a single HTML
file: a time-travel debugger that shows every node, message and fault on a
timeline you can scrub backwards from the failure.

## Bug 1: the payment runner that charged customers twice

The first example is a pattern I have seen in many codebases: workers elect
a leader through a lease row in a database, and the leader charges pending
payments. It looks correct and passes ordinary tests.

SimGo found customers charged twice in 175 of 200 seeds, and the trace
showed **two different bugs**:

- **The stale leader.** A partition separates the leader from the
  database. The leader still believes in its lease and charges job 19. Its
  "done" never arrives, the lease expires, another worker takes over, sees
  job 19 still pending, and charges it again.
- **The lost acknowledgement.** No leadership change at all: a worker
  charges a job, its "done" message is dropped, and the database hands out
  the same job again.

I only knew about the first one when I wrote the example. The fix for both
is the same: a fencing token (the lease epoch) checked by the database,
which records the charge and the "done" flag in one step. The fixed version
passes 500 seeds, and a separate test makes sure it still charges jobs, so a
"fix" that does nothing cannot pass.

## Bug 2: two Raft leaders in one term

Next I ran the real etcd raft library inside SimGo. etcd raft is a
deterministic state machine (`Tick`, `Step`, `Ready`), so it runs
unmodified; the application persists state, sends messages and applies
entries.

Two findings:

**SimGo's determinism check caught raft's own randomness first.** Two runs
of the same seed diverged. The cause: raft draws election timeouts from
`crypto/rand`. The workaround keeps raft unmodified: its internal election
timer is effectively disabled and elections start from a seeded SimGo timer
through the public `Campaign` API. After that, every seed replays exactly.

**Then the real experiment.** Raft's storage contract is explicit: persist
HardState and entries before sending the messages from the same `Ready`. A
tempting optimisation is to write immediately but fsync on a timer, to batch
the expensive fsyncs. Used correctly, raft passed 300 seeds of crashes,
pauses, partitions and packet loss. With batched fsync, 3,000 seeds
produced:

- **two leaders in term 7** (seed 404): a node forgot the vote it had cast
  after a crash, and voted again;
- **different data committed at the same log index** (seed 1251): silent
  data loss;
- 376 runs where raft's own sanity check panicked with
  "tocommit(35) is out of range [lastIndex(34)]. Was the raft log corrupted,
  truncated, or lost?"

To be clear, this is not a bug in etcd raft. It is what happens when an
application breaks raft's storage rules, which is exactly the kind of
mistake that survives code review.

## Bug 3: a lost write in plain net/http code

Most Go services do not look like event handlers; they start goroutines,
block on sockets and call `time.Sleep`. For those, SimGo has an experimental
goroutine mode built on Go 1.25's `testing/synctest`: inside a "bubble", the
`time` package uses a fake clock that only advances when every goroutine is
blocked. SimGo adds a simulated TCP network that hands code real `net.Conn`
and `net.Listener` values.

The showcase is a primary/backup key-value service written with the
standard `http.Server` and `http.Client`. The only SimGo-specific code is
where the listener and dialer come from. With asynchronous replication (the
primary says "OK" and replicates in the background), SimGo found
acknowledged writes missing after failover in 5 of 40 seeds.

Goroutine mode cannot be exactly reproducible in every case, and I measured
where it breaks rather than guessing. Across four workloads, runs were
byte-identical except in one situation: two goroutines **of the same
process** becoming runnable at the same virtual instant and racing on shared
state (or a `select` with two ready cases). Go's scheduler randomizes those,
and no library can seed it. So SimGo re-runs every failing seed and reports
the rate. For the `net/http` service every failure replays byte-identically,
15 of 15, even under the race detector. It did not start that way: under the
race detector some replays diverged, and I first blamed `net/http`'s
internals. Diffing two replays showed the real culprit was my simulator: two
goroutines sending on *different* connections at the same instant shared one
random stream per host, so their order changed the latencies. One stream per
connection fixed it.

## Running a million seeds

A few hundred seeds per commit catches common bugs; rare ones need far more.
SimGo includes a hosted mode: a job is split into seed shards, workers on
Fargate Spot check out the commit and run `go test` for their range, and
failures are grouped into distinct bugs, each with its trace. It scales to
zero when idle, and the same pipeline runs locally:

```
2000 seeds of TestDemoNaiveWorker in 8 shards, 4 workers: 1669 failing seeds

  bug 1: in 880 seeds: job 19 charged by two workers: first n2, then n3
  bug 2: in 789 seeds: job 28 charged twice by the same worker n3
```

## What I learned building it

- **Determinism is a property you test.** Every run has a fingerprint (a
  hash of the full trace). That check caught etcd raft's `crypto/rand`, and
  four bugs in my own simulator: stack traces with pointer values in panic
  reports, a listener lookup racing a server's start, a dial that could
  outlive the simulation, and latencies shared across connections. When a
  run does not reproduce, diff the traces before blaming the code under
  test.
- **Write invariants against what the outside world saw**, not internal
  state: "each job is charged at most once", "acknowledged writes survive".
- **Always pair a safety test with a progress test.** A system that does
  nothing is perfectly safe.

## Limits

SimGo is v0.1. Event-handler mode requires code shaped as handlers.
Goroutine mode is experimental, cannot kill goroutines that ignore their
context, and only reproduces same-instant races probabilistically. It finds
bugs; it does not prove their absence.

The code, examples and design documents are at
**github.com/Garvit-Jethwani/simgo**. If you run it against your own system,
I would love to hear what it finds.
