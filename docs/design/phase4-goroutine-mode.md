# Phase 4 design: running ordinary goroutine-based Go code

- Status: spike complete, design proposed
- Date: 2026-11-04
- Prototype: [`x/simnet`](../../x/simnet)
- Decision record: [ADR 0004](../adr/0004-goroutine-mode-on-synctest.md)

## Problem

SimGo's core (Phases 1 to 3) is exactly reproducible because node code is
written as event handlers that never block or start goroutines
([ADR 0002](../adr/0002-event-handler-nodes.md)). That works for libraries
built as state machines, like etcd raft, but most Go services look like this:

```go
ln, _ := net.Listen("tcp", ":7000")
for {
    conn, _ := ln.Accept()
    go handle(conn)                   // a goroutine per connection
}
...
time.Sleep(backoff)                   // real time
select { case <-ctx.Done(): ...; case msg := <-ch: ... }
```

Phase 4 asks: can SimGo test code like this **without rewriting it**, and
how reproducible would the results be?

## Building block: `testing/synctest` (Go 1.25)

`synctest.Test` runs a function in a *bubble*:

- the `time` package uses a fake clock inside the bubble;
- fake time advances only when every goroutine in the bubble is **durably
  blocked**: on a channel created in the bubble, `sync.Cond.Wait`,
  `time.Sleep`, or a bubbled `WaitGroup`;
- mutexes, real network I/O and system calls are **not** durable blocking.

So a simulated network must be built from channels and `sync.Cond`, not real
sockets, and then simulated latency costs no real time.

What synctest does **not** do: control the order in which runnable
goroutines execute, or the case a `select` picks when several are ready.
Both use the Go runtime's internal randomness, which no library can seed.

## The prototype: `x/simnet`

A simulated TCP network that hands code real `net.Conn` and `net.Listener`
values:

| Feature | Behaviour |
|---------|-----------|
| `Host.Listen`, `Host.Dial` | Drop-in replacements for `net.Listen` / `net.Dial` |
| Latency | Seeded, per host, in `[MinLatency, MaxLatency)` plus sub-millisecond jitter |
| Ordering | Per-direction FIFO, like TCP, even with random latencies |
| Deadlines | `SetReadDeadline` / `SetDeadline` work with fake time |
| Partitions | Data across a cut is held (like TCP retransmission) and delivered on heal |
| Crash | Closes the host's listeners and connections; peers read EOF; in-flight data to the host is dropped; dials time out |
| Log | Every dial, accept, send, deliver, drop, partition and crash, with payload hashes |

Two design choices exist only to make results reproducible:

1. **Per-host random streams.** Latencies come from a stream seeded by
   `(seed, host name)`, so the order in which hosts act cannot change the
   latencies they get. Connection IDs come from per-host dial counters for
   the same reason.
2. **Canonical log order.** Events at the same virtual instant on different
   connections are independent; the fingerprint sorts them by
   `(time, connection, kind, sequence)` instead of recording order.

Five functional tests cover echo with latency bounds, ordering of 200 writes
under random latency, partition and heal with read deadlines, crash and
restart, and refused dials. They pass under the race detector.

### Lessons from building it

Two bugs in the first version are now design rules:

- **Fake time stops when the bubble's root goroutine returns.** Anything
  still in flight never arrives, and goroutines waiting for it deadlock.
  `Network.Close()` crashes every host (unblocking all reads and accepts)
  and then calls `synctest.Wait()`. Every test must call it.
- **In-flight data from a crashed sender still arrives.** A crashed
  process's sockets are closed by the OS. Only a partition or a down
  *receiver* stops delivery. The first version held a crashed host's FIN
  forever, so peers never saw EOF.

## The experiment

[`experiment_test.go`](../../x/simnet/experiment_test.go) runs four workloads
30 times each with the same seed, at `GOMAXPROCS=1` and `GOMAXPROCS=4`, and
counts distinct network logs. One distinct log means reproducible.

| Workload | What happens | Raw | Canonical |
|----------|--------------|-----|-----------|
| Sequential clients, distinct periods | Every goroutine wakes at its own instants | 1 | **1** |
| Independent clients, same instants | Two **different hosts** wake at exactly the same instants and send | 2 | **1** |
| Shared state, same instants | Two goroutines in **one process** wake together and race on a mutex-protected counter | 30 | **30** |
| `select` with simultaneous cases | Two timers fire at the same instant | 30 | **30** |

Results were identical at `GOMAXPROCS=1` and `4`, and with the race detector.

A first version of the experiment reported the "same instants" workloads as
fully reproducible. They were not testing what they claimed: each goroutine
dialed first, and the random dial latency offset their timers so they never
actually woke together. Ties are now forced with an absolute schedule.

### What the numbers mean

Nondeterminism enters at exactly one point: **two goroutines of the same
process becoming runnable at the same virtual instant and then interacting**
(shared memory, or a `select` with several ready cases).

- Concurrency **across hosts** is not a problem. Hosts share no memory, and
  per-host randomness plus canonical ordering make their interleaving
  irrelevant.
- Concurrency **within a host at different instants** is not a problem.
  synctest only advances time when everything is blocked, so each instant's
  work finishes before the next instant begins.
- `GOMAXPROCS=1` does **not** help. Scheduling order and `select` choices
  are randomized by the runtime even on one CPU.

## Options considered

| Option | Reproducibility | Code changes needed | Effort |
|--------|-----------------|---------------------|--------|
| A. synctest + simnet, best effort | Exact unless same-instant races; measured and reported | Use `Host.Dial` / `Host.Listen`, run inside a bubble | Medium |
| B. Rewrite code to SimGo event handlers | Exact | Large | On the user |
| C. Source rewriting (gosim approach) | Exact, including goroutine order | None in theory, but external dependencies break | Very high |
| D. Patched Go runtime with a seedable scheduler | Exact | None | Very high; must track every Go release |

## Decision

**Option A, with three rules that make "best effort" honest:**

1. **Debugging never depends on replay.** The trace and time-travel view
   come from the failing run itself. Replaying a seed is a convenience.
2. **Reproducibility is measured, never assumed.** Every failing seed is run
   again N times; the report says *"reproduced 9 of 10 times"* or *"did not
   reproduce: same-instant race in host X"*. Exact replays are labelled as
   such.
3. **Ties are made rare where SimGo controls time.** Network deliveries,
   fault injection and restarts get seeded sub-millisecond jitter, so the
   remaining ties come only from the user's own timers firing together,
   which is often a real race worth flagging.

Option C stays on the table if Phase 4 adoption shows that same-instant
races are common in real code.

## Phase 4 plan

1. **Process model.** A node is `func(ctx context.Context, env *Process)`.
   Crash cancels `ctx`, crashes the host's network, and waits for the
   node's goroutines to exit; restart calls the function again with fresh
   volatile state. Package-level variables are not reset; the docs will say
   so.
2. **Disk.** Reuse `sim.Disk` semantics behind a small file API (`Open`,
   `Write`, `Sync`, `Rename`), so unsynced writes are lost on crash.
3. **Nemesis in the bubble.** A goroutine driven by the seed injects
   partitions, crashes and restarts at jittered instants.
4. **Observations and invariants.** `Process.Emit` is goroutine-safe;
   invariants are checked on emissions only, at the end of each instant,
   so they never race with code under test.
5. **Trace.** Convert the simnet log and emissions into `sim.Trace`, so the
   existing time-travel viewer works unchanged.
6. **Reproduction report** as described in the decision.
7. **Showcase.** A small HTTP-based service with retries and a leader
   lease, tested unmodified apart from its dialer and listener.

## Limitations to state up front

- Goroutines cannot be killed from outside. Crashed nodes must stop when
  their context is cancelled or their connections fail.
- Same-instant races inside a process are not reproducible. They are
  detected and reported.
- Real I/O (files, DNS, other sockets) escapes the simulation and breaks
  fake time; code must use the simulated network and disk.
- Requires Go 1.25 and must run inside `go test` (`synctest.Test` takes a
  `*testing.T`).
