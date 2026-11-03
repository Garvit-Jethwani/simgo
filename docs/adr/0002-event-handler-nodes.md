# ADR 0002: Nodes are event handlers with an explicit environment

- Status: accepted
- Date: 2026-11-03

## Context

ADR 0001 requires that node code never blocks, never spawns goroutines, and
never touches the real clock, network or disk. We need an API that makes
that natural rather than a list of rules people forget.

## Decision

A node implements three methods:

```go
Start(env *Env)
Receive(env *Env, from NodeID, msg Message)
Timer(env *Env, name string)
```

Everything the node can do goes through `*Env`: `Now`, `Send`, `SetTimer`,
`CancelTimer`, `Rand`, `Disk`, `Logf`, `Emit`.

- `Start` runs at boot and after every restart, so crash recovery is
  exercised constantly.
- `Emit` records externally visible actions (a card charged, a write
  acknowledged). Invariants are written against emissions, which keeps them
  about observable behaviour rather than internal state.
- Optional `Snapshot() any` exposes state to the time-travel viewer.

## Consequences

- The model matches existing deterministic designs: etcd's raft library
  (`Step` / `Ready`), TigerBeetle state machines, actor systems. Wrapping such
  libraries is straightforward.
- Code written with goroutines and blocking calls must be restructured, or
  wait for the Phase 4 adapter.
- Because each node gets its own random stream (seeded from the run seed,
  node ID and incarnation), adding a random call in one node does not shift
  the randomness every other node sees.
