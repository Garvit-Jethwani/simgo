# ADR 0001: Single-threaded deterministic simulation core

- Status: accepted
- Date: 2026-11-03

## Context

The point of SimGo is that a failing run can be replayed exactly. Go programs
are nondeterministic in several ways: the goroutine scheduler, the wall
clock, network timing, map iteration order, and randomness.

Options considered:

1. **Run real goroutines and hope for the best.** Fast to adopt, but a failing
   seed often does not reproduce, which defeats the purpose.
2. **Go 1.25 `testing/synctest`.** Gives virtual time and a way to wait until
   all goroutines in a bubble are blocked. It does not control the order in
   which runnable goroutines execute, and it does not simulate the network or
   disk.
3. **Source rewriting (the gosim approach).** Rewrites code to use a
   simulated runtime. Powerful, but complex, and it struggles with external
   dependencies.
4. **A single-threaded event loop that owns every source of
   nondeterminism.** The approach used by FoundationDB's simulator and
   TigerBeetle's VOPR.

## Decision

The core is option 4: one goroutine, a priority queue of events ordered by
`(virtual time, sequence number)`, and simulated clock, network, disk and
randomness, all derived from one seed.

## Consequences

- Same seed and same code produce a byte-identical trace. We verify this
  with a trace fingerprint (SHA-256).
- Runs are fast: no real sleeping, no locks, no context switches.
- Code under test must be written as event handlers (ADR 0002). This is a
  real adoption cost, mitigated by Phase 4 (synctest-based mode for
  goroutine code, with best-effort reproducibility).
- No data races are possible inside a simulation, so many simulations can
  run in parallel on separate goroutines without coordination.
