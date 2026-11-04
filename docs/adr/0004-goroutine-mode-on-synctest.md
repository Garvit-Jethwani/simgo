# ADR 0004: Goroutine mode on testing/synctest, with measured reproducibility

- Status: accepted
- Date: 2026-11-04
- Design: [Phase 4 design](../design/phase4-goroutine-mode.md)

## Context

Most Go services start goroutines, block on sockets and sleep in real time,
so they cannot run on SimGo's single-threaded core (ADR 0001, ADR 0002)
without being rewritten. Go 1.25's `testing/synctest` gives a fake clock
that advances when all goroutines are blocked, but it does not control the
order of runnable goroutines or `select` choices.

A spike built a simulated TCP network on synctest (`x/simnet`) and measured
reproducibility over 30 runs per workload:

| Workload | Distinct logs |
|----------|---------------|
| Goroutines at distinct instants | 1 |
| Different hosts at the same instant (canonical log) | 1 |
| Same process, shared state, same instant | 30 |
| `select` with simultaneous ready cases | 30 |

## Decision

Build goroutine mode on `testing/synctest` plus `x/simnet`:

- per-host random streams and canonical log ordering, so concurrency
  across hosts does not affect reproducibility;
- seeded jitter on every instant SimGo controls (deliveries, faults,
  restarts), so ties come only from user timers;
- traces captured from the failing run itself, so debugging never depends
  on exact replay;
- every failing seed re-run N times, with the reproduction rate reported.

## Consequences

- Unmodified goroutine code can be tested, as long as it uses the
  simulated dialer, listener and disk.
- Reproducibility is exact for most runs and explicitly reported when not.
  Same-instant races inside a process are surfaced as findings rather than
  hidden as flakiness.
- The event-handler core remains the choice when exact replay is required.
- Source rewriting (the gosim approach) stays a possible future option if
  same-instant races turn out to be common in real code.
