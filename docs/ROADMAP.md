# SimGo roadmap

Work is split into phases. Each phase ends with something usable, and each
task lands as its own commit.

## Phase 0: Foundations
- [x] Repository layout, Go module, license
- [x] Architecture document and diagrams
- [x] Architecture decision records (ADRs)
- [x] CI: build, vet, test (race detector), determinism check

## Phase 1: Simulation core (`sim`)
- [x] Virtual clock and deterministic event queue
- [x] Simulated network: latency, reordering, loss, duplication, partitions
- [x] Simulated disk with fsync semantics (unsynced writes lost on crash)
- [x] Nemesis: random crashes, restarts, pauses, partitions, clock skew
- [x] Invariants checked after every step; node panics reported as violations
- [x] Trace recording with per-node state snapshots
- [x] Tests: determinism (same seed gives the same trace hash), network, disk, faults

## Phase 2: Time-travel debugger (`viewer`, `simtest`)
- [x] Self-contained HTML viewer: space-time diagram, scrubber, state inspector
- [x] `simtest.Explore`: run many seeds from `go test`, write a trace for each failure
- [x] Replay a single seed with `SIMGO_SEED=<n>`

## Phase 3: Showcase examples
- [x] Lease-based job runner: SimGo finds the double-charge bug, the fenced version passes
- [x] Run the real etcd raft library: correct storage passes; batched fsync elects two leaders and loses committed data
- [x] Find the classic "acknowledged before fsync" bug in a write-ahead log with group commit

## Phase 4: Real Go code mode ([design](design/phase4-goroutine-mode.md))
- [x] Spike: simulated TCP network on Go 1.25 `testing/synctest` (`x/simnet`): real `net.Conn` / `net.Listener`, latency, FIFO order, deadlines, partitions, crashes
- [x] Experiment: measure reproducibility of goroutine code (exact unless two goroutines of one process race at the same instant)
- [x] Design document and ADR 0004
- [ ] Process model: node as `func(ctx, *Process)`, crash cancels and waits, restart re-runs
- [ ] Simulated file API on `sim.Disk` semantics (Open, Write, Sync, Rename)
- [ ] Seeded nemesis goroutine with jittered fault instants
- [ ] Goroutine-safe `Emit` and invariants checked between instants
- [ ] Convert simnet logs to `sim.Trace` so the time-travel viewer works unchanged
- [ ] Reproduction report: re-run failing seeds and report the rate
- [ ] Showcase: an unmodified goroutine-based service with retries and a lease

## Phase 5: Hosted exploration on AWS
- [ ] Run millions of seeds on Spot capacity (AWS Batch)
- [ ] Store failing traces in S3, browse them in a hosted viewer
- [ ] GitHub App: comment on PRs with new failures and a viewer link

## Phase 6: Launch
- [ ] Demo video and GIF of the viewer
- [ ] Blog post: "Finding a double-charge bug in 40 lines of Go"
- [ ] Show HN
