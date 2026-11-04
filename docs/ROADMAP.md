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

## Phase 4: Real Go code mode
- [ ] Run goroutine-based code inside Go 1.25 `testing/synctest` bubbles
- [ ] Simulated `net.Conn` / `net.Listener` so unmodified code can talk over the simulated network
- [ ] Simulated filesystem behind an `fs`-style interface

## Phase 5: Hosted exploration on AWS
- [ ] Run millions of seeds on Spot capacity (AWS Batch)
- [ ] Store failing traces in S3, browse them in a hosted viewer
- [ ] GitHub App: comment on PRs with new failures and a viewer link

## Phase 6: Launch
- [ ] Demo video and GIF of the viewer
- [ ] Blog post: "Finding a double-charge bug in 40 lines of Go"
- [ ] Show HN
