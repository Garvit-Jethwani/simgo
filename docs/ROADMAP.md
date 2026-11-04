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
- [x] Process model: node as `func(ctx, *Process)`; crash cancels the context, closes connections and fences off the old incarnation; restart re-runs
- [x] Crash-aware disk on `sim.Disk` semantics (Write, Delete, Sync, Read, Keys)
- [x] Seeded nemesis goroutine with jittered fault instants
- [x] Goroutine-safe `Emit`; invariants checked on every emission and at the end
- [x] Convert simnet logs to `sim.Trace` so the time-travel viewer works unchanged
- [x] Reproduction report: re-run failing seeds and report the rate
- [x] Showcase: a primary/backup service on unmodified `net/http` (async replication bug found, fix verified)

## Phase 5: Hosted exploration on AWS ([deployment guide](deploy/aws.md))
- [x] Machine-readable results: `SIMGO_REPORT` JSON records, `SIMGO_FIRST_SEED` for sharding
- [x] Hosted core (`cloud/hosted`): job validation, sharding, worker, failure grouping, HTTP API
- [x] AWS storage (`cloud/awsstore`): SQS queue with DLQ, private S3 traces with presigned links, single-table DynamoDB with TTL
- [x] `simgo-cloud` CLI (local, worker, api, submit, status) and the API Lambda
- [x] Terraform (`deploy/aws`): Fargate Spot workers scaling 0..N on queue depth, Lambda function URL, least-privilege IAM
- [x] Worker image (arm64) and CI jobs for the cloud module, Terraform validation and the image build
- [ ] GitHub App: comment on PRs with new failures and trace links
- [ ] Private repositories without tokens in job records

## Phase 6: Launch
- [x] Demo GIF and screenshots of the viewer
- [x] Guide to writing SimGo tests, runnable godoc examples
- [x] CONTRIBUTING, CHANGELOG, SECURITY
- [x] Drafts: blog post, Show HN, LinkedIn post (`docs/launch/`)
- [ ] v0.1.0 release tags (`v0.1.0`, `cloud/v0.1.0`, `examples/raft/v0.1.0`) on commit `59afe53`
- [ ] Publish the blog post and Show HN

## Next
- [ ] Static analyzer for the determinism rules (goroutines, `time.Now`, global rand, map iteration in node code)
- [ ] Shrinking: reduce a failing run to the fewest faults that still break the invariant
- [ ] Viewer windowing for very long traces
- [ ] Goroutine mode: disk errors and network-level pauses
- [ ] Hosted: GitHub App with PR comments, private repositories without tokens in job records
