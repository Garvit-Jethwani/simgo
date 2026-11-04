# Changelog

## v0.1.0 (2026-11-04)

First release.

### Core (`sim`, `simtest`, `viewer`)

- Deterministic single-threaded simulator: virtual clock, event queue,
  simulated network (latency, reordering, loss, duplication, partitions),
  simulated disk with fsync semantics, nemesis (crashes, restarts, pauses,
  partitions, clock skew), invariants checked after every step, node panics
  reported as violations.
- Traces with per-node state snapshots and a SHA-256 fingerprint; the same
  seed always produces the same trace.
- Self-contained HTML time-travel debugger: space-time diagram, scrubber,
  keyboard controls, state inspector, light and dark themes, phone layout.
- `simtest.Explore`: run many seeds from `go test`, write a trace per
  failure, print a one-line replay command, warn when node code is
  nondeterministic. `SIMGO_SEED`, `SIMGO_SEEDS`, `SIMGO_FIRST_SEED`,
  `SIMGO_OUT`, `SIMGO_MAX_REPORTED`, `SIMGO_REPORT` (JSON records).

### Goroutine mode (experimental: `x/simnet`, `x/simproc`)

- Simulated TCP network on Go 1.25 `testing/synctest` with real `net.Conn`
  and `net.Listener` values.
- Processes with crash and restart, a crash-aware disk, seeded fault
  injection, invariants on emissions, traces in the same viewer, and a
  measured reproduction rate for every failure.

### Examples

- `examples/lease`: two double-charge bugs in a lease-based job runner.
- `examples/wal`: acknowledged writes lost with ack-before-fsync group commit.
- `examples/raft`: real etcd raft; batched fsync elects two leaders and
  loses committed data.
- `examples/httpkv`: lost acknowledged writes in a primary/backup service on
  unmodified `net/http`.

### Hosted exploration (`cloud`, `deploy/aws`)

- Jobs split into seed shards, run by workers that check out a commit and
  run `go test`; failures grouped into distinct bugs with their traces.
- AWS implementation: Lambda function URL API, SQS with a dead-letter
  queue, Fargate Spot workers scaling from zero, DynamoDB, private S3 with
  presigned links. Terraform, worker image, and a local mode that runs the
  whole pipeline without AWS.
