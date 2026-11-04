# Show HN submission (draft)

**Title** (80 characters max):

> Show HN: SimGo – Deterministic simulation testing for Go with a time-travel debugger

**URL:** https://github.com/Garvit-Jethwani/simgo

**Text:**

I built SimGo to bring FoundationDB/TigerBeetle-style deterministic
simulation testing to Go. It runs a whole cluster inside `go test`,
controls the clock, network, disk and randomness from one seed, and injects
crashes, partitions, process pauses, packet loss and clock skew. Every
failure comes with the seed that replays it and a single HTML file that lets
you scrub backwards through every node, message and fault.

Things it found while I was building the examples:

- Running the real etcd raft library with fsync batched on a timer (a
  common optimisation that breaks raft's storage contract): two leaders in
  one term, and different data committed at the same index. Used
  correctly, raft passed 300 seeds.
- The determinism check flagged etcd raft's election timeouts, which come
  from crypto/rand. Elections are now driven through raft's public Campaign
  API instead, and every seed replays exactly.
- A lease-based payment runner charging customers twice, from two distinct
  bugs (stale leader, lost acknowledgement).
- Lost acknowledged writes in a primary/backup service written with plain
  net/http, via an experimental goroutine mode built on Go 1.25's
  testing/synctest.

The part I would most like feedback on is goroutine mode. Go's scheduler is
not seedable, so I measured where reproducibility breaks: runs are
byte-identical unless two goroutines of the same process race at the same
virtual instant. SimGo re-runs every failure and reports the reproduction
rate instead of pretending it is always exact. The design doc has the
experiment: docs/design/phase4-goroutine-mode.md

There is also a hosted mode (Lambda, SQS, Fargate Spot, Terraform) that
shards millions of seeds and groups failures into distinct bugs; it runs
locally too.

It is v0.1: event-handler mode is the solid part; goroutine mode is
experimental.

---

**Posting notes**

- Post on a weekday morning US Eastern time.
- Be around for the first two hours to answer questions.
- Likely questions, with short answers:
  - *How is this different from Jepsen?* Jepsen tests a real deployed
    cluster from the outside, without replay. SimGo runs your code inside a
    simulation, so failures replay exactly and run thousands of times faster.
  - *Why not Antithesis?* Antithesis is a commercial, language-agnostic
    platform with a deterministic hypervisor. SimGo is an open-source Go
    library you run in `go test`.
  - *Does it work on my existing code?* If it is a state machine (like etcd
    raft), yes. Goroutine code works in the experimental mode if it takes
    its listener, dialer and disk from SimGo.
