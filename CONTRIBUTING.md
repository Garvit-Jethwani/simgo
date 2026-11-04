# Contributing to SimGo

Thanks for your interest. Bug reports, examples of real bugs SimGo finds (or
misses), and pull requests are all welcome.

## Before you start

- For anything larger than a small fix, open an issue first so we can agree
  on the approach.
- Design changes get an ADR in `docs/adr/` (copy an existing one): context,
  decision, consequences.

## Development

Requirements: Go 1.25+, git. The cloud module also needs `make` and `zip`;
the Terraform checks need Terraform 1.6+.

The repository has three Go modules:

| Module | Directory | Dependencies |
|--------|-----------|--------------|
| Core library | `.` | none (keep it that way) |
| etcd raft example | `examples/raft` | etcd raft |
| Hosted service | `cloud` | AWS SDK, aws-lambda-go |

Run everything CI runs:

```bash
gofmt -l .
```

```bash
go vet ./... && go test -race -count=1 ./...
```

```bash
cd examples/raft && go vet ./... && go test -race -count=1 ./...
```

```bash
cd cloud && make test lambda
```

```bash
terraform fmt -check -recursive deploy
```

## Rules for changes to the simulator

These are what make SimGo trustworthy; reviews check them.

1. **Determinism is tested, not assumed.** Any change to `sim/` must keep
   `TestSameSeedSameTrace` and friends passing. If you add a source of
   randomness, derive it from the seed.
2. **Nothing run-specific may enter a trace.** Pointer values, goroutine
   IDs, wall-clock times and host paths make identical runs look different
   (see the panic-report fix in the history).
3. **Fault semantics are tested from the trace.** `TestFaultSemantics`
   replays traces and checks that nothing happens on crashed or paused nodes
   and nothing crosses a partition. Extend it with new fault kinds.
4. **Examples need a buggy version, a fixed version, and a progress test**,
   so a "fix" that does nothing cannot pass.
5. **Claims in docs come from runs.** If you change behaviour that a number
   in the README depends on, re-run and update the number.

## Commit messages

One logical change per commit. The subject says what changed
(`sim: ...`, `viewer: ...`, `docs: ...`); the body says why.
