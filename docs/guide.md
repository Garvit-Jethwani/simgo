# Writing SimGo tests

SimGo has two modes:

| | Event-handler mode (`sim`) | Goroutine mode (`x/simproc`, experimental) |
|---|---|---|
| Code under test | Written as event handlers (`Start`, `Receive`, `Timer`) | Ordinary Go: goroutines, `net/http`, `time.Sleep` |
| Replay | Always exact | Exact unless two goroutines of one process race at the same virtual instant; measured per failure |
| Faults | Crashes, pauses, partitions, clock skew, message loss and duplication | Crashes, partitions |
| Best for | Protocols and state machines (consensus, leases, replication logic), libraries like etcd raft | Services you do not want to restructure |

Both produce the same time-travel trace and run inside `go test`.

## Event-handler mode

### 1. Write nodes as event handlers

A node reacts to three things: booting, a message, a timer. Everything it
touches goes through `*sim.Env`.

```go
type Server struct {
    Counter int
}

type Inc struct{}
type Value struct{ N int }

func (s *Server) Start(env *sim.Env) {
    // Runs at boot and after every restart: recover durable state here.
    if b, ok := env.Disk().Read("counter"); ok {
        s.Counter, _ = strconv.Atoi(string(b))
    }
}

func (s *Server) Receive(env *sim.Env, from sim.NodeID, msg sim.Message) {
    if _, ok := msg.(Inc); ok {
        s.Counter++
        env.Disk().Write("counter", []byte(strconv.Itoa(s.Counter)))
        env.Disk().Sync()                       // without this, a crash loses it
        env.Emit("acked", s.Counter)            // an externally visible action
        env.Send(from, Value{N: s.Counter})
    }
}

func (s *Server) Timer(env *sim.Env, name string) {}

func (s *Server) Snapshot() any { return s }    // optional: state in the viewer
```

Rules that keep runs reproducible (SimGo detects violations and warns):

- no goroutines, no `time.Now`, no global `math/rand`; use `env.Now()`,
  `env.SetTimer`, `env.Rand()`;
- no real I/O; use `env.Send` and `env.Disk()`;
- do not let map iteration order decide what you send.

### 2. Describe what must always be true

Invariants run after every step. Write them against **emissions** (what the
outside world saw), not internal state:

```go
var acksAreUnique = sim.Invariant{
    Name: "acknowledged values are never reused",
    Check: func(w *sim.World) error {
        seen := map[int]bool{}
        for _, e := range w.Emitted("acked") {
            v := e.Value.(int)
            if seen[v] {
                return fmt.Errorf("value %d acknowledged twice", v)
            }
            seen[v] = true
        }
        return nil
    },
}
```

Make the message say *which kind* of failure happened. The hosted service
groups failures by message shape (numbers masked), so "charged twice by the
same worker" and "charged by two workers" are reported as two bugs, but
"job 7 charged twice" and "job 31 charged twice" as one.

### 3. Explore seeds from a test

```go
func TestCounterIsDurable(t *testing.T) {
    simtest.Explore(t, sim.Config{
        Nodes:   3,
        NewNode: newNode,
        Network: sim.NetworkConfig{MaxLatency: 20 * time.Millisecond, DropRate: 0.01},
        Faults: sim.FaultConfig{
            Interval:  time.Second,
            Crashes:   true,
            Pauses:    true,
            Partitions: true,
            ClockSkew: true,
            Protected: []sim.NodeID{0}, // models managed infrastructure
        },
        Invariants: []sim.Invariant{acksAreUnique},
    }, simtest.Options{Seeds: 500})
}
```

On failure, the output names the seed, the trace file and the replay
command:

```
simgo: seed 31: invariant "acknowledged values are never reused" violated at 4.2s: value 17 acknowledged twice
    trace:  /tmp/simgo/TestCounterIsDurable-seed-31.html
    replay: SIMGO_SEED=31 go test -run '^TestCounterIsDurable$' ./counter
```

### 4. Read the trace

Open the HTML file. The violation is selected; step backwards with `←`
(`Shift+←` jumps 10). Look for the fault that started it: a red ✕ (crash),
an orange bar (pause), a shaded band (partition), dashed red arrows
(dropped messages). The inspector shows every node's state at the selected
step.

### 5. Add a progress test

A "fix" that never acknowledges anything passes every safety invariant.
Run one seed without faults and assert work gets done. The examples all do
this (`TestFencedWorkerMakesProgress`, `TestSyncThenAckMakesProgress`).

## Goroutine mode (experimental)

### 1. Write the node as a normal program

The only change from production code: take the listener, the dialer and
the disk from `*simproc.Process`.

```go
func server(ctx context.Context, p *simproc.Process) error {
    ln, err := p.Listen(80)
    if err != nil {
        return err
    }
    srv := &http.Server{Handler: handler(p)}
    go func() { <-ctx.Done(); srv.Close() }()
    if err := srv.Serve(ln); err != http.ErrServerClosed && ctx.Err() == nil {
        return err
    }
    return nil
}

func client(ctx context.Context, p *simproc.Process) error {
    c := &http.Client{Timeout: time.Second, Transport: &http.Transport{DialContext: p.DialContext}}
    ...
    p.Emit("acked", key)
}
```

A crash cancels `ctx` and closes the node's connections; the node must
return when that happens. Goroutines cannot be killed from outside.

### 2. Explore

```go
func TestReplicationIsSafe(t *testing.T) {
    simproc.Explore(t, simproc.Config{
        Nodes: []simproc.Node{
            {Name: "primary", Main: primary},
            {Name: "backup", Main: backup},
            {Name: "client", Main: client, Protected: true},
        },
        Duration: 20 * time.Second,
        Faults:   simproc.Faults{Interval: 2 * time.Second, Crashes: true, Partitions: true},
        Invariants: []simproc.Invariant{ackedWritesReadable},
    }, simproc.Options{Options: simtest.Options{Seeds: 40}})
}
```

Every failing seed is re-run and its reproduction rate reported:

```
seed 20: invariant "acknowledged writes are readable after failover" violated at 6.54s: key k24 was acknowledged but backup does not have it
    reproduced: exactly, 5 of 5 replays
```

If a failure does not reproduce exactly, two goroutines of one process are
racing at the same virtual instant. The trace from the failing run is still
complete, so you can debug it either way.

## Environment variables

| Variable | Effect |
|----------|--------|
| `SIMGO_SEED=n` | Run only seed `n` and always write its trace |
| `SIMGO_SEEDS=n` | Explore `n` seeds instead of the test's default |
| `SIMGO_FIRST_SEED=n` | Start at seed `n` (used to shard exploration) |
| `SIMGO_OUT=dir` | Write traces to `dir` (default `$TMPDIR/simgo`) |
| `SIMGO_MAX_REPORTED=n` | Write traces and reports for the first `n` failures (default 3) |
| `SIMGO_REPORT=file` | Append a JSON record of every exploration to `file` |

## Testing third-party libraries

Libraries built as deterministic state machines run unmodified in
event-handler mode; [`examples/raft`](../examples/raft) runs etcd raft. Check
two things:

1. **Hidden randomness.** etcd raft draws election timeouts from
   `crypto/rand`; SimGo's determinism check flags it immediately. Move the
   decision into the simulation (there: elections are started from a seeded
   timer through the public `Campaign` API).
2. **Storage contracts.** Implement the library's durability requirements
   against `sim.Disk`, so crashes test them.

## Pinned seeds

A seed replays exactly, so pinning a seed that once failed is a good
regression test. But changing the simulator, the fault mix or a dependency
changes which seeds fail. Prefer tests that search a range
(`simtest.Search`) and pin seeds only for documentation, with a comment
explaining how to find new ones (see `examples/raft`).
