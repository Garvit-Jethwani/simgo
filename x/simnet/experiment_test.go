package simnet

// Phase 4 determinism experiment.
//
// Each workload runs many times with the same seed. We count how many
// distinct network logs come out, in recording order (raw) and in canonical
// order. One distinct fingerprint means the run is reproducible.
//
// Run with: go test -run TestDeterminismExperiment -v ./x/simnet
// Results are recorded in docs/design/phase4-goroutine-mode.md.

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

type workload struct {
	name string
	run  func(t *testing.T, nw *Network)
}

// tick sleeps until start + i*period. Goroutines given the same start and
// period wake at exactly the same virtual instants, whatever happened before.
func tick(start time.Time, i int, period time.Duration) {
	time.Sleep(time.Until(start.Add(time.Duration(i) * period)))
}

// client sends n requests on an absolute schedule, waiting for each reply.
func client(t *testing.T, nw *Network, name string, start time.Time, period time.Duration, n int) {
	c, err := nw.Host(name).Dial(context.Background(), "server:7000")
	if err != nil {
		t.Error(err)
		return
	}
	r := bufio.NewReader(c)
	for i := 0; i < n; i++ {
		tick(start, i+1, period)
		fmt.Fprintf(c, "%s %d\n", name, i)
		if _, err := r.ReadString('\n'); err != nil {
			t.Error(err)
			return
		}
	}
	c.Close()
}

func withEchoServer(t *testing.T, nw *Network, clients func()) {
	ln, err := nw.Host("server").Listen(7000)
	if err != nil {
		t.Fatal(err)
	}
	go echoServer(ln)
	clients()
}

var workloads = []workload{
	{
		// Each goroutine wakes at its own instants. Expected: reproducible.
		name: "sequential clients, distinct periods",
		run: func(t *testing.T, nw *Network) {
			withEchoServer(t, nw, func() {
				start := time.Now().Add(100 * time.Millisecond)
				var wg sync.WaitGroup
				wg.Go(func() { client(t, nw, "c1", start, 7*time.Millisecond, 40) })
				wg.Go(func() { client(t, nw, "c2", start.Add(time.Millisecond/3), 11*time.Millisecond, 40) })
				wg.Wait()
			})
		},
	},
	{
		// Two hosts wake at exactly the same instants and race to send.
		// The hosts share nothing, so only the recording order should vary.
		name: "independent clients, same instants",
		run: func(t *testing.T, nw *Network) {
			withEchoServer(t, nw, func() {
				start := time.Now().Add(100 * time.Millisecond)
				var wg sync.WaitGroup
				wg.Go(func() { client(t, nw, "c1", start, 10*time.Millisecond, 40) })
				wg.Go(func() { client(t, nw, "c2", start, 10*time.Millisecond, 40) })
				wg.Wait()
			})
		},
	},
	{
		// Two goroutines in ONE process wake at the same instant and race
		// on shared state (a mutex-protected counter), then send the value
		// they got. A real race in the code under test.
		name: "shared state, same instants",
		run: func(t *testing.T, nw *Network) {
			sink, _ := nw.Host("sink").Listen(1)
			go func() {
				for {
					c, err := sink.Accept()
					if err != nil {
						return
					}
					go func() { bufio.NewReader(c).WriteTo(discardWriter{}) }()
				}
			}()
			var mu sync.Mutex
			counter := 0
			start := time.Now().Add(100 * time.Millisecond)
			var wg sync.WaitGroup
			for w := 0; w < 2; w++ {
				wg.Go(func() {
					c, err := nw.Host("app").Dial(context.Background(), "sink:1")
					if err != nil {
						t.Error(err)
						return
					}
					for i := 0; i < 40; i++ {
						tick(start, i+1, 10*time.Millisecond)
						mu.Lock()
						counter++
						v := counter
						mu.Unlock()
						fmt.Fprintf(c, "%d\n", v)
					}
					c.Close()
				})
			}
			wg.Wait()
		},
	},
	{
		// One goroutine selects over two channels that become ready at the
		// same instant. Go picks a ready case at random.
		name: "select with simultaneous cases",
		run: func(t *testing.T, nw *Network) {
			sink, _ := nw.Host("sink").Listen(1)
			go func() {
				c, err := sink.Accept()
				if err == nil {
					bufio.NewReader(c).WriteTo(discardWriter{})
				}
			}()
			c, err := nw.Host("app").Dial(context.Background(), "sink:1")
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 40; i++ {
				a, b := time.After(5*time.Millisecond), time.After(5*time.Millisecond)
				select {
				case <-a:
					fmt.Fprintln(c, "a")
					<-b
				case <-b:
					fmt.Fprintln(c, "b")
					<-a
				}
			}
			c.Close()
		},
	},
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// measure runs w `runs` times with GOMAXPROCS=procs and returns the number
// of distinct raw and canonical fingerprints.
func measure(t *testing.T, w workload, procs, runs int) (raw, canonical int) {
	prev := runtime.GOMAXPROCS(procs)
	defer runtime.GOMAXPROCS(prev)
	rawSeen, canonSeen := map[string]bool{}, map[string]bool{}
	for i := 0; i < runs; i++ {
		synctest.Test(t, func(t *testing.T) {
			nw := New(Config{Seed: 42})
			w.run(t, nw)
			rawSeen[nw.RawFingerprint()] = true
			canonSeen[nw.Fingerprint()] = true
			nw.Close()
		})
	}
	return len(rawSeen), len(canonSeen)
}

func TestDeterminismExperiment(t *testing.T) {
	const runs = 30
	procs := []int{1, runtime.NumCPU()}
	t.Logf("%-40s %-10s %s", "workload", "GOMAXPROCS", "distinct logs in 30 runs (raw / canonical)")
	results := map[string][2]int{}
	for _, w := range workloads {
		for _, p := range procs {
			raw, canon := measure(t, w, p, runs)
			t.Logf("%-40s %-10d %d / %d", w.name, p, raw, canon)
			if p == procs[len(procs)-1] {
				results[w.name] = [2]int{raw, canon}
			}
		}
	}
	// The first two workloads must be reproducible in canonical form; that
	// is the property the design relies on. The last two are expected to
	// vary and are only reported.
	for _, name := range []string{workloads[0].name, workloads[1].name} {
		if c := results[name][1]; c != 1 {
			t.Errorf("%q: %d distinct canonical logs, want 1", name, c)
		}
	}
}

var _ net.Conn = (*conn)(nil)
