// Package httpkv is a primary/backup key-value service built on the standard
// library's net/http, tested with SimGo's goroutine mode (x/simproc).
//
// The servers are an ordinary http.Server and the clients an ordinary
// http.Client. The only SimGo-specific code is where the listener and the
// dialer come from (Process.Listen and Process.DialContext).
//
// The primary stores each write (fsynced) and replicates it to the backup.
// When the primary is unreachable, the client reads from the backup. The
// question is when the primary tells the client "OK":
//
//   - AsyncReplication (bug): reply immediately and replicate in the
//     background. If the primary crashes first, the backup never gets the
//     write, and a client failing over reads a write it was told was saved
//     as missing.
//   - SyncReplication (fix): reply only after the backup confirmed. A write
//     that cannot be replicated is not acknowledged.
package httpkv

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Garvit-Jethwani/simgo/x/simproc"
)

// Replication selects when the primary acknowledges a write.
type Replication int

const (
	SyncReplication Replication = iota
	AsyncReplication
)

func (r Replication) String() string {
	if r == AsyncReplication {
		return "async"
	}
	return "sync"
}

// Emitted event kinds.
const (
	EmitAcked = "acked"
	EmitRead  = "read"
)

// Read is emitted for every read the client makes of an acknowledged key.
type Read struct {
	Key   string
	Found bool
	From  string
}

// ----------------------------------------------------------------- servers

// store serves GET /kv/{key} from the node's disk.
func store(p *simproc.Process) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		v, ok, err := p.Disk().Read("kv/" + r.PathValue("key"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Write(v)
	})
	return mux
}

// save writes key=value durably.
func save(p *simproc.Process, key string, value []byte) error {
	d := p.Disk()
	if err := d.Write("kv/"+key, value); err != nil {
		return err
	}
	return d.Sync()
}

// serve runs handler on port 80 until ctx is cancelled.
func serve(ctx context.Context, p *simproc.Process, handler http.Handler) error {
	ln, err := p.Listen(80)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: handler}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	if err := srv.Serve(ln); err != http.ErrServerClosed && ctx.Err() == nil {
		return err
	}
	return nil
}

func httpClient(p *simproc.Process, timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout:   timeout,
		Transport: &http.Transport{DialContext: p.DialContext},
	}
}

// Primary accepts writes and replicates them to the backup.
func Primary(mode Replication) simproc.Main {
	return func(ctx context.Context, p *simproc.Process) error {
		replicaClient := httpClient(p, 500*time.Millisecond)
		defer replicaClient.CloseIdleConnections()

		replicate := func(ctx context.Context, key string, value []byte) error {
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
				"http://backup:80/replicate?key="+url.QueryEscape(key), strings.NewReader(string(value)))
			resp, err := replicaClient.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("backup replied %s", resp.Status)
			}
			return nil
		}

		mux := store(p)
		mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
			key := r.PathValue("key")
			value, _ := io.ReadAll(r.Body)
			if err := save(p, key, value); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			if mode == AsyncReplication {
				// BUG: acknowledge now, replicate later. If this node
				// crashes first, the backup never sees the write.
				go replicate(ctx, key, value)
				w.WriteHeader(http.StatusOK)
				return
			}
			if err := replicate(r.Context(), key, value); err != nil {
				http.Error(w, "replication failed: "+err.Error(), http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
		})
		return serve(ctx, p, mux)
	}
}

// Backup stores replicated writes and serves reads.
func Backup(ctx context.Context, p *simproc.Process) error {
	mux := store(p)
	mux.HandleFunc("POST /replicate", func(w http.ResponseWriter, r *http.Request) {
		value, _ := io.ReadAll(r.Body)
		if err := save(p, r.URL.Query().Get("key"), value); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return serve(ctx, p, mux)
}

// ------------------------------------------------------------------ client

// Client writes a new key every 100ms. After every successful write it reads
// back the most recently acknowledged key, from the primary if possible and
// from the backup otherwise (failover).
func Client(ctx context.Context, p *simproc.Process) error {
	c := httpClient(p, time.Second)
	defer c.CloseIdleConnections()
	lastAcked := ""

	get := func(host, key string) (found bool, ok bool) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+host+":80/kv/"+key, nil)
		resp, err := c.Do(req)
		if err != nil {
			return false, false
		}
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)
		switch resp.StatusCode {
		case http.StatusOK:
			return true, true
		case http.StatusNotFound:
			return false, true
		}
		return false, false
	}

	for i := 1; ; i++ {
		t := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}

		key := fmt.Sprintf("k%d", i)
		req, _ := http.NewRequestWithContext(ctx, http.MethodPut, "http://primary:80/kv/"+key, strings.NewReader(fmt.Sprintf("v%d", i)))
		if resp, err := c.Do(req); err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				p.Emit(EmitAcked, key)
				lastAcked = key
			}
		}

		if lastAcked == "" {
			continue
		}
		for _, host := range []string{"primary", "backup"} {
			if found, ok := get(host, lastAcked); ok {
				p.Emit(EmitRead, Read{Key: lastAcked, Found: found, From: host})
				break
			}
		}
	}
}

// AcknowledgedWritesAreReadable: a read of an acknowledged key, from either
// server, must find it.
var AcknowledgedWritesAreReadable = simproc.Invariant{
	Name: "acknowledged writes are readable after failover",
	Check: func(h *simproc.History) error {
		reads := h.Emitted(EmitRead)
		if len(reads) == 0 {
			return nil
		}
		r := reads[len(reads)-1].Value.(Read)
		if !r.Found {
			return fmt.Errorf("key %s was acknowledged but %s does not have it", r.Key, r.From)
		}
		return nil
	},
}

// Config returns a cluster of primary, backup and client with crashes and
// partitions.
func Config(mode Replication) simproc.Config {
	return simproc.Config{
		Nodes: []simproc.Node{
			{Name: "primary", Main: Primary(mode)},
			{Name: "backup", Main: Backup},
			{Name: "client", Main: Client, Protected: true},
		},
		Duration:   20 * time.Second,
		MinLatency: time.Millisecond,
		MaxLatency: 20 * time.Millisecond,
		Faults: simproc.Faults{
			Interval:    2 * time.Second,
			Crashes:     true,
			Partitions:  true,
			MaxDowntime: 2 * time.Second,
		},
		Invariants: []simproc.Invariant{AcknowledgedWritesAreReadable},
	}
}
