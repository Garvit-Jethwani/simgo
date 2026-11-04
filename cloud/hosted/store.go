package hosted

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrNotFound is returned when a job does not exist.
var ErrNotFound = errors.New("not found")

// Delivery is a shard received from a queue.
type Delivery struct {
	Shard  Shard
	Handle string // queue-specific receipt, passed back to Delete
}

// Queue distributes shards to workers. Receive returns (nil, nil) when no
// shard arrived within the queue's wait time. A shard that is received but
// not deleted becomes visible again later (at-least-once delivery).
type Queue interface {
	Send(ctx context.Context, s Shard) error
	Receive(ctx context.Context) (*Delivery, error)
	Delete(ctx context.Context, d *Delivery) error
}

// Blobs stores trace files.
type Blobs interface {
	Put(ctx context.Context, key string, body []byte, contentType string) error
	// URL returns a link a person can open, valid for at least a few days.
	URL(ctx context.Context, key string) (string, error)
}

// Results stores jobs, shard outcomes and failures.
type Results interface {
	PutJob(ctx context.Context, j Job) error
	GetJob(ctx context.Context, id string) (Job, error)
	PutShardResult(ctx context.Context, r ShardResult) error
	ShardResults(ctx context.Context, jobID string) ([]ShardResult, error)
	PutFailure(ctx context.Context, f Failure) error
	Failures(ctx context.Context, jobID string) ([]Failure, error)
}

// ------------------------------------------------------------- in-memory

// MemoryQueue is an in-process queue for tests and local mode.
type MemoryQueue struct {
	mu       sync.Mutex
	items    []Shard
	inflight map[string]Shard
	next     int
	// Wait is how long Receive waits for a shard when the queue is empty.
	Wait time.Duration
}

func NewMemoryQueue() *MemoryQueue {
	return &MemoryQueue{inflight: map[string]Shard{}, Wait: 50 * time.Millisecond}
}

func (q *MemoryQueue) Send(_ context.Context, s Shard) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items = append(q.items, s)
	return nil
}

func (q *MemoryQueue) Receive(ctx context.Context) (*Delivery, error) {
	deadline := time.Now().Add(q.Wait)
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			s := q.items[0]
			q.items = q.items[1:]
			q.next++
			h := strconv.Itoa(q.next)
			q.inflight[h] = s
			q.mu.Unlock()
			return &Delivery{Shard: s, Handle: h}, nil
		}
		q.mu.Unlock()
		if time.Now().After(deadline) {
			return nil, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (q *MemoryQueue) Delete(_ context.Context, d *Delivery) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.inflight, d.Handle)
	return nil
}

// Requeue puts an in-flight shard back, as a visibility timeout would.
func (q *MemoryQueue) Requeue(d *Delivery) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if s, ok := q.inflight[d.Handle]; ok {
		delete(q.inflight, d.Handle)
		q.items = append(q.items, s)
	}
}

// Len returns the number of shards waiting or in flight.
func (q *MemoryQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items) + len(q.inflight)
}

// MemoryResults is an in-process result store for tests and local mode.
type MemoryResults struct {
	mu       sync.Mutex
	jobs     map[string]Job
	shards   map[string]map[int]ShardResult
	failures map[string]map[string]Failure // job -> group/seed -> failure
}

func NewMemoryResults() *MemoryResults {
	return &MemoryResults{jobs: map[string]Job{}, shards: map[string]map[int]ShardResult{}, failures: map[string]map[string]Failure{}}
}

func (m *MemoryResults) PutJob(_ context.Context, j Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[j.ID] = j
	return nil
}

func (m *MemoryResults) GetJob(_ context.Context, id string) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return Job{}, ErrNotFound
	}
	return j, nil
}

func (m *MemoryResults) PutShardResult(_ context.Context, r ShardResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shards[r.JobID] == nil {
		m.shards[r.JobID] = map[int]ShardResult{}
	}
	m.shards[r.JobID][r.Index] = r
	return nil
}

func (m *MemoryResults) ShardResults(_ context.Context, jobID string) ([]ShardResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []ShardResult
	for _, r := range m.shards[jobID] {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out, nil
}

func (m *MemoryResults) PutFailure(_ context.Context, f Failure) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failures[f.JobID] == nil {
		m.failures[f.JobID] = map[string]Failure{}
	}
	// Keyed by seed: a shard retried after a crash overwrites, not duplicates.
	m.failures[f.JobID][f.Group+"/"+strconv.FormatUint(f.Seed, 10)] = f
	return nil
}

func (m *MemoryResults) Failures(_ context.Context, jobID string) ([]Failure, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Failure
	for _, f := range m.failures[jobID] {
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seed < out[j].Seed })
	return out, nil
}

// DirBlobs stores blobs as files under a directory (local mode). URL returns
// a file:// link.
type DirBlobs struct{ Dir string }

func (d DirBlobs) path(key string) (string, error) {
	clean := filepath.Clean("/" + key)
	if strings.Contains(key, "..") {
		return "", errors.New("invalid blob key")
	}
	return filepath.Join(d.Dir, filepath.FromSlash(clean)), nil
}

func (d DirBlobs) Put(_ context.Context, key string, body []byte, _ string) error {
	p, err := d.path(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return os.WriteFile(p, body, 0o644)
}

func (d DirBlobs) URL(_ context.Context, key string) (string, error) {
	p, err := d.path(key)
	if err != nil {
		return "", err
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	return "file://" + filepath.ToSlash(abs), nil
}
