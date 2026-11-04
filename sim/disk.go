package sim

import (
	"sort"
	"sync"
)

// Disk is a node's simulated durable storage.
//
// Writes go to a buffer first, like an OS page cache. They are visible to
// reads immediately, but they only survive a crash after Sync. This makes the
// classic "forgot to fsync" bug reproducible.
//
// A Disk is safe for concurrent use, so goroutine-based code (see x/simproc)
// can share one.
type Disk struct {
	mu      sync.Mutex
	durable map[string][]byte
	pending map[string][]byte
	deleted map[string]bool
}

// NewDisk returns an empty disk. The simulator creates one per node; use
// NewDisk when building other runners on top of this package.
func NewDisk() *Disk { return newDisk() }

func newDisk() *Disk {
	return &Disk{
		durable: map[string][]byte{},
		pending: map[string][]byte{},
		deleted: map[string]bool{},
	}
}

// Write stores value under key. It is lost on crash unless Sync is called.
func (d *Disk) Write(key string, value []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pending[key] = append([]byte(nil), value...)
	delete(d.deleted, key)
}

// Delete removes key. Like Write, it only survives a crash after Sync.
func (d *Disk) Delete(key string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.pending, key)
	d.deleted[key] = true
}

// Read returns the current value of key, including unsynced writes.
func (d *Disk) Read(key string) ([]byte, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.deleted[key] {
		return nil, false
	}
	if v, ok := d.pending[key]; ok {
		return append([]byte(nil), v...), true
	}
	v, ok := d.durable[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v...), true
}

// Keys returns all live keys in sorted order.
func (d *Disk) Keys() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	seen := map[string]bool{}
	for k := range d.durable {
		if !d.deleted[k] {
			seen[k] = true
		}
	}
	for k := range d.pending {
		seen[k] = true
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Sync makes all buffered writes durable.
func (d *Disk) Sync() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for k, v := range d.pending {
		d.durable[k] = v
	}
	for k := range d.deleted {
		delete(d.durable, k)
	}
	d.pending = map[string][]byte{}
	d.deleted = map[string]bool{}
}

// Crash discards everything that was not synced, as a power loss or process
// crash would, and returns how many writes were lost.
func (d *Disk) Crash() (lost int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	lost = len(d.pending) + len(d.deleted)
	d.pending = map[string][]byte{}
	d.deleted = map[string]bool{}
	return lost
}
