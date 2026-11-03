package sim

import "sort"

// Disk is a node's simulated durable storage.
//
// Writes go to a buffer first, like an OS page cache. They are visible to
// reads immediately, but they only survive a crash after Sync. This makes the
// classic "forgot to fsync" bug reproducible.
type Disk struct {
	durable map[string][]byte
	pending map[string][]byte
	deleted map[string]bool
}

func newDisk() *Disk {
	return &Disk{
		durable: map[string][]byte{},
		pending: map[string][]byte{},
		deleted: map[string]bool{},
	}
}

// Write stores value under key. It is lost on crash unless Sync is called.
func (d *Disk) Write(key string, value []byte) {
	d.pending[key] = append([]byte(nil), value...)
	delete(d.deleted, key)
}

// Delete removes key. Like Write, it only survives a crash after Sync.
func (d *Disk) Delete(key string) {
	delete(d.pending, key)
	d.deleted[key] = true
}

// Read returns the current value of key, including unsynced writes.
func (d *Disk) Read(key string) ([]byte, bool) {
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
	for k, v := range d.pending {
		d.durable[k] = v
	}
	for k := range d.deleted {
		delete(d.durable, k)
	}
	d.pending = map[string][]byte{}
	d.deleted = map[string]bool{}
}

// crash discards everything that was not synced.
func (d *Disk) crash() (lost int) {
	lost = len(d.pending) + len(d.deleted)
	d.pending = map[string][]byte{}
	d.deleted = map[string]bool{}
	return lost
}
