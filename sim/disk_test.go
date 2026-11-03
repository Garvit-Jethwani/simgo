package sim

import (
	"slices"
	"testing"
)

func TestDiskUnsyncedWritesAreLostOnCrash(t *testing.T) {
	d := newDisk()
	d.Write("a", []byte("1"))
	d.Sync()
	d.Write("a", []byte("2"))
	d.Write("b", []byte("x"))

	if v, _ := d.Read("a"); string(v) != "2" {
		t.Fatalf("read before crash = %q, want unsynced value %q", v, "2")
	}
	if lost := d.crash(); lost != 2 {
		t.Fatalf("crash lost %d writes, want 2", lost)
	}
	if v, _ := d.Read("a"); string(v) != "1" {
		t.Fatalf("read after crash = %q, want last synced value %q", v, "1")
	}
	if _, ok := d.Read("b"); ok {
		t.Fatal("unsynced key b survived the crash")
	}
}

func TestDiskDeleteNeedsSync(t *testing.T) {
	d := newDisk()
	d.Write("k", []byte("v"))
	d.Sync()

	d.Delete("k")
	if _, ok := d.Read("k"); ok {
		t.Fatal("deleted key still readable")
	}
	d.crash()
	if v, ok := d.Read("k"); !ok || string(v) != "v" {
		t.Fatal("unsynced delete survived the crash")
	}

	d.Delete("k")
	d.Sync()
	d.crash()
	if _, ok := d.Read("k"); ok {
		t.Fatal("synced delete was undone by a crash")
	}
}

func TestDiskReadsAreCopies(t *testing.T) {
	d := newDisk()
	buf := []byte("abc")
	d.Write("k", buf)
	buf[0] = 'X'
	v, _ := d.Read("k")
	v[1] = 'Y'
	if got, _ := d.Read("k"); string(got) != "abc" {
		t.Fatalf("disk contents changed through an aliased slice: %q", got)
	}
}

func TestDiskKeysSorted(t *testing.T) {
	d := newDisk()
	d.Write("b", nil)
	d.Write("a", nil)
	d.Sync()
	d.Write("c", nil)
	d.Delete("b")
	if got, want := d.Keys(), []string{"a", "c"}; !slices.Equal(got, want) {
		t.Fatalf("Keys() = %v, want %v", got, want)
	}
}
