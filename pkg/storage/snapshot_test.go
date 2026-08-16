package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestSnapshotCompactsWAL verifies compaction truncates the WAL and recovery
// restores the full state from the snapshot alone.
func TestSnapshotCompactsWAL(t *testing.T) {
	dir := t.TempDir()

	s, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if err := s.Put(fmt.Sprintf("k%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}

	if err := s.compact(); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(filepath.Join(dir, "wal.log")); err != nil || info.Size() != 0 {
		t.Fatalf("WAL not truncated after compact: %v, size=%d", err, info.Size())
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	for i := 0; i < 50; i++ {
		if _, err := s2.Get(fmt.Sprintf("k%d", i)); err != nil {
			t.Fatalf("missing k%d after snapshot recovery: %v", i, err)
		}
	}
}

// TestSnapshotWithWalTail verifies recovery combines the snapshot with WAL
// entries appended after it.
func TestSnapshotWithWalTail(t *testing.T) {
	dir := t.TempDir()

	s, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("a", "1"); err != nil {
		t.Fatal(err)
	}
	if err := s.compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("b", "2"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	for key, want := range map[string]string{"a": "1", "b": "2"} {
		got, err := s2.Get(key)
		if err != nil || got != want {
			t.Fatalf("Get(%q) = %q, %v; want %q", key, got, err, want)
		}
	}
}

// TestRecoveryIdempotentAfterSnapshotBeforeTruncate simulates a crash between
// the snapshot rename and the WAL truncation: replaying the WAL over the
// snapshot must not double-apply or lose data.
func TestRecoveryIdempotentAfterSnapshotBeforeTruncate(t *testing.T) {
	dir := t.TempDir()

	s, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("a", "1"); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("b", "2"); err != nil {
		t.Fatal(err)
	}

	// Crash point: snapshot written, WAL truncation never happened.
	if err := s.saveSnapshot(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	for key, want := range map[string]string{"a": "1", "b": "2"} {
		got, err := s2.Get(key)
		if err != nil || got != want {
			t.Fatalf("Get(%q) = %q, %v; want %q", key, got, err, want)
		}
	}
}