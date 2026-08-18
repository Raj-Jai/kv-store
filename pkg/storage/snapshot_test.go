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

// TestSerializeRestoreSnapshotRoundTrip verifies the M1.6 raft-bridge methods:
// SerializeSnapshot emits the on-disk snapshot format and RestoreSnapshot
// installs it into another store, persists it, and truncates its WAL.
func TestSerializeRestoreSnapshotRoundTrip(t *testing.T) {
	dirA, dirB := t.TempDir(), t.TempDir()

	src, err := OpenDiskStore(dirA)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := src.Put(fmt.Sprintf("k%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	data, err := src.SerializeSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	dst, err := OpenDiskStore(dirB)
	if err != nil {
		t.Fatal(err)
	}
	dst.Put("stale", "1")
	if err := dst.RestoreSnapshot(data); err != nil {
		t.Fatal(err)
	}
	if err := dst.Close(); err != nil {
		t.Fatal(err)
	}

	// The installed state must survive a reopen: snapshot.dat holds it and the
	// WAL was truncated, so no stale or extra entries leak in.
	re, err := OpenDiskStore(dirB)
	if err != nil {
		t.Fatal(err)
	}
	defer re.Close()
	for i := 0; i < 5; i++ {
		if _, err := re.Get(fmt.Sprintf("k%d", i)); err != nil {
			t.Fatalf("missing k%d after restore: %v", i, err)
		}
	}
	if _, err := re.Get("stale"); err == nil {
		t.Fatal("stale entry survived restore")
	}
}

func TestRestoreSnapshotRejectsBadData(t *testing.T) {
	s, err := OpenDiskStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.RestoreSnapshot([]byte("not json")); err == nil {
		t.Fatal("expected error restoring invalid snapshot")
	}
}

// TestCompactTruncatesWALAndSurvivesRestart exercises the exported Compact
// (used by the server snapshot bridge before advancing the raft log base).
func TestCompactTruncatesWALAndSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	s, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := s.Put(fmt.Sprintf("k%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(filepath.Join(dir, "wal.log")); err != nil || info.Size() != 0 {
		t.Fatalf("WAL not truncated after Compact: %v, size=%d", err, info.Size())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	re, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer re.Close()
	for i := 0; i < 10; i++ {
		if _, err := re.Get(fmt.Sprintf("k%d", i)); err != nil {
			t.Fatalf("missing k%d after Compact restart: %v", i, err)
		}
	}
}

func TestCompactIfNeeded(t *testing.T) {
	dir := t.TempDir()

	s, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// A small WAL does not trigger compaction.
	if err := s.Put("a", "1"); err != nil {
		t.Fatal(err)
	}
	if err := s.compactIfNeeded(); err != nil {
		t.Fatalf("compactIfNeeded on small WAL: %v", err)
	}
	if info, err := os.Stat(filepath.Join(dir, "wal.log")); err != nil || info.Size() == 0 {
		t.Fatalf("WAL should not be truncated for a small log: %v, size=%d", err, info.Size())
	}

	// A missing WAL file is an error, not a silent no-op.
	if err := os.Remove(filepath.Join(dir, "wal.log")); err != nil {
		t.Fatal(err)
	}
	if err := s.compactIfNeeded(); err == nil {
		t.Fatal("expected error when WAL file is missing")
	}
}
