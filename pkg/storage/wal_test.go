package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDiskStorePersistsAcrossRestart(t *testing.T) {
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

func TestDiskStoreReplayOrder(t *testing.T) {
	dir := t.TempDir()

	s, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.Put("a", "1")
	s.Delete("a")
	s.Put("b", "2")
	s.Clear()
	s.Put("c", "3")
	s.Close()

	s2, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	if _, err := s2.Get("b"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected b cleared, got %v", err)
	}
	v, err := s2.Get("c")
	if err != nil || v != "3" {
		t.Fatalf("Get(c) = %q, %v; want 3", v, err)
	}
}

func TestDiskStoreCrashRecovery(t *testing.T) {
	dir := t.TempDir()

	s, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Put("a", "1")
	// Simulate a crash: no Close. Acked writes must survive a reopen.
	s2, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	v, err := s2.Get("a")
	if err != nil || v != "1" {
		t.Fatalf("Get(a) = %q, %v; want 1", v, err)
	}
}

// TestReplayRejectsCorruptWAL verifies a WAL with an unknown opcode fails
// recovery cleanly instead of applying garbage.
func TestReplayRejectsCorruptWAL(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "wal.log"), []byte{0x7F}, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDiskStore(dir); err == nil {
		t.Fatal("expected error replaying a WAL with an unknown opcode")
	}
}

// TestReplayRejectsTruncatedRecord verifies a WAL whose final record is cut
// short fails recovery cleanly.
func TestReplayRejectsTruncatedRecord(t *testing.T) {
	dir := t.TempDir()
	// opPut with a declared key length of 16 but zero bytes present.
	if err := os.WriteFile(filepath.Join(dir, "wal.log"), []byte{0x01, 0x00, 0x00, 0x00, 0x10}, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDiskStore(dir); err == nil {
		t.Fatal("expected error replaying a truncated WAL record")
	}
}

// TestOpenWALInvalidPath verifies OpenWAL surfaces a create failure.
func TestOpenWALInvalidPath(t *testing.T) {
	if _, err := OpenWAL(filepath.Join(t.TempDir(), "missing", "wal.log")); err == nil {
		t.Fatal("expected error opening a WAL in a missing directory")
	}
}

// TestOpenDiskStoreRejectsCorruptSnapshot verifies a corrupt snapshot.dat
// fails recovery cleanly.
func TestOpenDiskStoreRejectsCorruptSnapshot(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "snapshot.dat"), []byte("not json"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDiskStore(dir); err == nil {
		t.Fatal("expected error loading a corrupt snapshot")
	}
}

func TestDiskStoreSize(t *testing.T) {
	s, err := OpenDiskStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Put("a", "1")
	s.Put("b", "2")
	if s.Size() != 2 {
		t.Fatalf("Size = %d, want 2", s.Size())
	}
}
