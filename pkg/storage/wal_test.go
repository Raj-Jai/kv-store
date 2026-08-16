package storage

import (
	"errors"
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
