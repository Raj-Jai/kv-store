package storage

import (
	"errors"
	"testing"
	"time"
)

func TestMemStoreExpireLazy(t *testing.T) {
	s := NewMemStore()
	now := time.Unix(1_000_000, 0)
	s.now = func() time.Time { return now }

	s.Put("k", "v")
	if err := s.Expire("k", now.Add(time.Hour).UnixNano()); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get("k"); err != nil || got != "v" {
		t.Fatalf("before expiry Get = %q, %v", got, err)
	}

	now = now.Add(2 * time.Hour)
	if _, err := s.Get("k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after expiry Get = %v, want ErrNotFound", err)
	}
	if s.ExpiredCount() != 1 {
		t.Fatalf("ExpiredCount = %d, want 1", s.ExpiredCount())
	}
	if s.Size() != 0 {
		t.Fatalf("Size after expiry = %d, want 0", s.Size())
	}
}

func TestMemStoreExpireMissing(t *testing.T) {
	s := NewMemStore()
	if err := s.Expire("gone", time.Now().UnixNano()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Expire(missing) = %v, want ErrNotFound", err)
	}
}

func TestMemStoreSweepExpired(t *testing.T) {
	s := NewMemStore()
	now := time.Unix(1_000_000, 0)
	s.now = func() time.Time { return now }

	s.Put("live", "1")
	s.Put("dead", "1")
	s.Expire("dead", now.Add(time.Hour).UnixNano())

	now = now.Add(2 * time.Hour)
	if n := s.SweepExpired(); n != 1 {
		t.Fatalf("SweepExpired = %d, want 1", n)
	}
	if s.Size() != 1 {
		t.Fatalf("Size after sweep = %d, want 1", s.Size())
	}
	if s.ExpiredCount() != 1 {
		t.Fatalf("ExpiredCount = %d, want 1", s.ExpiredCount())
	}
}

func TestMemStorePutClearsTTL(t *testing.T) {
	s := NewMemStore()
	now := time.Unix(1_000_000, 0)
	s.now = func() time.Time { return now }

	s.Put("k", "v")
	s.Expire("k", now.Add(time.Hour).UnixNano())
	s.Put("k", "v2") // fresh write must clear the deadline

	now = now.Add(2 * time.Hour)
	if got, err := s.Get("k"); err != nil || got != "v2" {
		t.Fatalf("Get = %q, %v; want v2 (TTL cleared)", got, err)
	}
}

func TestMemStoreIncrOnExpiredKeyStartsAtOne(t *testing.T) {
	s := NewMemStore()
	now := time.Unix(1_000_000, 0)
	s.now = func() time.Time { return now }

	s.Put("k", "5")
	s.Expire("k", now.Add(time.Hour).UnixNano())
	now = now.Add(2 * time.Hour)

	if v, err := s.Incr("k"); err != nil || v != 1 {
		t.Fatalf("Incr(expired) = %d, %v; want 1", v, err)
	}
}

func TestMemStoreCASOnExpiredKey(t *testing.T) {
	s := NewMemStore()
	now := time.Unix(1_000_000, 0)
	s.now = func() time.Time { return now }

	s.Put("k", "a")
	s.Expire("k", now.Add(time.Hour).UnixNano())
	now = now.Add(2 * time.Hour)

	if _, err := s.CAS("k", "a", "b"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("CAS(expired) = %v, want ErrNotFound", err)
	}
}

func TestMemStoreExpireAlreadyExpiredKey(t *testing.T) {
	s := NewMemStore()
	now := time.Unix(1_000_000, 0)
	s.now = func() time.Time { return now }

	s.Put("k", "v")
	s.Expire("k", now.Add(time.Hour).UnixNano())
	now = now.Add(2 * time.Hour)

	if err := s.Expire("k", now.Add(time.Hour).UnixNano()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Expire(already-expired) = %v, want ErrNotFound", err)
	}
}

func TestDiskStoreExpireNotResurrectedAfterRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.Put("k", "v")
	// Absolute deadline in the recent past: already expired on recovery.
	if err := s.Expire("k", time.Now().Add(-time.Second).UnixNano()); err != nil {
		t.Fatal(err)
	}
	if err := s.compact(); err != nil {
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
	if _, err := s2.Get("k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired key resurrected after restart: %v", err)
	}
}

func TestDiskStoreExpireSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.Put("k", "v")
	if err := s.Expire("k", time.Now().Add(time.Hour).UnixNano()); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got, err := s2.Get("k"); err != nil || got != "v" {
		t.Fatalf("Get after restart = %q, %v; want v", got, err)
	}
}

func TestDiskStoreExpiredCount(t *testing.T) {
	s, err := OpenDiskStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Put("k", "v")
	s.Expire("k", time.Now().Add(-time.Second).UnixNano())
	s.Get("k") // lazy expiry path
	if s.ExpiredCount() == 0 {
		t.Fatal("ExpiredCount = 0, want > 0")
	}
}

func TestDiskStoreExpireMissingKey(t *testing.T) {
	s, err := OpenDiskStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Expire("gone", time.Now().UnixNano()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Expire(missing) = %v, want ErrNotFound", err)
	}
}

func TestSnapshotPersistsTTL(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.Put("k", "v")
	s.Expire("k", time.Now().Add(time.Hour).UnixNano())
	if err := s.compact(); err != nil {
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
	if e := s2.mem.data["k"]; e.Exp == 0 {
		t.Fatal("TTL not persisted in snapshot")
	}
}
