package storage

import (
	"errors"
	"testing"
	"time"
)

// TestFakeEngine covers the contract fake both developers build against.
func TestFakeEngine(t *testing.T) {
	f := NewFakeEngine()

	if _, err := f.Get("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(missing) err = %v, want ErrNotFound", err)
	}
	if err := f.Put("a", "1"); err != nil {
		t.Fatal(err)
	}
	if v, err := f.Get("a"); err != nil || v != "1" {
		t.Fatalf("Get(a) = %q, %v; want 1", v, err)
	}
	if err := f.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Get("a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(a) after delete err = %v, want ErrNotFound", err)
	}
	if err := f.Put("b", "2"); err != nil {
		t.Fatal(err)
	}
	if err := f.Clear(); err != nil {
		t.Fatal(err)
	}
	if !f.WasCleared() {
		t.Fatal("WasCleared() = false after Clear")
	}
	if _, err := f.Get("b"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(b) after clear err = %v, want ErrNotFound", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestFakeEngineNewOps keeps the contract fake covered inside pkg/storage
// (the raft tests exercise it too, but the per-package gate needs it here).
func TestFakeEngineNewOps(t *testing.T) {
	f := NewFakeEngine()
	now := time.Unix(1_000_000, 0)
	f.now = func() time.Time { return now }

	if v, err := f.Incr("n"); err != nil || v != 1 {
		t.Fatalf("Incr = %d, %v; want 1", v, err)
	}
	if v, err := f.Incr("n"); err != nil || v != 2 {
		t.Fatalf("Incr = %d, %v; want 2", v, err)
	}

	f.Put("txt", "x")
	if _, err := f.Incr("txt"); !errors.Is(err, ErrNotNumeric) {
		t.Fatalf("Incr(non-numeric) = %v, want ErrNotNumeric", err)
	}

	if _, err := f.CAS("gone", "a", "b"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("CAS(missing) = %v, want ErrNotFound", err)
	}
	ok, err := f.CAS("n", "2", "9")
	if err != nil || !ok {
		t.Fatalf("CAS(match) = %v, %v; want true", ok, err)
	}
	ok, err = f.CAS("n", "2", "9")
	if err != nil || ok {
		t.Fatalf("CAS(mismatch) = %v, %v; want false", ok, err)
	}

	if err := f.Expire("n", now.Add(time.Hour).UnixNano()); err != nil {
		t.Fatal(err)
	}
	if err := f.Expire("gone", now.Add(time.Hour).UnixNano()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Expire(missing) = %v, want ErrNotFound", err)
	}
	now = now.Add(2 * time.Hour)
	if _, err := f.Get("n"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(expired) = %v, want ErrNotFound", err)
	}

	f.Put("a", "1")
	f.Put("b", "2")
	f.Put("c", "3")
	items, next, err := f.Scan("", 2, "")
	if err != nil || len(items) != 2 || next != "b" {
		t.Fatalf("Scan = %+v (next=%q), %v", items, next, err)
	}
	items, _, err = f.Scan("", 10, "b")
	if err != nil || len(items) != 1 {
		t.Fatalf("pattern Scan = %+v, %v", items, err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}
