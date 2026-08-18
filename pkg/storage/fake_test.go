package storage

import (
	"errors"
	"testing"
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
