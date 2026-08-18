package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestMemStoreIncr(t *testing.T) {
	s := NewMemStore()

	v, err := s.Incr("n")
	if err != nil || v != 1 {
		t.Fatalf("Incr(missing) = %d, %v; want 1", v, err)
	}
	v, _ = s.Incr("n")
	if v != 2 {
		t.Fatalf("Incr = %d, want 2", v)
	}

	if err := s.Put("txt", "not-a-number"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Incr("txt"); !errors.Is(err, ErrNotNumeric) {
		t.Fatalf("Incr(non-numeric) = %v, want ErrNotNumeric", err)
	}

	if got, err := s.Get("n"); err != nil || got != "2" {
		t.Fatalf("Get(n) = %q, %v; want 2", got, err)
	}
}

func TestMemStoreCAS(t *testing.T) {
	s := NewMemStore()

	if _, err := s.CAS("k", "a", "b"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("CAS(missing) = %v, want ErrNotFound", err)
	}

	s.Put("k", "a")
	ok, err := s.CAS("k", "x", "z")
	if err != nil || ok {
		t.Fatalf("CAS(mismatch) = %v, %v; want false", ok, err)
	}
	if got, _ := s.Get("k"); got != "a" {
		t.Fatalf("value after mismatch = %q, want a", got)
	}

	ok, err = s.CAS("k", "a", "b")
	if err != nil || !ok {
		t.Fatalf("CAS(match) = %v, %v; want true", ok, err)
	}
	if got, _ := s.Get("k"); got != "b" {
		t.Fatalf("value after match = %q, want b", got)
	}
}

func TestDiskStoreIncrCasExpire(t *testing.T) {
	s, err := OpenDiskStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if v, err := s.Incr("n"); err != nil || v != 1 {
		t.Fatalf("Incr = %d, %v", v, err)
	}
	if v, err := s.Incr("n"); err != nil || v != 2 {
		t.Fatalf("Incr = %d, %v", v, err)
	}

	ok, err := s.CAS("n", "2", "10")
	if err != nil || !ok {
		t.Fatalf("CAS(match) = %v, %v", ok, err)
	}
	ok, err = s.CAS("n", "2", "99")
	if err != nil || ok {
		t.Fatalf("CAS(mismatch) = %v, %v; want false", ok, err)
	}
	if got, _ := s.Get("n"); got != "10" {
		t.Fatalf("value = %q, want 10", got)
	}
}

func TestDiskStoreIncrConcurrent(t *testing.T) {
	s, err := OpenDiskStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Incr("counter"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	if got, _ := s.Get("counter"); got != fmt.Sprintf("%d", n) {
		t.Fatalf("counter = %q, want %d", got, n)
	}
}

func TestDiskStoreCasMissingKey(t *testing.T) {
	s, err := OpenDiskStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.CAS("gone", "a", "b"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("CAS(missing) = %v, want ErrNotFound", err)
	}
}

func TestDiskStoreIncrNonNumeric(t *testing.T) {
	s, err := OpenDiskStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Put("txt", "hello")
	if _, err := s.Incr("txt"); !errors.Is(err, ErrNotNumeric) {
		t.Fatalf("Incr(non-numeric) = %v, want ErrNotNumeric", err)
	}
}

// TestDiskStoreSingleOpWalFailure covers the WAL-append error branch of the
// single-op path by closing the WAL file underneath the store.
func TestDiskStoreSingleOpWalFailure(t *testing.T) {
	s, err := OpenDiskStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.wal.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := s.Incr("k"); err == nil {
		t.Fatal("Incr succeeded with a closed WAL")
	}
	if _, err := s.CAS("k", "a", "b"); err == nil {
		t.Fatal("CAS succeeded with a closed WAL")
	}
	if err := s.Expire("k", time.Now().UnixNano()); err == nil {
		t.Fatal("Expire succeeded with a closed WAL")
	}
}

// TestIncrNonNumericThenCrashRestart guards against bricking the store: a
// failed (non-numeric) Incr is WAL-logged before the memory apply, and replay
// must re-evaluate it as a no-op rather than refusing to start. Close() masks
// the bug by compacting and truncating the WAL, so a crash is simulated by
// copying the data dir first.
func TestIncrNonNumericThenCrashRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.Put("k", "txt")
	if _, err := s.Incr("k"); err == nil {
		t.Fatal("expected ErrNotNumeric")
	}

	crash := t.TempDir()
	for _, name := range []string{"wal.log", "snapshot.dat"} {
		src := filepath.Join(dir, name)
		if _, err := os.Stat(src); err == nil {
			data, err := os.ReadFile(src)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(crash, name), data, 0644); err != nil {
				t.Fatal(err)
			}
		}
	}
	s.Close()

	restored, err := OpenDiskStore(crash)
	if err != nil {
		t.Fatalf("crash-restart after failed Incr: %v", err)
	}
	defer restored.Close()
	if v, _ := restored.Get("k"); v != "txt" {
		t.Fatalf("Get = %q, want txt", v)
	}
}
