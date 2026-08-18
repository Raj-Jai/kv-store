package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWalNewOpsRoundtrip(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendPut("k", "5"); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendIncr("k"); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendCAS("k", "6", "7"); err != nil {
		t.Fatal(err)
	}
	expiresAt := time.Now().Add(time.Hour).UnixNano()
	if err := w.AppendExpire("k", expiresAt); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := OpenWAL(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	m := NewMemStore()
	if err := w2.Replay(m); err != nil {
		t.Fatal(err)
	}

	if got, _ := m.Get("k"); got != "7" {
		t.Fatalf("value after replay = %q, want 7", got)
	}
	if e := m.data["k"]; e.Exp != expiresAt {
		t.Fatalf("expiry after replay = %d, want %d", e.Exp, expiresAt)
	}
}

func TestWalReplayCasMismatchNoop(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendCAS("k", "a", "b"); err != nil { // old never existed
		t.Fatal(err)
	}
	w.Sync()
	w.Close()

	w2, err := OpenWAL(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	m := NewMemStore()
	if err := w2.Replay(m); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Get("k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after mismatch replay = %v, want ErrNotFound", err)
	}
}

func TestWalReplayExpireMissingNoop(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	w.AppendExpire("gone", time.Now().Add(time.Hour).UnixNano())
	w.Sync()
	w.Close()

	w2, err := OpenWAL(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if err := w2.Replay(NewMemStore()); err != nil {
		t.Fatalf("replay of expire-on-missing should be a no-op, got %v", err)
	}
}

func TestWalReplayTruncatedNewOps(t *testing.T) {
	// A record cut short at each new opcode must fail cleanly, not corrupt
	// the replay or hang.
	cases := []func(*WAL) error{
		func(w *WAL) error { return w.AppendIncr("k") },
		func(w *WAL) error { return w.AppendCAS("k", "a", "b") },
		func(w *WAL) error { return w.AppendExpire("k", 12345) },
	}
	for i, write := range cases {
		dir := t.TempDir()
		w, err := OpenWAL(filepath.Join(dir, "wal.log"))
		if err != nil {
			t.Fatal(err)
		}
		if err := write(w); err != nil {
			t.Fatal(err)
		}
		// Chop the trailing byte so the record is incomplete.
		info, _ := os.Stat(filepath.Join(dir, "wal.log"))
		os.Truncate(filepath.Join(dir, "wal.log"), info.Size()-1)
		w.Close()

		w2, err := OpenWAL(filepath.Join(dir, "wal.log"))
		if err != nil {
			t.Fatal(err)
		}
		if err := w2.Replay(NewMemStore()); err == nil {
			t.Fatalf("case %d: truncated record replayed without error", i)
		}
		w2.Close()
	}
}
