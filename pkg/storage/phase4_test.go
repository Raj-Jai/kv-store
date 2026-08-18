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

func TestMemStoreScanPagination(t *testing.T) {
	s := NewMemStore()
	for i := 0; i < 10; i++ {
		s.Put(fmt.Sprintf("k%02d", i), fmt.Sprintf("v%d", i))
	}

	var got []string
	cursor := ""
	pages := 0
	for {
		items, next, err := s.Scan(cursor, 3, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, it := range items {
			got = append(got, it.Key)
		}
		pages++
		if next == "" {
			break
		}
		cursor = next
		if pages > 10 {
			t.Fatal("scan did not terminate")
		}
	}

	if len(got) != 10 {
		t.Fatalf("scan returned %d keys, want 10", len(got))
	}
	for i := 0; i < 10; i++ {
		if got[i] != fmt.Sprintf("k%02d", i) {
			t.Fatalf("scan key %d = %q", i, got[i])
		}
	}
}

func TestMemStoreScanPattern(t *testing.T) {
	s := NewMemStore()
	s.Put("user:1", "a")
	s.Put("user:2", "b")
	s.Put("post:1", "c")
	s.Put("abc", "d")

	items, next, err := s.Scan("", 10, "user:*")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].Key != "user:1" || items[1].Key != "user:2" {
		t.Fatalf("pattern scan = %+v", items)
	}
	if next != "" {
		t.Fatalf("next = %q, want empty", next)
	}

	items, _, err = s.Scan("", 10, "*1")
	if err != nil || len(items) != 2 || items[0].Key != "post:1" || items[1].Key != "user:1" {
		t.Fatalf("suffix pattern scan = %+v, %v", items, err)
	}
}

func TestMemStoreScanSkipsExpired(t *testing.T) {
	s := NewMemStore()
	now := time.Unix(1_000_000, 0)
	s.now = func() time.Time { return now }

	s.Put("alive", "1")
	s.Put("dead", "1")
	s.Expire("dead", now.Add(time.Hour).UnixNano())

	now = now.Add(2 * time.Hour)
	items, _, err := s.Scan("", 10, "")
	if err != nil || len(items) != 1 || items[0].Key != "alive" {
		t.Fatalf("scan = %+v, %v; want only alive", items, err)
	}
}

func TestMemStoreScanBadCount(t *testing.T) {
	s := NewMemStore()
	if _, _, err := s.Scan("", 0, ""); err == nil {
		t.Fatal("Scan(count=0) = nil, want error")
	}
}

func TestMatchGlob(t *testing.T) {
	cases := []struct {
		pat, s string
		want   bool
	}{
		{"", "anything", true},
		{"*", "", true},
		{"*", "x", true},
		{"a", "a", true},
		{"a", "b", false},
		{"a*", "abc", true},
		{"a*", "cba", false},
		{"*b", "ab", true},
		{"*b", "ba", false},
		{"a*c", "abc", true},
		{"a*c", "ac", true},
		{"a*c", "ab", false},
		{"user:*", "user:1", true},
		{"user:*", "users", false},
		{"*1*", "post:1:x", true},
		{"*1*", "post2", false},
	}
	for _, c := range cases {
		if got := matchGlob(c.pat, c.s); got != c.want {
			t.Fatalf("matchGlob(%q, %q) = %v, want %v", c.pat, c.s, got, c.want)
		}
	}
}

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

func TestSnapshotV1Migration(t *testing.T) {
	dir := t.TempDir()
	// Write a legacy (v1) snapshot: a raw map[string]string.
	if err := atomicWriteFile(filepath.Join(dir, "snapshot.dat"), []byte(`{"a":"1","b":"2"}`), 0644); err != nil {
		t.Fatal(err)
	}

	s, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if got, _ := s.Get("a"); got != "1" {
		t.Fatalf("Get(a) = %q, want 1", got)
	}
	if got, _ := s.Get("b"); got != "2" {
		t.Fatalf("Get(b) = %q, want 2", got)
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

func TestDiskStoreScan(t *testing.T) {
	s, err := OpenDiskStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 5; i++ {
		s.Put(fmt.Sprintf("k%d", i), "v")
	}
	items, next, err := s.Scan("", 2, "k*")
	if err != nil || len(items) != 2 {
		t.Fatalf("scan = %+v, %v", items, err)
	}
	if next != "k1" {
		t.Fatalf("next = %q, want k1", next)
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

func TestMemStoreCloseNoop(t *testing.T) {
	s := NewMemStore()
	if err := s.Close(); err != nil {
		t.Fatalf("MemStore.Close = %v", err)
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
