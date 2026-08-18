package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

var testKey = bytes.Repeat([]byte{0x42}, AtRestKeySize)

func TestAtRestCipherRoundTrip(t *testing.T) {
	c, err := newAtRestCipher(testKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{0, 1, 100, 1024 * 1024} {
		plain := make([]byte, size)
		for i := range plain {
			plain[i] = byte(i)
		}
		sealed, err := c.Encrypt(plain)
		if err != nil {
			t.Fatalf("encrypt size %d: %v", size, err)
		}
		// Every seal carries a fresh nonce: the ciphertext must differ.
		again, err := c.Encrypt(plain)
		if err != nil {
			t.Fatalf("encrypt again size %d: %v", size, err)
		}
		if bytes.Equal(sealed, again) {
			t.Fatalf("two seals of identical plaintext are identical at size %d", size)
		}
		got, err := c.Decrypt(sealed)
		if err != nil {
			t.Fatalf("decrypt size %d: %v", size, err)
		}
		if !bytes.Equal(got, plain) {
			t.Fatalf("round trip corrupted data at size %d", size)
		}
	}
}

func TestAtRestCipherTamperDetected(t *testing.T) {
	c, _ := newAtRestCipher(testKey)
	sealed, _ := c.Encrypt([]byte("secret"))
	sealed[len(sealed)-1] ^= 0x01 // flip one ciphertext byte
	if _, err := c.Decrypt(sealed); !errors.Is(err, ErrDecrypt) {
		t.Fatalf("tampered blob must fail to decrypt, got %v", err)
	}
}

func TestAtRestCipherWrongKey(t *testing.T) {
	c, _ := newAtRestCipher(testKey)
	other, _ := newAtRestCipher(bytes.Repeat([]byte{0x24}, AtRestKeySize))
	sealed, _ := c.Encrypt([]byte("secret"))
	if _, err := other.Decrypt(sealed); !errors.Is(err, ErrDecrypt) {
		t.Fatalf("wrong key must fail to decrypt, got %v", err)
	}
}

func TestAtRestCipherBadKeyLength(t *testing.T) {
	if _, err := newAtRestCipher([]byte("short")); err == nil {
		t.Fatal("short key must be rejected")
	}
}

// writeEncryptedStore fills an at-rest encrypted store with every op kind,
// closes it (which snapshots and truncates the WAL), and reopens it so the
// caller can assert the recovered state.
func writeEncryptedStore(t *testing.T, dir string, key []byte) *DiskStore {
	t.Helper()
	s, err := OpenDiskStoreWithKey(dir, key)
	if err != nil {
		t.Fatalf("open encrypted store: %v", err)
	}
	future := time.Now().Add(time.Hour).UnixNano()
	ops := []struct {
		name string
		run  func() error
	}{
		{"put", func() error { return s.Put("k", "v1") }},
		{"incr", func() error { _, err := s.Incr("n"); return err }},
		{"incr-again", func() error { _, err := s.Incr("n"); return err }},
		{"expire", func() error { return s.Expire("k", future) }},
		{"cas", func() error { _, err := s.CAS("n", "2", "cas"); return err }},
		{"delete-absent", func() error { return s.Delete("nope") }},
		{"put-b", func() error { return s.Put("b", "x") }},
		{"clear-reset", func() error { return s.Clear() }},
	}
	for _, op := range ops {
		if err := op.run(); err != nil {
			t.Fatalf("%s: %v", op.name, err)
		}
	}
	// Re-add after Clear so the reopened store has live state.
	if err := s.Put("k", "encrypted"); err != nil {
		t.Fatalf("put after clear: %v", err)
	}
	if err := s.Put("n", "42"); err != nil {
		t.Fatalf("put n: %v", err)
	}
	if err := s.Expire("n", future); err != nil {
		t.Fatalf("expire n: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close encrypted store: %v", err)
	}
	return s
}

func TestEncryptedStorePersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	writeEncryptedStore(t, dir, testKey)

	s, err := OpenDiskStoreWithKey(dir, testKey)
	if err != nil {
		t.Fatalf("reopen encrypted store: %v", err)
	}
	defer s.Close()

	if v, err := s.Get("k"); err != nil || v != "encrypted" {
		t.Fatalf("k = %q, %v; want encrypted, nil", v, err)
	}
	if v, err := s.Get("n"); err != nil || v != "42" {
		t.Fatalf("n = %q, %v; want 42, nil", v, err)
	}
	if got, err := s.Get("b"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("b should be gone after Clear, got %q, %v", got, err)
	}

	// The on-disk files must carry the encrypted markers, never plaintext JSON
	// or a raw record stream.
	raw, err := os.ReadFile(filepath.Join(dir, "snapshot.dat"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(raw, []byte(snapshotMagic)) {
		t.Fatalf("snapshot not sealed at rest: prefix %q", raw[:4])
	}
	if bytes.Contains(raw, []byte("encrypted")) {
		t.Fatal("plaintext state leaked into the at-rest snapshot")
	}

	walRaw, err := os.ReadFile(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if len(walRaw) == 0 || walRaw[0] != walEncryptedMagic {
		t.Fatalf("wal missing encrypted magic: %v", walRaw[:min(len(walRaw), 1)])
	}
}

func TestEncryptedStoreWrongKeyFails(t *testing.T) {
	dir := t.TempDir()
	writeEncryptedStore(t, dir, testKey)

	wrong := bytes.Repeat([]byte{0x99}, AtRestKeySize)
	if _, err := OpenDiskStoreWithKey(dir, wrong); err == nil {
		t.Fatal("opening an encrypted store with the wrong key must fail")
	}
}

func TestEncryptedStoreRejectedWithoutKey(t *testing.T) {
	dir := t.TempDir()
	writeEncryptedStore(t, dir, testKey)

	if _, err := OpenDiskStore(dir); err == nil {
		t.Fatal("opening an encrypted store without a key must fail")
	}
}

func TestPlaintextStoreRejectedUnderCipher(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k", "plain"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := OpenDiskStoreWithKey(dir, testKey); err == nil {
		t.Fatal("opening a plaintext store under a cipher must fail")
	}
}

func TestEncryptedWALTruncateKeepsMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	c, _ := newAtRestCipher(testKey)

	wal, err := OpenWALEncrypted(path, c)
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.AppendPut("k", "v"); err != nil {
		t.Fatal(err)
	}
	if err := wal.Sync(); err != nil {
		t.Fatal(err)
	}
	// Compaction empties the log; the encrypted framing must survive so the
	// next append is still parseable after a restart.
	if err := wal.Truncate(); err != nil {
		t.Fatal(err)
	}
	if err := wal.AppendPut("after", "truncate"); err != nil {
		t.Fatal(err)
	}
	if err := wal.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if raw[0] != walEncryptedMagic {
		t.Fatalf("magic lost after truncate: %v", raw[0])
	}

	wal2, err := OpenWALEncrypted(path, c)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()
	m := NewMemStore()
	if err := wal2.Replay(m); err != nil {
		t.Fatal(err)
	}
	if v, _ := m.Get("after"); v != "truncate" {
		t.Fatalf("after-truncate record lost: %q", v)
	}
	if _, err := m.Get("k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("pre-truncate record should be gone: %v", err)
	}
}

func TestEncryptedWALReplayAllOps(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	c, _ := newAtRestCipher(testKey)

	wal, err := OpenWALEncrypted(path, c)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	future := now.Add(time.Hour).UnixNano()
	if err := wal.AppendPut("k", "5"); err != nil {
		t.Fatal(err)
	}
	if err := wal.AppendIncr("k"); err != nil {
		t.Fatal(err)
	}
	if err := wal.AppendIncr("absent"); err != nil {
		t.Fatal(err)
	}
	if err := wal.AppendCAS("k", "6", "cas"); err != nil {
		t.Fatal(err)
	}
	if err := wal.AppendExpire("k", future); err != nil {
		t.Fatal(err)
	}
	if err := wal.AppendDelete("k"); err != nil {
		t.Fatal(err)
	}
	if err := wal.AppendClear(); err != nil {
		t.Fatal(err)
	}
	if err := wal.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	wal2, err := OpenWALEncrypted(path, c)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()
	m := NewMemStore()
	if err := wal2.Replay(m); err != nil {
		t.Fatal(err)
	}
	if len(m.data) != 0 {
		t.Fatalf("all ops replayed to a cleared store, got %v", m.data)
	}
}

// TestEncryptedStoreRaftSnapshotStaysPlaintext pins the boundary: snapshots
// shipped to raft peers over the wire keep the plaintext JSON format (TLS
// protects them in transit), while the copy persisted locally is sealed.
func TestEncryptedStoreRaftSnapshotStaysPlaintext(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenDiskStoreWithKey(dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Put("k", "wire"); err != nil {
		t.Fatal(err)
	}

	wire, err := s.SerializeSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.HasPrefix(wire, []byte(snapshotMagic)) {
		t.Fatal("raft snapshot must travel in plaintext JSON, not sealed")
	}
	if !bytes.Contains(wire, []byte("wire")) {
		t.Fatal("raft snapshot missing state")
	}

	// A follower that receives it must persist the sealed form.
	if err := s.RestoreSnapshot(wire); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "snapshot.dat"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(raw, []byte(snapshotMagic)) {
		t.Fatal("locally persisted snapshot is not sealed at rest")
	}
}

func TestFuzzWALReplayEncrypted(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	seeds := [][]byte{
		{},
		{byte(opClear)},
		{byte(opPut), 1, 0, 0, 0, 'k', 1, 0, 0, 0, 'v'},
		{byte(opDelete), 1, 0, 0, 0, 'k'},
		{byte(opClear), byte(opPut), 1, 0, 0, 0, 'a', 1, 0, 0, 0, 'b', byte(opDelete), 1, 0, 0, 0, 'a'},
		{0},
		{0xFF},
		{byte(opPut), 0, 0, 0, 0x10},
		{byte(opPut), 0xFF, 0xFF, 0xFF, 0xFF, 1, 0, 0, 0, 'v'},
		{byte(opIncr), 1, 0, 0, 0, 'k'},
		{byte(opCAS), 1, 0, 0, 0, 'k', 1, 0, 0, 0, 'a', 1, 0, 0, 0, 'b'},
		{byte(opExpire), 1, 0, 0, 0, 'k', 0, 0, 0, 0, 0, 0, 0, 1},
		{byte(opPut), 1, 0, 0, 0, 'k', 1, 0, 0, 0, '5', byte(opIncr), 1, 0, 0, 0, 'k'},
	}
	for _, s := range seeds {
		if err := fuzzEncryptedReplayOnce(t, s); err != nil {
			t.Fatalf("seed replay: %v", err)
		}
	}
}

func fuzzEncryptedReplayOnce(t *testing.T, data []byte) error {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	c, _ := newAtRestCipher(testKey)

	wal, err := OpenWALEncrypted(path, c)
	if err != nil {
		return err
	}
	if err := wal.append(data); err != nil {
		wal.Close()
		return err
	}
	if err := wal.Sync(); err != nil {
		wal.Close()
		return err
	}
	if err := wal.Close(); err != nil {
		return err
	}

	wal2, err := OpenWALEncrypted(path, c)
	if err != nil {
		return err
	}
	defer wal2.Close()
	m := NewMemStore()
	m.now = func() time.Time { return time.Unix(0, 0) }
	if err := wal2.Replay(m); err != nil {
		return nil // clean refusal is allowed
	}
	if want := decodeWALPrefix(data, 0); !reflect.DeepEqual(m.data, want) {
		return errors.New("encrypted replay diverged from decoded prefix")
	}
	return nil
}
