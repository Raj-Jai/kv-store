package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

const (
	snapshotName      = "snapshot.dat"
	snapshotInterval  = 5 * time.Second
	snapshotThreshold = 1024 * 1024 // WAL size in bytes that triggers compaction
)

// snapshotVersion is the on-disk snapshot format. Version 1 was the raw
// map[string]string; version 2 wraps the data in an envelope so TTLs persist.
const snapshotVersion = 2

// snapshotFile is the version-2 on-disk snapshot envelope.
type snapshotFile struct {
	V    int              `json:"v"`
	Data map[string]entry `json:"data"`
}

// snapshotLoop periodically compacts the store once the WAL exceeds
// snapshotThreshold, so the log never grows unbounded.
func (s *DiskStore) snapshotLoop() {
	defer close(s.snapDone)

	ticker := time.NewTicker(snapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.compactIfNeeded(); err != nil {
				log.Printf("snapshot failed: %v", err)
			}
		case <-s.stopSnap:
			return
		}
	}
}

func (s *DiskStore) compactIfNeeded() error {
	info, err := os.Stat(s.walPath)
	if err != nil {
		return err
	}
	if info.Size() < snapshotThreshold {
		return nil
	}
	return s.compact()
}

// compact writes the current memory state to a snapshot and empties the WAL,
// so recovery needs only the snapshot plus any entries appended after it.
func (s *DiskStore) compact() error {
	s.snapMu.Lock()
	defer s.snapMu.Unlock()
	return s.compactLocked()
}

// saveSnapshot serializes memory state to snapshot.dat atomically. With a
// cipher configured the JSON payload is sealed before writing: the file is
// [snapshotMagic][nonce||ciphertext||tag], and a plaintext file is the raw
// JSON (detected by snapshotMagic's absence).
func (s *DiskStore) saveSnapshot() error {
	s.mem.mu.RLock()
	data, err := json.Marshal(snapshotFile{V: snapshotVersion, Data: s.mem.data})
	s.mem.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}
	if s.cipher != nil {
		sealed, err := s.cipher.Encrypt(data)
		if err != nil {
			return fmt.Errorf("encrypt snapshot: %w", err)
		}
		blob := make([]byte, 0, len(snapshotMagic)+len(sealed))
		blob = append(blob, snapshotMagic...)
		blob = append(blob, sealed...)
		data = blob
	}
	return atomicWriteFile(s.snapshotPath, data, 0644)
}

// SerializeSnapshot returns the current memory state in the on-disk snapshot
// format, for shipping to a lagging raft peer (Developer B, M1.6). It is
// safe to call concurrently with the periodic compaction loop.
func (s *DiskStore) SerializeSnapshot() ([]byte, error) {
	s.snapMu.Lock()
	defer s.snapMu.Unlock()

	s.mem.mu.RLock()
	defer s.mem.mu.RUnlock()
	data, err := json.Marshal(snapshotFile{V: snapshotVersion, Data: s.mem.data})
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot: %w", err)
	}
	return data, nil
}

// RestoreSnapshot replaces memory state with a snapshot received from a raft
// leader and persists it as snapshot.dat, so the installed state survives a
// crash even if the raft log base was not yet recorded (Developer B, M1.6).
func (s *DiskStore) RestoreSnapshot(data []byte) error {
	s.snapMu.Lock()
	defer s.snapMu.Unlock()

	// Replace, not merge: decode into a fresh map so keys absent from the
	// snapshot (e.g. deleted before the leader took it) do not survive in the
	// follower's state machine.
	fresh := make(map[string]entry)
	if err := decodeSnapshot(data, fresh); err != nil {
		return err
	}
	s.mem.mu.Lock()
	s.mem.data = fresh
	s.mem.mu.Unlock()

	if err := s.saveSnapshot(); err != nil {
		return err
	}
	return s.wal.Truncate()
}

// Compact forces a compaction now: the current memory state is written to
// snapshot.dat and the WAL is emptied. Call it after a raft log compaction
// has been triggered, before the raft log base is advanced, so a crash in
// between leaves a recoverable (idempotent) prefix (Developer B, M1.6).
func (s *DiskStore) Compact() error {
	s.snapMu.Lock()
	defer s.snapMu.Unlock()
	return s.compactLocked()
}

func (s *DiskStore) compactLocked() error {
	if err := s.saveSnapshot(); err != nil {
		return err
	}
	return s.wal.Truncate()
}

// loadSnapshot replaces memory state with the contents of snapshot.dat,
// decrypting an at-rest sealed snapshot when a cipher is configured.
func (s *DiskStore) loadSnapshot() error {
	data, err := os.ReadFile(s.snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read snapshot: %w", err)
	}
	encrypted := len(data) >= len(snapshotMagic) && string(data[:len(snapshotMagic)]) == snapshotMagic
	if encrypted && s.cipher == nil {
		return errors.New("snapshot is encrypted at rest; provide ENCRYPTION_KEY or KEY_FILE")
	}
	if encrypted {
		sealed := data[len(snapshotMagic):]
		data, err = s.cipher.Decrypt(sealed)
		if err != nil {
			return fmt.Errorf("decrypt snapshot: %w", err)
		}
	} else if s.cipher != nil {
		return errors.New("existing snapshot is not encrypted; provide no key or re-encrypt the data directory")
	}
	fresh := make(map[string]entry)
	if err := decodeSnapshot(data, fresh); err != nil {
		return fmt.Errorf("load snapshot: %w", err)
	}
	s.mem.data = fresh
	return nil
}

// decodeSnapshot parses a snapshot blob into dst. It accepts the version-2
// envelope, and migrates version-1 snapshots (a raw map[string]string) so
// old data directories still load.
func decodeSnapshot(data []byte, dst map[string]entry) error {
	var sf snapshotFile
	if err := json.Unmarshal(data, &sf); err == nil && (sf.V > 0 || sf.Data != nil) {
		for k, e := range sf.Data {
			dst[k] = e
		}
		return nil
	}

	var v1 map[string]string
	if err := json.Unmarshal(data, &v1); err != nil {
		return fmt.Errorf("unmarshal snapshot: %w", err)
	}
	for k, v := range v1 {
		dst[k] = entry{Value: v}
	}
	return nil
}

// atomicWriteFile writes data to path via a temp file, fsync, and rename, so
// readers never observe a partially written file.
func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp := path + ".tmp"

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer os.Remove(tmp)

	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync temp file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}

	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsync dir: %w", err)
	}
	return nil
}
