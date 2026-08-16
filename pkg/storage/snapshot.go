package storage

import (
	"encoding/json"
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

	if err := s.saveSnapshot(); err != nil {
		return err
	}
	return s.wal.Truncate()
}

// saveSnapshot serializes memory state to snapshot.dat atomically.
func (s *DiskStore) saveSnapshot() error {
	s.mem.mu.RLock()
	data, err := json.Marshal(s.mem.data)
	s.mem.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}
	return atomicWriteFile(s.snapshotPath, data, 0644)
}

// loadSnapshot replaces memory state with the contents of snapshot.dat.
func (s *DiskStore) loadSnapshot() error {
	data, err := os.ReadFile(s.snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read snapshot: %w", err)
	}
	if err := json.Unmarshal(data, &s.mem.data); err != nil {
		return fmt.Errorf("unmarshal snapshot: %w", err)
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