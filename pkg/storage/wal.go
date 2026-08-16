package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// opCode identifies the type of a WAL record.
type opCode uint8

const (
	opPut opCode = iota + 1
	opDelete
	opClear
)

// WAL is an append-only log of mutations in the format
// [opCode][keyLen][key][valLen][value]. An operation is durable once
// Sync returns; Replay restores the logged state into memory.
type WAL struct {
	path string
	f    *os.File
}

// OpenWAL opens the log at path, creating it if it does not exist.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return &WAL{path: path, f: f}, nil
}

func (w *WAL) AppendPut(key, value string) error {
	buf := make([]byte, 0, 9+len(key)+len(value))
	buf = append(buf, byte(opPut))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(key)))
	buf = append(buf, key...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(value)))
	buf = append(buf, value...)
	return w.append(buf)
}

func (w *WAL) AppendDelete(key string) error {
	buf := make([]byte, 0, 5+len(key))
	buf = append(buf, byte(opDelete))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(key)))
	buf = append(buf, key...)
	return w.append(buf)
}

func (w *WAL) AppendClear() error {
	return w.append([]byte{byte(opClear)})
}

func (w *WAL) append(b []byte) error {
	_, err := w.f.Write(b)
	return err
}

// Sync flushes buffered writes to disk.
func (w *WAL) Sync() error {
	return w.f.Sync()
}

// Close closes the underlying file.
func (w *WAL) Close() error {
	return w.f.Close()
}

// Replay reads every record in order and applies it to m, restoring the
// state acknowledged before any crash.
func (w *WAL) Replay(m *MemStore) error {
	f, err := os.Open(w.path)
	if err != nil {
		return fmt.Errorf("open wal for replay: %w", err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		op, err := r.ReadByte()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read wal: %w", err)
		}

		switch opCode(op) {
		case opPut:
			key, err := readString(r)
			if err != nil {
				return fmt.Errorf("read wal put key: %w", err)
			}
			value, err := readString(r)
			if err != nil {
				return fmt.Errorf("read wal put value: %w", err)
			}
			m.Put(key, value)
		case opDelete:
			key, err := readString(r)
			if err != nil {
				return fmt.Errorf("read wal delete key: %w", err)
			}
			m.Delete(key)
		case opClear:
			m.Clear()
		default:
			return fmt.Errorf("corrupted wal: unknown op code %d", op)
		}
	}
}

func readString(r *bufio.Reader) (string, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return "", err
	}
	buf := make([]byte, binary.BigEndian.Uint32(lenBuf[:]))
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// DiskStore is a durable Engine: every mutation is appended to the WAL and
// synced before it is applied to memory, so an acknowledged write survives a
// crash. Recovery replays the WAL into memory on startup.
type DiskStore struct {
	mem *MemStore
	wal *WAL
}

// OpenDiskStore creates a persistent store in dataDir, restoring any
// previously acknowledged state by replaying the WAL.
func OpenDiskStore(dataDir string) (*DiskStore, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	wal, err := OpenWAL(filepath.Join(dataDir, "wal.log"))
	if err != nil {
		return nil, err
	}

	s := &DiskStore{mem: NewMemStore(), wal: wal}
	if err := s.wal.Replay(s.mem); err != nil {
		wal.Close()
		return nil, err
	}
	return s, nil
}

func (s *DiskStore) Get(key string) (string, error) {
	return s.mem.Get(key)
}

func (s *DiskStore) Put(key, value string) error {
	if err := s.wal.AppendPut(key, value); err != nil {
		return err
	}
	if err := s.wal.Sync(); err != nil {
		return err
	}
	return s.mem.Put(key, value)
}

func (s *DiskStore) Delete(key string) error {
	if err := s.wal.AppendDelete(key); err != nil {
		return err
	}
	if err := s.wal.Sync(); err != nil {
		return err
	}
	return s.mem.Delete(key)
}

func (s *DiskStore) Clear() error {
	if err := s.wal.AppendClear(); err != nil {
		return err
	}
	if err := s.wal.Sync(); err != nil {
		return err
	}
	return s.mem.Clear()
}

// Close flushes pending writes and closes the WAL.
func (s *DiskStore) Close() error {
	if err := s.wal.Sync(); err != nil {
		return err
	}
	return s.wal.Close()
}
