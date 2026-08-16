package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// opCode identifies the type of a WAL record.
type opCode uint8

const (
	opPut opCode = iota + 1
	opDelete
	opClear
)

// WAL is an append-only log of mutations in the format
// [opCode][keyLen][key][valLen][value]. An operation is durable once Sync
// returns; Replay restores the logged state into memory.
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

// Truncate empties the log file. Callers must ensure no append is in flight.
func (w *WAL) Truncate() error {
	return os.Truncate(w.path, 0)
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

// DiskStore is a durable Engine: mutations are queued, appended to the WAL,
// and synced with a single fsync per batch before being applied to memory, so
// an acknowledged write survives a crash. Recovery loads the latest snapshot
// and replays any WAL entries written after it.
type DiskStore struct {
	mem *MemStore
	wal *WAL

	batchChan chan *batchRequest
	batchDone chan struct{}
	snapMu    sync.Mutex // serializes WAL append/apply against snapshot+truncate

	snapshotPath string
	walPath      string

	stopSnap  chan struct{}
	snapDone  chan struct{}
	closeOnce sync.Once
	closeMu   sync.RWMutex // guards the closed flag and batchChan shutdown
	closed    bool
}

// OpenDiskStore creates a persistent store in dataDir, restoring any
// previously acknowledged state by loading the snapshot and replaying the WAL.
func OpenDiskStore(dataDir string) (*DiskStore, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	wal, err := OpenWAL(filepath.Join(dataDir, "wal.log"))
	if err != nil {
		return nil, err
	}

	s := &DiskStore{
		mem:          NewMemStore(),
		wal:          wal,
		walPath:      filepath.Join(dataDir, "wal.log"),
		snapshotPath: filepath.Join(dataDir, "snapshot.dat"),
		batchChan:    make(chan *batchRequest, 2*maxBatchSize),
		batchDone:    make(chan struct{}),
		stopSnap:     make(chan struct{}),
		snapDone:     make(chan struct{}),
	}

	if err := s.loadSnapshot(); err != nil {
		wal.Close()
		return nil, err
	}
	if err := s.wal.Replay(s.mem); err != nil {
		wal.Close()
		return nil, err
	}

	go s.batchLoop()
	go s.snapshotLoop()

	return s, nil
}

func (s *DiskStore) Get(key string) (string, error) {
	return s.mem.Get(key)
}

func (s *DiskStore) Put(key, value string) error {
	return s.submit(&batchRequest{op: opPut, key: key, value: value})
}

func (s *DiskStore) Delete(key string) error {
	return s.submit(&batchRequest{op: opDelete, key: key})
}

func (s *DiskStore) Clear() error {
	return s.submit(&batchRequest{op: opClear})
}

// submit queues a mutation and blocks until it is durable. Writes issued after
// the store is closed return ErrClosed.
func (s *DiskStore) submit(req *batchRequest) error {
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed {
		return ErrClosed
	}
	req.errChan = make(chan error, 1)
	s.batchChan <- req
	return <-req.errChan
}

// Close drains pending writes, saves a snapshot, truncates the WAL, and
// closes the log file. In-flight writes finish and stay durable; new writes
// fail with ErrClosed.
func (s *DiskStore) Close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		close(s.stopSnap)
		<-s.snapDone

		s.closeMu.Lock()
		s.closed = true
		close(s.batchChan)
		s.closeMu.Unlock()

		<-s.batchDone

		if err := s.compact(); err != nil {
			closeErr = err
		}
		if err := s.wal.Close(); err != nil {
			closeErr = err
		}
	})
	return closeErr
}