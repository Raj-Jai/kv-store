package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// opCode identifies the type of a WAL record.
type opCode uint8

const (
	opPut opCode = iota + 1
	opDelete
	opClear
	opIncr
	opCAS
	opExpire
)

// maxRecordField bounds a single key/value payload in a WAL record. The HTTP
// layer rejects request bodies above this size, so a length header claiming
// more is corruption, not a legitimate write. Bounding the declared length
// keeps replay from allocating memory proportional to an attacker-controlled
// header before it discovers the record is truncated.
const maxRecordField = 1 << 20

// WAL is an append-only log of mutations in the format
// [opCode][keyLen][key][valLen][value]. An operation is durable once Sync
// returns; Replay restores the logged state into memory.
//
// When opened with a cipher (OpenWALEncrypted), every record is sealed with
// AES-256-GCM before it is written: the file starts with walEncryptedMagic,
// then each record is [uint32 sealedLen][nonce||ciphertext||tag]. The framing
// after the magic is per-record length-prefixed because the record bodies are
// opaque ciphertext that the plaintext parser could not scan.
type WAL struct {
	path   string
	f      *os.File
	cipher *atRestCipher
}

// OpenWAL opens the log at path, creating it if it does not exist.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return &WAL{path: path, f: f}, nil
}

// OpenWALEncrypted opens (or creates) an at-rest encrypted log. A freshly
// created file is stamped with walEncryptedMagic. An existing file must
// already carry the magic, otherwise it was written without encryption and
// opening it under a cipher would silently leave data at rest in plaintext.
func OpenWALEncrypted(path string, c *atRestCipher) (*WAL, error) {
	w, err := OpenWAL(path)
	if err != nil {
		return nil, err
	}
	st, err := w.f.Stat()
	if err != nil {
		w.Close()
		return nil, fmt.Errorf("stat wal: %w", err)
	}
	if st.Size() == 0 {
		if _, err := w.f.Write([]byte{walEncryptedMagic}); err != nil {
			w.Close()
			return nil, fmt.Errorf("stamp encrypted wal: %w", err)
		}
		if err := w.f.Sync(); err != nil {
			w.Close()
			return nil, fmt.Errorf("sync encrypted wal stamp: %w", err)
		}
		return &WAL{path: path, f: w.f, cipher: c}, nil
	}
	first := make([]byte, 1)
	if _, err := w.f.ReadAt(first, 0); err != nil {
		w.Close()
		return nil, fmt.Errorf("read wal header: %w", err)
	}
	if first[0] != walEncryptedMagic {
		w.Close()
		return nil, errors.New("existing wal is not encrypted; provide no key or re-encrypt the data directory")
	}
	return &WAL{path: path, f: w.f, cipher: c}, nil
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

// AppendIncr logs an increment of key. Replay re-evaluates the arithmetic,
// which is deterministic given an identical prefix.
func (w *WAL) AppendIncr(key string) error {
	buf := make([]byte, 0, 5+len(key))
	buf = append(buf, byte(opIncr))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(key)))
	buf = append(buf, key...)
	return w.append(buf)
}

// AppendCAS logs a compare-and-swap of key. The record is written only when
// the compare matched at append time; replay re-evaluates the compare.
func (w *WAL) AppendCAS(key, old, new string) error {
	buf := make([]byte, 0, 9+len(key)+len(old)+len(new))
	buf = append(buf, byte(opCAS))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(key)))
	buf = append(buf, key...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(old)))
	buf = append(buf, old...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(new)))
	buf = append(buf, new...)
	return w.append(buf)
}

// AppendExpire logs an absolute expiry deadline (unix nanoseconds) for key.
func (w *WAL) AppendExpire(key string, expiresAt int64) error {
	buf := make([]byte, 0, 13+len(key))
	buf = append(buf, byte(opExpire))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(key)))
	buf = append(buf, key...)
	buf = binary.BigEndian.AppendUint64(buf, uint64(expiresAt))
	return w.append(buf)
}

func (w *WAL) append(b []byte) error {
	if w.cipher != nil {
		sealed, err := w.cipher.Encrypt(b)
		if err != nil {
			return err
		}
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(sealed)))
		if _, err := w.f.Write(lenBuf[:]); err != nil {
			return err
		}
		_, err = w.f.Write(sealed)
		return err
	}
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
// An encrypted log re-stamps its magic header so the next record stays framed.
func (w *WAL) Truncate() error {
	if err := os.Truncate(w.path, 0); err != nil {
		return err
	}
	if w.cipher != nil {
		// After truncate the size is 0, so an O_APPEND write lands at offset 0,
		// re-stamping the magic header that prefixes every sealed record.
		if _, err := w.f.Write([]byte{walEncryptedMagic}); err != nil {
			return err
		}
		return w.f.Sync()
	}
	return nil
}

// Replay reads every record in order and applies it to m, restoring the
// state acknowledged before any crash.
func (w *WAL) Replay(m *MemStore) error {
	if w.cipher != nil {
		return w.replayEncrypted(m)
	}
	return w.replayPlain(m)
}

func (w *WAL) replayPlain(m *MemStore) error {
	f, err := os.Open(w.path)
	if err != nil {
		return fmt.Errorf("open wal for replay: %w", err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	st, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat wal for replay: %w", err)
	}
	remaining := st.Size()

	for remaining > 0 {
		op, err := r.ReadByte()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read wal: %w", err)
		}
		remaining--

		consumed, err := applyRecord(op, r, remaining, m)
		if err != nil {
			return err
		}
		remaining -= consumed
	}
	return nil
}

// replayEncrypted reads the sealed records of an encrypted log, decrypting
// each one before parsing it. A truncated tail record (a crash mid-append) is
// skipped, matching the plaintext parser's tolerance of a partial record.
func (w *WAL) replayEncrypted(m *MemStore) error {
	f, err := os.Open(w.path)
	if err != nil {
		return fmt.Errorf("open encrypted wal for replay: %w", err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	magic, err := r.ReadByte()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read encrypted wal header: %w", err)
	}
	if magic != walEncryptedMagic {
		return errors.New("encrypted wal missing header magic")
	}

	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err == io.EOF {
			return nil
		} else if err != nil {
			// A crash between the header and a record body leaves a trailing
			// fragment; treat it as a truncated append and stop.
			return nil
		}
		sealedLen := binary.BigEndian.Uint32(lenBuf[:])
		if uint64(sealedLen) > maxRecordField*2+64 {
			return fmt.Errorf("encrypted wal record exceeds bound: %d", sealedLen)
		}
		sealed := make([]byte, sealedLen)
		if _, err := io.ReadFull(r, sealed); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("read encrypted wal record: %w", err)
		}
		plain, err := w.cipher.Decrypt(sealed)
		if err != nil {
			return fmt.Errorf("decrypt wal record: %w", err)
		}
		// A sealed blob is a self-contained stream of one or more records.
		// Production appends seal exactly one record per blob; parsing a
		// stream keeps replay correct and lets fuzzers feed arbitrary blobs.
		br := bufio.NewReader(bytes.NewReader(plain))
		remaining := int64(len(plain))
		for remaining > 0 {
			op, err := br.ReadByte()
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("read decrypted wal record: %w", err)
			}
			remaining--
			consumed, err := applyRecord(op, br, remaining, m)
			if err != nil {
				return err
			}
			remaining -= consumed
		}
	}
}

// applyRecord applies one log record whose op byte was already read. r is
// positioned at the record's fields; remaining is how many field bytes exist
// after the op byte. It returns the number of field bytes consumed.
func applyRecord(op byte, r *bufio.Reader, remaining int64, m *MemStore) (int64, error) {
	switch opCode(op) {
	case opPut:
		key, n1, err := readString(r, remaining)
		if err != nil {
			return 0, fmt.Errorf("read wal put key: %w", err)
		}
		value, n2, err := readString(r, remaining-n1)
		if err != nil {
			return 0, fmt.Errorf("read wal put value: %w", err)
		}
		m.Put(key, value)
		return n1 + n2, nil
	case opDelete:
		key, n, err := readString(r, remaining)
		if err != nil {
			return 0, fmt.Errorf("read wal delete key: %w", err)
		}
		m.Delete(key)
		return n, nil
	case opClear:
		m.Clear()
		return 0, nil
	case opIncr:
		key, n, err := readString(r, remaining)
		if err != nil {
			return 0, fmt.Errorf("read wal incr key: %w", err)
		}
		// Deterministic re-evaluation: an incr that failed at apply time
		// (non-numeric value, or overflow at int64 max) changed no state,
		// so replaying it is a no-op (mirrors the leniency applied to
		// CAS/Expire below).
		if _, err := m.Incr(key); err != nil && !errors.Is(err, ErrNotNumeric) && !errors.Is(err, ErrOverflow) {
			return 0, fmt.Errorf("replay wal incr: %w", err)
		}
		return n, nil
	case opCAS:
		key, n1, err := readString(r, remaining)
		if err != nil {
			return 0, fmt.Errorf("read wal cas key: %w", err)
		}
		old, n2, err := readString(r, remaining-n1)
		if err != nil {
			return 0, fmt.Errorf("read wal cas old: %w", err)
		}
		new, n3, err := readString(r, remaining-n1-n2)
		if err != nil {
			return 0, fmt.Errorf("read wal cas new: %w", err)
		}
		// Deterministic re-evaluation: a CAS whose key is absent is a
		// no-op, exactly as it would have been at apply time.
		if _, err := m.CAS(key, old, new); err != nil && !errors.Is(err, ErrNotFound) {
			return 0, fmt.Errorf("replay wal cas: %w", err)
		}
		return n1 + n2 + n3, nil
	case opExpire:
		key, n1, err := readString(r, remaining)
		if err != nil {
			return 0, fmt.Errorf("read wal expire key: %w", err)
		}
		if remaining-n1 < 8 {
			return 0, fmt.Errorf("read wal expire ts: %w", io.ErrUnexpectedEOF)
		}
		var tsBuf [8]byte
		if _, err := io.ReadFull(r, tsBuf[:]); err != nil {
			return 0, fmt.Errorf("read wal expire ts: %w", err)
		}
		expiresAt := int64(binary.BigEndian.Uint64(tsBuf[:]))
		// Deterministic re-evaluation: expiring an absent key is a no-op.
		if err := m.Expire(key, expiresAt); err != nil && !errors.Is(err, ErrNotFound) {
			return 0, fmt.Errorf("replay wal expire: %w", err)
		}
		return n1 + 8, nil
	default:
		return 0, fmt.Errorf("corrupted wal: unknown op code %d", op)
	}
}

// readString reads a BigEndian length-prefixed string. remaining is the number
// of bytes still present in the file; a declared length beyond it (or beyond
// maxRecordField) is rejected before any allocation, so a corrupt header can
// never trigger a large allocation. It returns the number of bytes consumed.
func readString(r *bufio.Reader, remaining int64) (string, int64, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return "", 0, err
	}
	length := binary.BigEndian.Uint32(lenBuf[:])
	if length > maxRecordField {
		return "", 4, fmt.Errorf("wal record field exceeds %d bytes", maxRecordField)
	}
	if int64(length) > remaining-4 {
		return "", 4, io.ErrUnexpectedEOF
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", 4 + int64(length), err
	}
	return string(buf), 4 + int64(length), nil
}

// DiskStore is a durable Engine: mutations are queued, appended to the WAL,
// and synced with a single fsync per batch before being applied to memory, so
// an acknowledged write survives a crash. Recovery loads the latest snapshot
// and replays any WAL entries written after it.
type DiskStore struct {
	mem *MemStore
	wal *WAL

	// cipher encrypts wal.log and snapshot.dat at rest when non-nil
	// (ENCRYPTION_KEY / KEY_FILE). Snapshots shipped to raft peers over the
	// network stay in the plaintext JSON format; only persistence is sealed.
	cipher *atRestCipher

	batchChan chan *batchRequest
	batchDone chan struct{}
	snapMu    sync.Mutex // serializes WAL append/apply against snapshot+truncate

	snapshotPath string
	walPath      string

	stopSnap  chan struct{}
	snapDone  chan struct{}
	stopExp   chan struct{}
	expDone   chan struct{}
	closeOnce sync.Once
	closeMu   sync.RWMutex // guards the closed flag and batchChan shutdown
	closed    bool
}

// OpenDiskStore creates a persistent store in dataDir, restoring any
// previously acknowledged state by loading the snapshot and replaying the WAL.
func OpenDiskStore(dataDir string) (*DiskStore, error) {
	return openDiskStore(dataDir, nil)
}

// OpenDiskStoreWithKey is OpenDiskStore with AES-256-GCM encryption at rest
// for wal.log and snapshot.dat. The key must be exactly 32 bytes; every file
// in dataDir must have been written under the same key.
func OpenDiskStoreWithKey(dataDir string, key []byte) (*DiskStore, error) {
	c, err := newAtRestCipher(key)
	if err != nil {
		return nil, err
	}
	return openDiskStore(dataDir, c)
}

func openDiskStore(dataDir string, cipher *atRestCipher) (*DiskStore, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	var wal *WAL
	var err error
	if cipher != nil {
		wal, err = OpenWALEncrypted(filepath.Join(dataDir, "wal.log"), cipher)
	} else {
		wal, err = OpenWAL(filepath.Join(dataDir, "wal.log"))
	}
	if err != nil {
		return nil, err
	}

	s := &DiskStore{
		mem:          NewMemStore(),
		wal:          wal,
		cipher:       cipher,
		walPath:      filepath.Join(dataDir, "wal.log"),
		snapshotPath: filepath.Join(dataDir, "snapshot.dat"),
		batchChan:    make(chan *batchRequest, 2*maxBatchSize),
		batchDone:    make(chan struct{}),
		stopSnap:     make(chan struct{}),
		snapDone:     make(chan struct{}),
		stopExp:      make(chan struct{}),
		expDone:      make(chan struct{}),
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
	go s.expireLoop()

	return s, nil
}

func (s *DiskStore) Get(key string) (string, error) {
	return s.mem.Get(key)
}

func (s *DiskStore) Put(key, value string) error {
	_, err := s.submit(&batchRequest{op: opPut, key: key, value: value})
	return err
}

func (s *DiskStore) Delete(key string) error {
	_, err := s.submit(&batchRequest{op: opDelete, key: key})
	return err
}

func (s *DiskStore) Clear() error {
	_, err := s.submit(&batchRequest{op: opClear})
	return err
}

// Incr atomically increments key by 1 and returns the new value. It is
// serialized with every other write and durable before the memory state
// changes.
func (s *DiskStore) Incr(key string) (int64, error) {
	res, err := s.submit(&batchRequest{op: opIncr, key: key})
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

// CAS swaps key to new when its value equals old, returning true on success
// and false on a mismatch (ErrNotFound when the key is absent).
func (s *DiskStore) CAS(key, old, new string) (bool, error) {
	res, err := s.submit(&batchRequest{op: opCAS, key: key, old: old, value: new})
	if err != nil {
		return false, err
	}
	return res.(bool), nil
}

// Expire sets an absolute expiry deadline (unix nanoseconds) for key.
func (s *DiskStore) Expire(key string, expiresAt int64) error {
	_, err := s.submit(&batchRequest{op: opExpire, key: key, expiresAt: expiresAt})
	return err
}

// Scan pages over live keys sorting after cursor that match pattern.
func (s *DiskStore) Scan(cursor string, count int, pattern string) ([]KeyValue, string, error) {
	return s.mem.Scan(cursor, count, pattern)
}

// Size returns the number of live keys.
func (s *DiskStore) Size() int { return s.mem.Size() }

// ExpiredCount reports how many keys the store has dropped by expiry.
func (s *DiskStore) ExpiredCount() uint64 { return s.mem.ExpiredCount() }

// submit queues a mutation and blocks until it is durable. Writes issued after
// the store is closed return ErrClosed.
func (s *DiskStore) submit(req *batchRequest) (any, error) {
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	req.errChan = make(chan error, 1)
	s.batchChan <- req
	err := <-req.errChan
	if err != nil {
		return nil, err
	}
	return req.result, nil
}

// Close drains pending writes, saves a snapshot, truncates the WAL, and
// closes the log file. In-flight writes finish and stay durable; new writes
// fail with ErrClosed.
func (s *DiskStore) Close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		close(s.stopSnap)
		<-s.snapDone
		close(s.stopExp)
		<-s.expDone

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

// expireInterval is how often the active-expiration sweep runs.
const expireInterval = time.Second

// expireLoop actively drops expired keys in the background so memory does not
// grow with dead entries between lazy-expiry hits.
func (s *DiskStore) expireLoop() {
	defer close(s.expDone)

	ticker := time.NewTicker(expireInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.mem.SweepExpired()
		case <-s.stopExp:
			return
		}
	}
}
