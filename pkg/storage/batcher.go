package storage

import (
	"errors"
	"time"
)

const (
	// maxBatchSize bounds the number of operations flushed in one fsync.
	maxBatchSize = 1000
	// flushInterval is the maximum time an operation waits before its batch
	// is flushed to disk.
	flushInterval = 10 * time.Millisecond
)

// batchRequest is a queued mutation awaiting a durable write.
type batchRequest struct {
	op        opCode
	key       string
	old       string
	value     string
	expiresAt int64
	result    any
	errChan   chan error
}

// batchLoop collects writes into batches and decides when to write each batch
// with a single fsync: when it is full or after flushInterval. Ops that need
// the current memory state to decide their outcome (Incr/CAS/Expire) are
// never batched: the pending batch is flushed first so they see every prior
// acknowledged write, then they are processed singly. Closing batchChan
// writes the remainder and signals batchDone.
func (s *DiskStore) batchLoop() {
	var batch []*batchRequest
	var timer *time.Timer
	var timerC <-chan time.Time

	for {
		select {
		case req, ok := <-s.batchChan:
			if !ok {
				s.writeBatch(batch)
				close(s.batchDone)
				return
			}
			if req.op >= opIncr {
				s.writeBatch(batch)
				batch = nil
				if timer != nil {
					timer.Stop()
					timer = nil
					timerC = nil
				}
				s.writeSingle(req)
				continue
			}
			batch = append(batch, req)
			if timer == nil {
				timer = time.NewTimer(flushInterval)
				timerC = timer.C
			}
			if len(batch) >= maxBatchSize {
				s.writeBatch(batch)
				batch = nil
				timer.Stop()
				timer = nil
				timerC = nil
			}
		case <-timerC:
			s.writeBatch(batch)
			batch = nil
			timer.Stop()
			timer = nil
			timerC = nil
		}
	}
}

// writeSingle durably executes one state-dependent write. The batch is empty
// at this point, so the memory state reflects every prior acknowledged write.
func (s *DiskStore) writeSingle(req *batchRequest) {
	s.snapMu.Lock()

	var finalErr error
	switch req.op {
	case opIncr:
		if err := s.wal.AppendIncr(req.key); err != nil {
			finalErr = err
			break
		}
		if err := s.wal.Sync(); err != nil {
			finalErr = err
			break
		}
		v, err := s.mem.Incr(req.key)
		if err != nil {
			finalErr = err
			break
		}
		req.result = v
	case opCAS:
		cur, gerr := s.mem.Get(req.key)
		if errors.Is(gerr, ErrNotFound) {
			finalErr = ErrNotFound
			break
		}
		if gerr != nil {
			finalErr = gerr
			break
		}
		if cur != req.old {
			req.result = false
			break
		}
		if err := s.wal.AppendCAS(req.key, req.old, req.value); err != nil {
			finalErr = err
			break
		}
		if err := s.wal.Sync(); err != nil {
			finalErr = err
			break
		}
		s.mem.Put(req.key, req.value)
		req.result = true
	case opExpire:
		if _, gerr := s.mem.Get(req.key); errors.Is(gerr, ErrNotFound) {
			finalErr = ErrNotFound
			break
		} else if gerr != nil {
			finalErr = gerr
			break
		}
		if err := s.wal.AppendExpire(req.key, req.expiresAt); err != nil {
			finalErr = err
			break
		}
		if err := s.wal.Sync(); err != nil {
			finalErr = err
			break
		}
		finalErr = s.mem.Expire(req.key, req.expiresAt)
	default:
		finalErr = errors.New("storage: unexpected single op")
	}

	s.snapMu.Unlock()
	req.errChan <- finalErr
}

// writeBatch appends the batch to the WAL, performs a single fsync, then
// applies the operations to memory in the same order.
func (s *DiskStore) writeBatch(batch []*batchRequest) {
	if len(batch) == 0 {
		return
	}

	s.snapMu.Lock()

	var finalErr error
	for _, req := range batch {
		var err error
		switch req.op {
		case opPut:
			err = s.wal.AppendPut(req.key, req.value)
		case opDelete:
			err = s.wal.AppendDelete(req.key)
		case opClear:
			err = s.wal.AppendClear()
		}
		if err != nil {
			finalErr = err
			break
		}
	}

	if finalErr == nil {
		finalErr = s.wal.Sync()
	}

	if finalErr == nil {
		for _, req := range batch {
			switch req.op {
			case opPut:
				s.mem.Put(req.key, req.value)
			case opDelete:
				s.mem.Delete(req.key)
			case opClear:
				s.mem.Clear()
			}
		}
	}

	s.snapMu.Unlock()

	for _, req := range batch {
		req.errChan <- finalErr
	}
}