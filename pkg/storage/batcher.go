package storage

import "time"

const (
	// maxBatchSize bounds the number of operations flushed in one fsync.
	maxBatchSize = 1000
	// flushInterval is the maximum time an operation waits before its batch
	// is flushed to disk.
	flushInterval = 10 * time.Millisecond
)

// batchRequest is a queued mutation awaiting a durable write.
type batchRequest struct {
	op      opCode
	key     string
	value   string
	errChan chan error
}

// batchLoop collects writes into batches and decides when to write each batch
// with a single fsync: when it is full or after flushInterval. Closing
// batchChan writes the remainder and signals batchDone.
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