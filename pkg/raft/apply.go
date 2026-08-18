package raft

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

// Apply loop — Developer B (M1.4). A single goroutine per node drains
// commitIndex → lastApplied into the state machine, exactly once and in
// order, and notifies waiters that an index has been applied.

// ApplyTracker tracks per-index waiters so callers can block until a log
// entry has been applied (e.g. before answering an HTTP write), and records
// the result each applied entry produced.
type ApplyTracker struct {
	mu      sync.Mutex
	waiters map[int][]chan struct{}
	results map[int]applyResult
}

// applyResult is what applying one log entry produced. val is non-nil only
// for ops that return a value (Incr's new value, CAS's outcome).
type applyResult struct {
	val any
	err error
}

func newApplyTracker() *ApplyTracker {
	return &ApplyTracker{
		waiters: make(map[int][]chan struct{}),
		results: make(map[int]applyResult),
	}
}

// WaitIndex returns a channel that is closed once log index i has been
// applied. If the index is never committed the caller must time out on its
// own. The result of applying i is available via Result once the channel is
// closed.
func (t *ApplyTracker) WaitIndex(i int) <-chan struct{} {
	ch := make(chan struct{})
	t.mu.Lock()
	t.waiters[i] = append(t.waiters[i], ch)
	t.mu.Unlock()
	return ch
}

// Result returns the apply result for an index whose waiter channel has been
// closed.
func (t *ApplyTracker) Result(i int) (any, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	r, ok := t.results[i]
	if !ok {
		return nil, errors.New("raft: no apply result recorded for index")
	}
	return r.val, r.err
}

func (t *ApplyTracker) applied(i int, r applyResult) {
	t.mu.Lock()
	t.results[i] = r
	for _, ch := range t.waiters[i] {
		close(ch)
	}
	delete(t.waiters, i)
	t.mu.Unlock()
}

// ApplyIndex reports the highest applied log index.
func (n *Node) ApplyIndex() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied
}

// SnapshotBase reports the compaction base: the raft log index of the last
// entry folded into a snapshot (0 when nothing has been compacted).
func (n *Node) SnapshotBase() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastIncludedIndex
}

// LogTerm reports the term of the entry at raft index i, falling back to the
// compaction base's term for indexes at or below the base. Returns -1 when
// the index is unknown (ahead of the log).
func (n *Node) LogTerm(i int) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.logTermAt(i)
}

// Flush forces any pending durable raft state — including a compaction base
// recorded by CompactLog — to be written now. Call it after CompactLog so a
// crash never leaves the base only in memory.
func (n *Node) Flush() error {
	return n.persist()
}

// StartApply launches the apply loop as a single goroutine and returns a
// tracker for callers that want to block until a specific index is applied.
// The tracker is also retained on the node so state-dependent writes
// (Incr/CAS/Expire) can wait for their own apply result. The loop ends when
// ctx is cancelled or the node is stopped.
func (n *Node) StartApply(ctx context.Context) *ApplyTracker {
	tr := newApplyTracker()
	n.mu.Lock()
	n.applyTr = tr
	n.mu.Unlock()
	go n.applyLoop(ctx, tr)
	return tr
}

func (n *Node) applyLoop(ctx context.Context, tr *ApplyTracker) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stop:
			return
		default:
		}

		n.mu.Lock()
		if n.lastApplied >= n.commitIndex {
			n.mu.Unlock()
			time.Sleep(time.Millisecond)
			continue
		}
		n.lastApplied++
		idx := n.lastApplied
		entry, ok := n.entryAt(idx)
		n.mu.Unlock()

		if !ok {
			// Already compacted into a snapshot and applied from it.
			tr.applied(idx, applyResult{})
			continue
		}
		res, err := n.applyCmd(entry.Cmd)
		if err != nil {
			log.Printf("raft: apply entry %d failed: %v", idx, err)
		}
		tr.applied(idx, applyResult{val: res, err: err})
	}
}
