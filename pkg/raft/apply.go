package raft

import (
	"context"
	"log"
	"sync"
	"time"
)

// Apply loop — Developer B (M1.4). A single goroutine per node drains
// commitIndex → lastApplied into the state machine, exactly once and in
// order, and notifies waiters that an index has been applied.

// ApplyTracker tracks per-index waiters so callers can block until a log
// entry has been applied (e.g. before answering an HTTP write).
type ApplyTracker struct {
	mu      sync.Mutex
	waiters map[int][]chan struct{}
}

func newApplyTracker() *ApplyTracker {
	return &ApplyTracker{waiters: make(map[int][]chan struct{})}
}

// WaitIndex returns a channel that is closed once log index i has been
// applied. If the index is never committed the caller must time out on its
// own.
func (t *ApplyTracker) WaitIndex(i int) <-chan struct{} {
	ch := make(chan struct{})
	t.mu.Lock()
	t.waiters[i] = append(t.waiters[i], ch)
	t.mu.Unlock()
	return ch
}

func (t *ApplyTracker) applied(i int) {
	t.mu.Lock()
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
// The loop ends when ctx is cancelled or the node is stopped.
func (n *Node) StartApply(ctx context.Context) *ApplyTracker {
	tr := newApplyTracker()
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
			tr.applied(idx)
			continue
		}
		if err := n.applyCmd(entry.Cmd); err != nil {
			log.Printf("raft: apply entry %d failed: %v", idx, err)
		}
		tr.applied(idx)
	}
}
