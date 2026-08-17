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
		entry := n.log[idx-1]
		n.mu.Unlock()

		if err := n.applyCmd(entry.Cmd); err != nil {
			log.Printf("raft: apply entry %d failed: %v", idx, err)
		}
		tr.applied(idx)
	}
}
