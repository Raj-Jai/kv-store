package raft

import (
	"context"
	"testing"
	"time"

	"github.com/Raj-Jai/kv-store/pkg/storage"
)

func waitApplied(t *testing.T, tr *ApplyTracker, idx int) {
	t.Helper()
	select {
	case <-tr.WaitIndex(idx):
	case <-time.After(2 * time.Second):
		t.Fatalf("entry %d never applied", idx)
	}
}

func TestApplyAppliesCommittedInOrder(t *testing.T) {
	n := newTestNode([]Entry{
		{Term: 1, Cmd: cmdPut("a")},
		{Term: 1, Cmd: cmdPut("b")},
		{Term: 2, Cmd: cmdPut("c")},
	})
	n.mu.Lock()
	n.commitIndex = 3
	n.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr := n.StartApply(ctx)
	defer n.Stop()

	waitApplied(t, tr, 3)

	store := n.store.(*storage.FakeEngine)
	for _, k := range []string{"a", "b", "c"} {
		if got, err := store.Get(k); err != nil || got != "v" {
			t.Fatalf("key %s = %q, %v; want %q", k, got, err, "v")
		}
	}
	if n.ApplyIndex() != 3 {
		t.Fatalf("ApplyIndex = %d, want 3", n.ApplyIndex())
	}
}

func TestApplyDoesNotApplyUncommitted(t *testing.T) {
	n := newTestNode([]Entry{
		{Term: 1, Cmd: cmdPut("a")},
		{Term: 1, Cmd: cmdPut("b")},
	})
	n.mu.Lock()
	n.commitIndex = 1
	n.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr := n.StartApply(ctx)
	defer n.Stop()

	waitApplied(t, tr, 1)
	// The loop must not race ahead of commitIndex.
	time.Sleep(50 * time.Millisecond)

	store := n.store.(*storage.FakeEngine)
	if _, err := store.Get("b"); err != storage.ErrNotFound {
		t.Fatalf("uncommitted entry was applied: %q", "b")
	}
	if n.ApplyIndex() != 1 {
		t.Fatalf("ApplyIndex = %d, want 1", n.ApplyIndex())
	}
}

func TestApplyAppliesExactlyOnceInOrder(t *testing.T) {
	n := newTestNode([]Entry{
		{Term: 1, Cmd: storage.Command{Op: storage.OpPut, Key: "k", Value: "1"}},
		{Term: 1, Cmd: storage.Command{Op: storage.OpPut, Key: "k", Value: "2"}},
		{Term: 1, Cmd: storage.Command{Op: storage.OpPut, Key: "k", Value: "3"}},
	})
	n.mu.Lock()
	n.commitIndex = 3
	n.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr := n.StartApply(ctx)
	defer n.Stop()

	waitApplied(t, tr, 3)

	store := n.store.(*storage.FakeEngine)
	v, err := store.Get("k")
	if err != nil || v != "3" {
		t.Fatalf("final value %q, %v; want %q", v, err, "3")
	}
	if n.ApplyIndex() != 3 {
		t.Fatalf("ApplyIndex = %d, want 3", n.ApplyIndex())
	}
}
