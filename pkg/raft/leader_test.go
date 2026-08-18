package raft

import (
	"errors"
	"strings"
	"testing"

	"github.com/Raj-Jai/kv-store/pkg/storage"
)

func TestFollowerWriteReturnsNotLeaderError(t *testing.T) {
	leader := "http://peer-a:8080"
	store := storage.NewFakeEngine()
	n := NewNode("n1", peers(4), grantTransport{}, store)
	n.mu.Lock()
	n.leaderID = &leader
	n.mu.Unlock()

	err := n.Put("k", "v")
	var nle *storage.NotLeaderError
	if !errors.As(err, &nle) {
		t.Fatalf("expected NotLeaderError, got %v", err)
	}
	if nle.LeaderAddr != leader {
		t.Fatalf("expected LeaderAddr=%q, got %q", leader, nle.LeaderAddr)
	}
}

func TestFollowerWriteUnknownLeader(t *testing.T) {
	store := storage.NewFakeEngine()
	n := NewNode("n1", peers(4), grantTransport{}, store)

	err := n.Put("k", "v")
	var nle *storage.NotLeaderError
	if !errors.As(err, &nle) {
		t.Fatalf("expected NotLeaderError, got %v", err)
	}
	if nle.LeaderAddr != "" {
		t.Fatalf("expected empty LeaderAddr, got %q", nle.LeaderAddr)
	}
}

func TestLeaderProposeAppendsToLog(t *testing.T) {
	store := storage.NewFakeEngine()
	n := NewNode("n1", peers(2), grantTransport{}, store)
	n.mu.Lock()
	n.becomeLeader()
	n.mu.Unlock()

	if err := n.Put("k", "v"); err != nil {
		t.Fatal(err)
	}
	if err := n.Delete("k"); err != nil {
		t.Fatal(err)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.log) != 2 {
		t.Fatalf("expected 2 log entries, got %d", len(n.log))
	}
	if n.log[0].Cmd.Op != storage.OpPut || n.log[0].Cmd.Key != "k" || n.log[0].Cmd.Value != "v" {
		t.Fatalf("unexpected first entry: %+v", n.log[0])
	}
	if n.log[1].Cmd.Op != storage.OpDelete {
		t.Fatalf("unexpected second entry: %+v", n.log[1])
	}
}

func TestSingleNodePutGetRoundTrip(t *testing.T) {
	store := storage.NewFakeEngine()
	n := NewNode("solo", nil, grantTransport{}, store)

	if err := n.Put("k", "v"); err != nil {
		t.Fatal(err)
	}
	if got, err := n.Get("k"); err != nil || got != "v" {
		t.Fatalf("Get(k) = %q, %v; want v, nil", got, err)
	}
	if err := n.Delete("k"); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Get("k"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestLeaderHelpers(t *testing.T) {
	n := NewNode("n1", peers(1), grantTransport{}, nil)
	if n.IsLeader() {
		t.Fatal("multi-node follower should not be leader")
	}
	if got := n.Leader(); got != "" {
		t.Fatalf("expected empty leader, got %q", got)
	}

	n.mu.Lock()
	n.becomeLeader()
	n.mu.Unlock()
	if !n.IsLeader() {
		t.Fatal("expected IsLeader after becoming leader")
	}
	if got := n.Leader(); got != "n1" {
		t.Fatalf("expected leader n1, got %q", got)
	}
}

func TestUnknownOpError(t *testing.T) {
	n := NewNode("solo", nil, grantTransport{}, nil)
	err := n.applyCmd(storage.Command{Op: 42})
	if err == nil || !strings.Contains(err.Error(), "unknown command op") {
		t.Fatalf("expected unknown-op error, got %v", err)
	}
}
