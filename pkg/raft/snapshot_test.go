package raft

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/Raj-Jai/kv-store/pkg/storage"
)

type fixedSnapshotter struct {
	snap Snapshot
}

func (s fixedSnapshotter) Snapshot() (Snapshot, error) { return s.snap, nil }

type recSink struct {
	mu   sync.Mutex
	data []byte
}

func (s *recSink) ApplySnapshot(data []byte) error {
	s.mu.Lock()
	s.data = append([]byte(nil), data...)
	s.mu.Unlock()
	return nil
}

func (s *recSink) got() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.data...)
}

func TestCompactLogTrimsAndTracksBase(t *testing.T) {
	trans := &followerTransport{followers: map[string]*testFollower{"f1": {}}}
	n := NewNode("leader", []string{"f1"}, trans, storage.NewFakeEngine())
	n.mu.Lock()
	n.becomeLeader()
	n.mu.Unlock()
	preload(n, 2, 5)

	n.CompactLog(3, 2)

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.lastIncludedIndex != 3 || n.lastIncludedTerm != 2 {
		t.Fatalf("base not recorded: %d/%d", n.lastIncludedIndex, n.lastIncludedTerm)
	}
	if n.firstIndex() != 4 || n.lastLogIndex() != 5 || n.lastLogTerm() != 2 {
		t.Fatalf("index math off: first=%d last=%d term=%d", n.firstIndex(), n.lastLogIndex(), n.lastLogTerm())
	}
	if len(n.log) != 2 {
		t.Fatalf("expected 2 retained entries, got %d", len(n.log))
	}
	if _, ok := n.entryAt(3); ok {
		t.Fatal("compacted entry should not be reachable")
	}
	if e, ok := n.entryAt(4); !ok || e.Cmd.Key != "k3" {
		t.Fatalf("entry at 4 wrong: %+v ok=%v", e, ok)
	}
}

func TestCompactLogIdempotent(t *testing.T) {
	trans := &followerTransport{followers: map[string]*testFollower{"f1": {}}}
	n := NewNode("leader", []string{"f1"}, trans, storage.NewFakeEngine())
	n.mu.Lock()
	n.becomeLeader()
	n.mu.Unlock()
	preload(n, 2, 5)

	n.CompactLog(3, 2)
	n.CompactLog(3, 2) // second call must be a no-op
	n.CompactLog(9, 99)

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.lastIncludedIndex != 9 || len(n.log) != 0 {
		t.Fatalf("expected full compaction to base 9, got base=%d len=%d", n.lastIncludedIndex, len(n.log))
	}
	if n.lastLogIndex() != 9 || n.lastLogTerm() != 2 {
		t.Fatalf("full compaction should keep lastLogTerm, got last=%d term=%d", n.lastLogIndex(), n.lastLogTerm())
	}
}

func TestInstallSnapshotToLaggingFollower(t *testing.T) {
	follower := &testFollower{}
	trans := &followerTransport{followers: map[string]*testFollower{"f1": follower}}
	n := NewNode("leader", []string{"f1"}, trans, storage.NewFakeEngine())
	n.SetSnapshotter(fixedSnapshotter{snap: Snapshot{
		LastIncludedIndex: 3,
		LastIncludedTerm:  2,
		Data:              []byte("snapdata"),
	}})
	n.mu.Lock()
	n.becomeLeader()
	n.mu.Unlock()
	preload(n, 2, 5)
	n.CompactLog(3, 2)

	// The follower is empty and needs raft index 1, which is compacted away:
	// the leader must install the snapshot first, then stream entries 4-5.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n.sendHeartbeats()
		if len(follower.snapshots) == 1 && len(follower.snapshot()) == 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if len(follower.snapshots) != 1 {
		t.Fatalf("expected 1 snapshot install, got %d", len(follower.snapshots))
	}
	if !bytes.Equal(follower.snapshots[0], []byte("snapdata")) {
		t.Fatalf("snapshot data mismatch: %q", follower.snapshots[0])
	}
	if follower.lastIncludedIndex != 3 {
		t.Fatalf("follower base not adopted: %d", follower.lastIncludedIndex)
	}
	got := follower.snapshot()
	if len(got) != 2 || got[0].Cmd.Key != "k3" || got[1].Cmd.Key != "k4" {
		t.Fatalf("follower did not receive entries after snapshot: %+v", got)
	}

	// The follower must be marked caught up to the snapshot base.
	if next, match := replIndexes(n, "f1"); next != 6 || match != 5 {
		t.Fatalf("expected next=6 match=5, got next=%d match=%d", next, match)
	}
}

func TestCaughtUpFollowerGetsNoSnapshot(t *testing.T) {
	follower := &testFollower{}
	trans := &followerTransport{followers: map[string]*testFollower{"f1": follower}}
	n := NewNode("leader", []string{"f1"}, trans, storage.NewFakeEngine())
	n.SetSnapshotter(fixedSnapshotter{snap: Snapshot{LastIncludedIndex: 3, LastIncludedTerm: 2}})
	n.mu.Lock()
	n.becomeLeader()
	n.mu.Unlock()
	preload(n, 2, 5)

	// Follower is fully caught up (nextIndex starts at 6); replication must
	// use AppendEntries, never a snapshot.
	for i := 0; i < 5; i++ {
		n.sendHeartbeats()
	}
	if len(follower.snapshots) != 0 {
		t.Fatalf("caught-up follower should not receive snapshots")
	}
}

func TestHandleInstallSnapshotAdoptsBaseAndAppliesData(t *testing.T) {
	sink := &recSink{}
	n := NewNode("a", []string{"p1"}, FakeTransport{}, storage.NewFakeEngine())
	n.SetSnapshotSink(sink)
	n.mu.Lock()
	n.term = 4
	n.mu.Unlock()

	resp := n.HandleInstallSnapshot(InstallSnapshotRequest{
		Term:              5,
		LeaderID:          "L",
		LastIncludedIndex: 3,
		LastIncludedTerm:  2,
		Data:              []byte("state"),
	})
	if !resp.Success {
		t.Fatal("expected snapshot accepted")
	}
	n.mu.Lock()
	if n.lastIncludedIndex != 3 || n.lastIncludedTerm != 2 || n.term != 5 {
		t.Fatalf("base/term not adopted: idx=%d term=%d nterm=%d", n.lastIncludedIndex, n.lastIncludedTerm, n.term)
	}
	if n.role != RoleFollower || n.leaderID == nil || *n.leaderID != "L" {
		t.Fatal("node should be follower of L")
	}
	n.mu.Unlock()
	if !bytes.Equal(sink.got(), []byte("state")) {
		t.Fatalf("snapshot data not applied: %q", sink.got())
	}
}

func TestHandleInstallSnapshotTrimsCoveredEntries(t *testing.T) {
	n := NewNode("a", []string{"p1"}, FakeTransport{}, storage.NewFakeEngine())
	n.mu.Lock()
	n.term = 1
	n.log = []Entry{
		{Term: 1, Cmd: storage.Command{Op: storage.OpPut, Key: "k1"}},
		{Term: 1, Cmd: storage.Command{Op: storage.OpPut, Key: "k2"}},
		{Term: 1, Cmd: storage.Command{Op: storage.OpPut, Key: "k3"}},
	}
	n.mu.Unlock()

	resp := n.HandleInstallSnapshot(InstallSnapshotRequest{
		Term:              2,
		LeaderID:          "L",
		LastIncludedIndex: 2,
		LastIncludedTerm:  1,
	})
	if !resp.Success {
		t.Fatal("expected snapshot accepted")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.log) != 1 || n.log[0].Cmd.Key != "k3" {
		t.Fatalf("covered entries not trimmed: %+v", n.log)
	}
	if n.lastLogIndex() != 3 {
		t.Fatalf("expected lastLogIndex 3, got %d", n.lastLogIndex())
	}
}

func TestHandleInstallSnapshotLowerTermRejected(t *testing.T) {
	n := NewNode("a", []string{"p1"}, FakeTransport{}, storage.NewFakeEngine())
	n.mu.Lock()
	n.term = 5
	n.mu.Unlock()
	resp := n.HandleInstallSnapshot(InstallSnapshotRequest{Term: 4, LeaderID: "L"})
	if resp.Success || resp.Term != 5 {
		t.Fatalf("expected rejection with term 5, got %+v", resp)
	}
}

// snapshotHigherTermTransport rejects AppendEntries at the same term (forcing
// the leader to rewind its nextIndex below the compaction base) and replies
// with a higher term only to InstallSnapshot — so the leader steps down
// specifically because of the snapshot exchange.
type snapshotHigherTermTransport struct {
	snapTerm int
}

func (snapshotHigherTermTransport) RequestVote(_ string, req VoteRequest) (VoteResponse, error) {
	return VoteResponse{Term: req.Term, VoteGranted: true}, nil
}

func (snapshotHigherTermTransport) AppendEntries(_ string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	return AppendEntriesResponse{Term: req.Term, Success: false}, nil
}

func (t snapshotHigherTermTransport) InstallSnapshot(_ string, req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	return InstallSnapshotResponse{Term: t.snapTerm, Success: false}, nil
}

func TestLeaderStepsDownOnHigherTermSnapshot(t *testing.T) {
	n := NewNode("leader", []string{"f1"}, snapshotHigherTermTransport{snapTerm: 99}, storage.NewFakeEngine())
	n.SetSnapshotter(fixedSnapshotter{snap: Snapshot{LastIncludedIndex: 3, LastIncludedTerm: 2}})
	n.mu.Lock()
	n.becomeLeader()
	n.mu.Unlock()
	preload(n, 2, 5)
	n.CompactLog(3, 2)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n.sendHeartbeats()
		n.mu.Lock()
		if n.role == RoleFollower && n.term == 99 {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	t.Fatalf("expected step-down to follower at 99, got role=%v term=%d", n.role, n.term)
}
