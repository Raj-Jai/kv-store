package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Raj-Jai/kv-store/pkg/storage"
)

// testFollower is a test-side stand-in for Developer B's inbound node.go: it
// applies B's log-matching rule so Developer A can grade leader-side
// replication without the real follower.
type testFollower struct {
	mu                sync.Mutex
	log               []Entry
	lastIncludedIndex int
	lastIncludedTerm  int
	rejectAll         bool
	snapshots         [][]byte
}

func (f *testFollower) lastLogIndex() int { return f.lastIncludedIndex + len(f.log) }

func (f *testFollower) logTermAt(index int) int {
	if index == 0 {
		return 0
	}
	if index <= f.lastIncludedIndex {
		return f.lastIncludedTerm
	}
	if off := index - f.lastIncludedIndex - 1; off < len(f.log) {
		return f.log[off].Term
	}
	return -1
}

func (f *testFollower) handle(req AppendEntriesRequest) AppendEntriesResponse {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rejectAll {
		return AppendEntriesResponse{Term: req.Term, Success: false}
	}
	if req.PrevLogIndex > f.lastLogIndex() {
		return AppendEntriesResponse{Term: req.Term, Success: false}
	}
	if req.PrevLogIndex > 0 && f.logTermAt(req.PrevLogIndex) != req.PrevLogTerm {
		return AppendEntriesResponse{Term: req.Term, Success: false}
	}
	keep := req.PrevLogIndex - f.lastIncludedIndex
	if keep < 0 {
		keep = 0
	}
	if keep > len(f.log) {
		keep = len(f.log)
	}
	f.log = append(f.log[:keep], req.Entries...)
	return AppendEntriesResponse{Term: req.Term, Success: true}
}

func (f *testFollower) install(req InstallSnapshotRequest) InstallSnapshotResponse {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rejectAll {
		return InstallSnapshotResponse{Term: req.Term, Success: false}
	}
	f.log = nil
	f.lastIncludedIndex = req.LastIncludedIndex
	f.lastIncludedTerm = req.LastIncludedTerm
	f.snapshots = append(f.snapshots, append([]byte(nil), req.Data...))
	return InstallSnapshotResponse{Term: req.Term, Success: true}
}

func (f *testFollower) snapshot() []Entry {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Entry(nil), f.log...)
}

// followerTransport dispatches AppendEntries to a named testFollower. A
// missing peer rejects; when higherTerm is set every AppendEntries returns
// that term (the hostile step-down case).
type followerTransport struct {
	mu         sync.Mutex
	followers  map[string]*testFollower
	higherTerm int
}

func (t *followerTransport) RequestVote(_ string, req VoteRequest) (VoteResponse, error) {
	return VoteResponse{Term: req.Term, VoteGranted: true}, nil
}

func (t *followerTransport) AppendEntries(peer string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	if t.higherTerm > req.Term {
		return AppendEntriesResponse{Term: t.higherTerm, Success: false}, nil
	}
	t.mu.Lock()
	f := t.followers[peer]
	t.mu.Unlock()
	if f == nil {
		return AppendEntriesResponse{Term: req.Term, Success: false}, nil
	}
	return f.handle(req), nil
}

func (t *followerTransport) InstallSnapshot(peer string, req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	if t.higherTerm > req.Term {
		return InstallSnapshotResponse{Term: t.higherTerm, Success: false}, nil
	}
	t.mu.Lock()
	f := t.followers[peer]
	t.mu.Unlock()
	if f == nil {
		return InstallSnapshotResponse{Term: req.Term, Success: false}, nil
	}
	return f.install(req), nil
}

func leaderWithFollowers(t *testing.T, f *followerTransport, store storage.Engine) *Node {
	t.Helper()
	var peers []string
	for p := range f.followers {
		peers = append(peers, p)
	}
	n := NewNode("leader", peers, f, store)
	n.mu.Lock()
	n.becomeLeader()
	n.mu.Unlock()
	return n
}

func waitFollowerLog(t *testing.T, f *testFollower, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(f.snapshot()) == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("follower log never reached %d entries (got %d)", want, len(f.snapshot()))
}

func preloadLeaderLog(n *Node, term int, count int) {
	n.mu.Lock()
	for i := 0; i < count; i++ {
		n.log = append(n.log, Entry{Term: term, Cmd: storage.Command{
			Op: storage.OpPut, Key: fmt.Sprintf("k%d", i), Value: "v",
		}})
	}
	n.mu.Unlock()
}

func TestProposeReplicatesToFollower(t *testing.T) {
	follower := &testFollower{}
	trans := &followerTransport{followers: map[string]*testFollower{"f1": follower}}
	n := leaderWithFollowers(t, trans, storage.NewFakeEngine())

	for i := 0; i < 3; i++ {
		if err := n.Put(fmt.Sprintf("k%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	waitFollowerLog(t, follower, 3)

	got := follower.snapshot()
	for i, e := range got {
		if e.Cmd.Key != fmt.Sprintf("k%d", i) {
			t.Fatalf("entry %d: unexpected key %q", i, e.Cmd.Key)
		}
	}
}

func TestProposeReplicatesDelete(t *testing.T) {
	follower := &testFollower{}
	trans := &followerTransport{followers: map[string]*testFollower{"f1": follower}}
	n := leaderWithFollowers(t, trans, storage.NewFakeEngine())

	if err := n.Put("k", "v"); err != nil {
		t.Fatal(err)
	}
	if err := n.Delete("k"); err != nil {
		t.Fatal(err)
	}
	waitFollowerLog(t, follower, 2)

	got := follower.snapshot()
	if got[0].Cmd.Op != storage.OpPut || got[1].Cmd.Op != storage.OpDelete {
		t.Fatalf("expected put then delete, got %+v", got)
	}
}

func TestCatchUpLaggingFollower(t *testing.T) {
	follower := &testFollower{} // starts with an empty log
	trans := &followerTransport{followers: map[string]*testFollower{"f1": follower}}
	n := leaderWithFollowers(t, trans, storage.NewFakeEngine())

	preloadLeaderLog(n, 2, 5)
	// Drive replication directly (heartbeat path) since the preloaded
	// entries predate any propose.
	converged := false
	for i := 0; i < 100 && !converged; i++ {
		n.sendHeartbeats()
		converged = len(follower.snapshot()) == 5
	}
	if !converged {
		t.Fatalf("lagging follower never caught up, log len %d", len(follower.snapshot()))
	}
	if _, match := replIndexes(n, "f1"); match != 5 {
		t.Fatalf("expected matchIndex 5, got %d", match)
	}
}

func TestDivergentSuffixOverwritten(t *testing.T) {
	// The follower holds stale entries at conflicting terms; the leader must
	// rewind to a matching prefix and overwrite the divergent suffix.
	follower := &testFollower{log: []Entry{
		{Term: 1, Cmd: storage.Command{Op: storage.OpPut, Key: "stale"}},
		{Term: 1, Cmd: storage.Command{Op: storage.OpPut, Key: "stale"}},
		{Term: 1, Cmd: storage.Command{Op: storage.OpPut, Key: "stale"}},
	}}
	trans := &followerTransport{followers: map[string]*testFollower{"f1": follower}}
	n := leaderWithFollowers(t, trans, storage.NewFakeEngine())

	preloadLeaderLog(n, 2, 5)

	converged := false
	for i := 0; i < 100 && !converged; i++ {
		n.sendHeartbeats()
		converged = len(follower.snapshot()) == 5
	}
	if !converged {
		t.Fatalf("divergent follower never converged, log len %d", len(follower.snapshot()))
	}
	got := follower.snapshot()
	if got[0].Term != 2 || got[0].Cmd.Key != "k0" {
		t.Fatalf("stale suffix not overwritten: %+v", got[0])
	}
}

func TestRejectedFollowerRewindsButFloorsAtOne(t *testing.T) {
	// A peer that never accepts: nextIndex must rewind with backoff and never
	// drop below 1, and matchIndex must stay 0.
	trans := &followerTransport{followers: map[string]*testFollower{
		"f1": {rejectAll: true},
	}}
	n := leaderWithFollowers(t, trans, storage.NewFakeEngine())

	preloadLeaderLog(n, 1, 3)
	for i := 0; i < 10; i++ {
		n.sendHeartbeats()
	}

	next, match := replIndexes(n, "f1")
	if next < 1 {
		t.Fatalf("nextIndex dropped below 1: %d", next)
	}
	if match != 0 {
		t.Fatalf("expected matchIndex 0 for never-accepting peer, got %d", match)
	}
}

func TestHigherTermFollowerStepsDownLeader(t *testing.T) {
	follower := &testFollower{}
	trans := &followerTransport{followers: map[string]*testFollower{"f1": follower}, higherTerm: 99}
	n := leaderWithFollowers(t, trans, storage.NewFakeEngine())

	preloadLeaderLog(n, 1, 2)
	n.sendHeartbeats()

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != RoleFollower || n.term != 99 {
		t.Fatalf("expected follower at term 99, got role=%v term=%d", n.role, n.term)
	}
}

func replIndexes(n *Node, peer string) (next, match int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	st := n.replState()
	return st.nextIndex[peer], st.matchIndex[peer]
}
