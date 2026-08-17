package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Raj-Jai/kv-store/pkg/storage"
)

// verifyingTransport fails any RequestVote whose raft state (term + self-vote)
// is not already durable — it proves persist() ran before the RPC was sent.
type verifyingTransport struct {
	store RaftStore
}

func (t verifyingTransport) RequestVote(_ string, req VoteRequest) (VoteResponse, error) {
	st, err := t.store.Load()
	if err != nil {
		return VoteResponse{}, err
	}
	if st.Term != req.Term || st.VotedFor == nil || *st.VotedFor != req.CandidateID {
		return VoteResponse{}, fmt.Errorf("raft state not durable before RequestVote: %+v", st)
	}
	return VoteResponse{Term: req.Term, VoteGranted: true}, nil
}

func (verifyingTransport) AppendEntries(_ string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	return AppendEntriesResponse{Term: req.Term, Success: true}, nil
}

func (verifyingTransport) InstallSnapshot(_ string, req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	return InstallSnapshotResponse{Term: req.Term, Success: true}, nil
}

func newFileStore(t *testing.T) RaftStore {
	t.Helper()
	return NewFileRaftStore(filepath.Join(t.TempDir(), "raftstate.json"))
}

func TestFileRaftStoreRoundTrip(t *testing.T) {
	s := newFileStore(t)
	votedFor := "c1"
	want := RaftState{
		Term:              7,
		VotedFor:          &votedFor,
		Log:               []Entry{{Term: 7, Cmd: storage.Command{Op: storage.OpPut, Key: "k", Value: "v"}}},
		LastIncludedIndex: 3,
		LastIncludedTerm:  2,
	}
	if err := s.Save(want); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.Term != 7 || got.VotedFor == nil || *got.VotedFor != "c1" {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	if got.LastIncludedIndex != 3 || got.LastIncludedTerm != 2 {
		t.Fatalf("compaction base lost: %+v", got)
	}
	if len(got.Log) != 1 || got.Log[0].Cmd.Key != "k" {
		t.Fatalf("log lost: %+v", got.Log)
	}
}

func TestElectionStateDurableBeforeRequestVote(t *testing.T) {
	store := newFileStore(t)
	n := NewNode("c", []string{"p1"}, verifyingTransport{store: store}, storage.NewFakeEngine())
	if err := n.SetRaftStore(store); err != nil {
		t.Fatal(err)
	}
	n.startElection()
	if !n.IsLeader() {
		t.Fatal("expected single-round election to succeed")
	}
}

func TestVotePersistedBeforeGrantResponse(t *testing.T) {
	store := newFileStore(t)
	n := NewNode("a", []string{"p1"}, FakeTransport{}, storage.NewFakeEngine())
	if err := n.SetRaftStore(store); err != nil {
		t.Fatal(err)
	}

	resp := n.HandleRequestVote(VoteRequest{Term: 3, CandidateID: "c"})
	if !resp.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	st, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if st.Term != 3 || st.VotedFor == nil || *st.VotedFor != "c" {
		t.Fatalf("vote not durable when response returned: %+v", st)
	}
}

func TestNoDoubleVoteAfterRestart(t *testing.T) {
	store := newFileStore(t)

	// First life: grants a vote to c1 in term 5.
	n1 := NewNode("a", []string{"p1"}, FakeTransport{}, storage.NewFakeEngine())
	if err := n1.SetRaftStore(store); err != nil {
		t.Fatal(err)
	}
	if resp := n1.HandleRequestVote(VoteRequest{Term: 5, CandidateID: "c1"}); !resp.VoteGranted {
		t.Fatal("expected first vote granted")
	}

	// Crash: node is discarded.

	// Second life: same durable state, a different candidate asks for the vote
	// in term 5 — must be refused, otherwise one term would hold two votes.
	n2 := NewNode("a", []string{"p1"}, FakeTransport{}, storage.NewFakeEngine())
	if err := n2.SetRaftStore(store); err != nil {
		t.Fatal(err)
	}
	if resp := n2.HandleRequestVote(VoteRequest{Term: 5, CandidateID: "c2"}); resp.VoteGranted {
		t.Fatal("double vote in term 5 after restart")
	}
	// The original candidate is still the recorded vote.
	if resp := n2.HandleRequestVote(VoteRequest{Term: 5, CandidateID: "c1"}); !resp.VoteGranted {
		t.Fatal("expected re-grant for the same candidate")
	}
}

func TestLogPersistedOnProposeAndRestored(t *testing.T) {
	store := newFileStore(t)
	trans := &followerTransport{followers: map[string]*testFollower{"f1": {}}}

	n := NewNode("leader", []string{"f1"}, trans, storage.NewFakeEngine())
	if err := n.SetRaftStore(store); err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	n.becomeLeader()
	n.mu.Unlock()

	for i := 0; i < 3; i++ {
		if err := n.Put(fmt.Sprintf("k%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}

	// Crash + restart on the same durable state.
	n2 := NewNode("leader", []string{"f1"}, trans, storage.NewFakeEngine())
	if err := n2.SetRaftStore(store); err != nil {
		t.Fatal(err)
	}
	n2.mu.Lock()
	defer n2.mu.Unlock()
	if len(n2.log) != 3 {
		t.Fatalf("expected 3 log entries after restart, got %d", len(n2.log))
	}
	if n2.log[2].Cmd.Key != "k2" {
		t.Fatalf("unexpected last entry: %+v", n2.log[2])
	}
}

func TestCompactionBasePersistedAndRestored(t *testing.T) {
	store := newFileStore(t)
	trans := &followerTransport{followers: map[string]*testFollower{"f1": {}}}
	n := NewNode("leader", []string{"f1"}, trans, storage.NewFakeEngine())
	if err := n.SetRaftStore(store); err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	n.becomeLeader()
	n.mu.Unlock()
	preload(n, 2, 5)
	n.CompactLog(3, 2)
	n.persist()

	n2 := NewNode("leader", []string{"f1"}, trans, storage.NewFakeEngine())
	if err := n2.SetRaftStore(store); err != nil {
		t.Fatal(err)
	}
	n2.mu.Lock()
	defer n2.mu.Unlock()
	if n2.lastIncludedIndex != 3 || n2.lastIncludedTerm != 2 {
		t.Fatalf("compaction base not restored: idx=%d term=%d", n2.lastIncludedIndex, n2.lastIncludedTerm)
	}
	if n2.lastLogIndex() != 5 || len(n2.log) != 2 {
		t.Fatalf("unexpected log after restore: lastLogIndex=%d len=%d", n2.lastLogIndex(), len(n2.log))
	}
}

func TestFileStoreLoadMissingFileIsEmpty(t *testing.T) {
	s := NewFileRaftStore(filepath.Join(t.TempDir(), "absent.json"))
	st, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if st.Term != 0 || st.VotedFor != nil || len(st.Log) != 0 {
		t.Fatalf("expected empty state, got %+v", st)
	}
}

func TestFileStoreCorruptFileErrors(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := os.WriteFile(path, []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := NewFileRaftStore(path)
	if _, err := s.Load(); err == nil {
		t.Fatal("expected error loading corrupt state")
	}
}

func TestUnknownOpErrorIsNotDirty(t *testing.T) {
	// applyCmd on an unknown op must fail cleanly.
	store := newFileStore(t)
	n := NewNode("leader", []string{"f1"}, FakeTransport{}, storage.NewFakeEngine())
	if err := n.SetRaftStore(store); err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	n.becomeLeader()
	n.mu.Unlock()
	if err := n.applyCmd(storage.Command{Op: 42}); err == nil {
		t.Fatal("expected error for unknown op")
	}
}

func TestProposePersistsBeforeReplication(t *testing.T) {
	store := newFileStore(t)
	// The follower transport records whether a heartbeat/append arrives
	// before the leader's log is durable.
	trans := &followerTransport{followers: map[string]*testFollower{"f1": {}}}
	n := NewNode("leader", []string{"f1"}, trans, storage.NewFakeEngine())
	if err := n.SetRaftStore(store); err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	n.becomeLeader()
	n.mu.Unlock()

	if err := n.Put("k", "v"); err != nil {
		t.Fatal(err)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Log) != 1 || st.Log[0].Cmd.Key != "k" {
		t.Fatalf("proposed entry not durable: %+v", st.Log)
	}
}