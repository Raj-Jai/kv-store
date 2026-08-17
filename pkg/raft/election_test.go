package raft

import (
	"testing"
)

type grantTransport struct{}

func (grantTransport) RequestVote(_ string, req VoteRequest) (VoteResponse, error) {
	return VoteResponse{Term: req.Term, VoteGranted: true}, nil
}

func (grantTransport) AppendEntries(_ string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	return AppendEntriesResponse{Term: req.Term, Success: true}, nil
}

func (grantTransport) InstallSnapshot(_ string, req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	return InstallSnapshotResponse{Term: req.Term, Success: true}, nil
}

type denyTransport struct{}

func (denyTransport) RequestVote(_ string, req VoteRequest) (VoteResponse, error) {
	return VoteResponse{Term: req.Term, VoteGranted: false}, nil
}

func (denyTransport) AppendEntries(_ string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	return AppendEntriesResponse{Term: req.Term, Success: true}, nil
}

func (denyTransport) InstallSnapshot(_ string, req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	return InstallSnapshotResponse{Term: req.Term, Success: true}, nil
}

type higherTermTransport struct{ term int }

func (t higherTermTransport) RequestVote(_ string, req VoteRequest) (VoteResponse, error) {
	return VoteResponse{Term: t.term, VoteGranted: false}, nil
}

func (t higherTermTransport) AppendEntries(_ string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	return AppendEntriesResponse{Term: t.term, Success: false}, nil
}

func (t higherTermTransport) InstallSnapshot(_ string, req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	return InstallSnapshotResponse{Term: t.term, Success: false}, nil
}

func elect(t *testing.T, n *Node) {
	t.Helper()
	n.startElection()
}

func peers(n int) []string {
	p := make([]string, n)
	for i := range p {
		p[i] = string(rune('a' + i))
	}
	return p
}

func TestNewNodeSingleNodeIsLeader(t *testing.T) {
	n := NewNode("solo", nil, FakeTransport{}, nil)
	leader, isLeader := n.LeaderID()
	if !isLeader || leader != "solo" {
		t.Fatalf("single node should be leader, got leader=%q isLeader=%v", leader, isLeader)
	}
}

func TestNoPeersBecomesLeader(t *testing.T) {
	n := NewNode("solo", nil, grantTransport{}, nil)
	elect(t, n)
	if n.role != RoleLeader {
		t.Fatal("single node should win its own election")
	}
}

func TestMajorityVotesBecomesLeader(t *testing.T) {
	// 4 peers + self = 5 nodes, quorum 3. All grant.
	n := NewNode("n1", peers(4), grantTransport{}, nil)
	elect(t, n)
	if n.role != RoleLeader {
		t.Fatalf("expected leader with full majority, got role %v", n.role)
	}
}

func TestMajorityWithoutSelfVoteNeedsThreeVotes(t *testing.T) {
	// quorum math: 4 peers + self, floor(5/2)+1 = 3.
	if got := majority(peers(4)); got != 3 {
		t.Fatalf("majority(5 nodes) = %d, want 3", got)
	}
	if got := majority(nil); got != 1 {
		t.Fatalf("majority(1 node) = %d, want 1", got)
	}
}

func TestInsufficientVotesStaysFollower(t *testing.T) {
	n := NewNode("n1", peers(4), denyTransport{}, nil)
	elect(t, n)
	if n.role != RoleFollower {
		t.Fatalf("expected to remain follower without majority, got %v", n.role)
	}
}

func TestHigherTermResponseStepsDown(t *testing.T) {
	n := NewNode("n1", peers(1), higherTermTransport{term: 99}, nil)
	elect(t, n)

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != RoleFollower {
		t.Fatalf("expected step-down to follower, got %v", n.role)
	}
	if n.term != 99 {
		t.Fatalf("expected to adopt term 99, got %d", n.term)
	}
}

func TestHeartbeatHigherTermStepsDown(t *testing.T) {
	n := NewNode("n1", peers(1), higherTermTransport{term: 42}, nil)
	n.mu.Lock()
	n.becomeLeader()
	n.mu.Unlock()
	n.sendHeartbeats()

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != RoleFollower || n.term != 42 {
		t.Fatalf("expected follower at term 42, got role=%v term=%d", n.role, n.term)
	}
}

func TestStopIsIdempotent(t *testing.T) {
	n := NewNode("n1", nil, FakeTransport{}, nil)
	n.Stop()
	n.Stop() // must not panic
}
