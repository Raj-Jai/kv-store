package raft

import (
	"testing"

	"github.com/Raj-Jai/kv-store/pkg/storage"
)

// newTestNode builds a node on the FakeTransport with the given log, so tests
// can drive the inbound handlers directly.
func newTestNode(log []Entry) *Node {
	n := NewNode("n1", []string{"p1", "p2"}, FakeTransport{}, storage.NewFakeEngine())
	if len(log) > 0 {
		n.mu.Lock()
		n.log = log
		n.mu.Unlock()
	}
	return n
}

func cmdPut(key string) storage.Command {
	return storage.Command{Op: storage.OpPut, Key: key, Value: "v"}
}

func TestRequestVoteGranted(t *testing.T) {
	n := newTestNode(nil)
	n.mu.Lock()
	n.term = 3
	n.mu.Unlock()

	resp := n.HandleRequestVote(VoteRequest{Term: 3, CandidateID: "c1"})
	if !resp.VoteGranted || resp.Term != 3 {
		t.Fatalf("expected granted vote at term 3, got %+v", resp)
	}
	// At most one vote per term.
	if resp2 := n.HandleRequestVote(VoteRequest{Term: 3, CandidateID: "c2"}); resp2.VoteGranted {
		t.Fatal("granted a second vote in the same term")
	}
	// Idempotent retry by the same candidate.
	if resp3 := n.HandleRequestVote(VoteRequest{Term: 3, CandidateID: "c1"}); !resp3.VoteGranted {
		t.Fatal("should re-grant to the same candidate")
	}
}

func TestRequestVoteDeniedWhenLogStale(t *testing.T) {
	n := newTestNode([]Entry{{Term: 3, Cmd: cmdPut("a")}})
	n.mu.Lock()
	n.term = 3
	n.mu.Unlock()

	// Candidate's last term is behind ours.
	if resp := n.HandleRequestVote(VoteRequest{Term: 3, CandidateID: "c", LastLogIndex: 1, LastLogTerm: 2}); resp.VoteGranted {
		t.Fatal("candidate with a stale log must not win the vote")
	}
	// Candidate has no log at all while we have one.
	if resp := n.HandleRequestVote(VoteRequest{Term: 3, CandidateID: "c", LastLogIndex: 0, LastLogTerm: 0}); resp.VoteGranted {
		t.Fatal("candidate with an empty log must not win against a non-empty log")
	}
}

func TestRequestVoteLowerTermDenied(t *testing.T) {
	n := newTestNode(nil)
	n.mu.Lock()
	n.term = 5
	n.mu.Unlock()

	resp := n.HandleRequestVote(VoteRequest{Term: 3, CandidateID: "c"})
	if resp.VoteGranted || resp.Term != 5 {
		t.Fatalf("expected denial at our term, got %+v", resp)
	}
}

func TestRequestVoteHigherTermStepsDown(t *testing.T) {
	n := newTestNode(nil)
	n.mu.Lock()
	n.term = 2
	n.mu.Unlock()

	resp := n.HandleRequestVote(VoteRequest{Term: 5, CandidateID: "c"})
	if !resp.VoteGranted {
		t.Fatal("expected grant at higher term with empty log")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.term != 5 || n.role != RoleFollower {
		t.Fatalf("expected follower at term 5, got term=%d role=%v", n.term, n.role)
	}
}

func TestAppendEntriesAppendsAndAdvancesCommit(t *testing.T) {
	n := newTestNode([]Entry{{Term: 1, Cmd: cmdPut("a")}})

	resp := n.HandleAppendEntries(AppendEntriesRequest{
		Term: 2, LeaderID: "L", PrevLogIndex: 1, PrevLogTerm: 1,
		Entries:      []Entry{{Term: 2, Cmd: cmdPut("b")}},
		LeaderCommit: 2,
	})
	if !resp.Success || resp.Term != 2 {
		t.Fatalf("expected success at term 2, got %+v", resp)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.log) != 2 || n.log[1].Cmd.Key != "b" {
		t.Fatalf("log not appended: %+v", n.log)
	}
	if n.commitIndex != 2 {
		t.Fatalf("commit index = %d, want 2", n.commitIndex)
	}
}

func TestAppendEntriesTruncatesDivergentSuffix(t *testing.T) {
	n := newTestNode([]Entry{
		{Term: 1, Cmd: cmdPut("a")},
		{Term: 1, Cmd: cmdPut("stale")},
		{Term: 1, Cmd: cmdPut("stale")},
	})

	resp := n.HandleAppendEntries(AppendEntriesRequest{
		Term: 2, LeaderID: "L", PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []Entry{{Term: 2, Cmd: cmdPut("b")}, {Term: 2, Cmd: cmdPut("c")}},
	})
	if !resp.Success {
		t.Fatal("expected success")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.log) != 3 || n.log[0].Cmd.Key != "a" || n.log[1].Cmd.Key != "b" || n.log[2].Cmd.Key != "c" {
		t.Fatalf("divergent suffix not overwritten: %+v", n.log)
	}
	if n.log[1].Term != 2 || n.log[2].Term != 2 {
		t.Fatalf("replacement entries must carry the leader term: %+v", n.log)
	}
}

func TestAppendEntriesHeartbeatTruncatesExtraSuffix(t *testing.T) {
	// Follower holds uncommitted entries beyond the leader's log; a heartbeat
	// must drop them so the logs converge.
	n := newTestNode([]Entry{
		{Term: 1, Cmd: cmdPut("a")},
		{Term: 1, Cmd: cmdPut("b")},
		{Term: 2, Cmd: cmdPut("stale")},
	})

	resp := n.HandleAppendEntries(AppendEntriesRequest{
		Term: 2, LeaderID: "L", PrevLogIndex: 2, PrevLogTerm: 1, LeaderCommit: 2,
	})
	if !resp.Success {
		t.Fatal("expected success")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.log) != 2 {
		t.Fatalf("stale suffix not dropped: %+v", n.log)
	}
	if n.commitIndex != 2 {
		t.Fatalf("commit index = %d, want 2", n.commitIndex)
	}
}

func TestAppendEntriesRejectsStaleTerm(t *testing.T) {
	n := newTestNode(nil)
	n.mu.Lock()
	n.term = 5
	n.mu.Unlock()

	resp := n.HandleAppendEntries(AppendEntriesRequest{Term: 3, LeaderID: "L"})
	if resp.Success || resp.Term != 5 {
		t.Fatalf("expected rejection at our term, got %+v", resp)
	}
}

func TestAppendEntriesRejectsMissingPrefix(t *testing.T) {
	n := newTestNode([]Entry{{Term: 1, Cmd: cmdPut("a")}})
	resp := n.HandleAppendEntries(AppendEntriesRequest{Term: 2, LeaderID: "L", PrevLogIndex: 5, PrevLogTerm: 1})
	if resp.Success {
		t.Fatal("expected rejection for a missing prefix")
	}
}

func TestAppendEntriesRejectsTermMismatch(t *testing.T) {
	n := newTestNode([]Entry{{Term: 1, Cmd: cmdPut("a")}})
	resp := n.HandleAppendEntries(AppendEntriesRequest{Term: 2, LeaderID: "L", PrevLogIndex: 1, PrevLogTerm: 9})
	if resp.Success {
		t.Fatal("expected rejection for a prev-term mismatch")
	}
}

func TestAppendEntriesStepsDownCandidate(t *testing.T) {
	n := newTestNode(nil)
	n.mu.Lock()
	n.becomeCandidate()
	n.mu.Unlock()

	resp := n.HandleAppendEntries(AppendEntriesRequest{Term: 3, LeaderID: "L"})
	if !resp.Success {
		t.Fatal("expected success from a same-term leader")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != RoleFollower || n.term != 3 {
		t.Fatalf("expected follower at term 3, got role=%v term=%d", n.role, n.term)
	}
	if n.leaderID == nil || *n.leaderID != "L" {
		t.Fatalf("leader not recorded: %v", n.leaderID)
	}
}

func TestAppendEntriesStepsDownLeader(t *testing.T) {
	n := newTestNode(nil)
	n.mu.Lock()
	n.becomeCandidate()
	n.becomeLeader()
	n.mu.Unlock()

	resp := n.HandleAppendEntries(AppendEntriesRequest{Term: 1, LeaderID: "L"})
	if !resp.Success {
		t.Fatal("expected success")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != RoleFollower {
		t.Fatalf("leader must step down on a same-term AppendEntries from another leader, role=%v", n.role)
	}
}
