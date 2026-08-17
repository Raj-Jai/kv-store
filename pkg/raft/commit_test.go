package raft

import (
	"fmt"
	"testing"

	"github.com/Raj-Jai/kv-store/pkg/storage"
)

// commitLeader builds a 5-node leader (4 followers) whose transport serves
// the given followers.
func commitLeader(t *testing.T, followers map[string]*testFollower) *Node {
	t.Helper()
	trans := &followerTransport{followers: followers}
	var peers []string
	for p := range followers {
		peers = append(peers, p)
	}
	n := NewNode("leader", peers, trans, storage.NewFakeEngine())
	n.mu.Lock()
	n.becomeLeader()
	n.mu.Unlock()
	return n
}

func commitIndex(n *Node) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// catchUp synchronously drives the named peers to catch up. The commit tests
// preload the leader log directly instead of proposing, so replication is
// fully under the test's control (propose's background goroutines would
// replicate to every follower).
func catchUp(t *testing.T, n *Node, peers ...string) {
	t.Helper()
	for _, p := range peers {
		n.replicateToPeer(p)
	}
}

// preload sets the leader term and appends entries at that term.
func preload(n *Node, term int, count int) {
	n.mu.Lock()
	n.term = term
	for i := 0; i < count; i++ {
		n.log = append(n.log, Entry{Term: term, Cmd: storage.Command{
			Op: storage.OpPut, Key: fmt.Sprintf("k%d", i), Value: "v",
		}})
	}
	n.mu.Unlock()
}

// appendLog appends entries at the given term without touching n.term.
func appendLog(n *Node, term int, count int) {
	n.mu.Lock()
	for i := 0; i < count; i++ {
		n.log = append(n.log, Entry{Term: term, Cmd: storage.Command{
			Op: storage.OpPut, Key: fmt.Sprintf("k%d", i), Value: "v",
		}})
	}
	n.mu.Unlock()
}

func fiveFollowers() map[string]*testFollower {
	return map[string]*testFollower{"f1": {}, "f2": {}, "f3": {}, "f4": {}}
}

func TestCommitAdvancesOnMajority(t *testing.T) {
	n := commitLeader(t, fiveFollowers())
	preload(n, 1, 3)

	// 2 of 4 followers catch up: with the leader that is 3/5.
	catchUp(t, n, "f1", "f2")
	if got := commitIndex(n); got != 3 {
		t.Fatalf("expected commitIndex 3 after majority catch-up, got %d", got)
	}
}

func TestCommitStaysPutOnMinority(t *testing.T) {
	n := commitLeader(t, fiveFollowers())
	preload(n, 1, 3)

	// Only 1 follower catches up: leader + 1 = 2/5, not a majority.
	catchUp(t, n, "f1")
	if got := commitIndex(n); got != 0 {
		t.Fatalf("expected commitIndex 0 without majority, got %d", got)
	}
}

func TestCommitIndexAdvancesMonotonically(t *testing.T) {
	n := commitLeader(t, fiveFollowers())

	preload(n, 1, 1)
	catchUp(t, n, "f1", "f2")
	if got := commitIndex(n); got != 1 {
		t.Fatalf("expected commitIndex 1, got %d", got)
	}

	appendLog(n, 1, 2)
	catchUp(t, n, "f1", "f2")
	if got := commitIndex(n); got != 3 {
		t.Fatalf("expected commitIndex 3, got %d", got)
	}

	// A minority catch-up on a later entry must not regress commitIndex.
	appendLog(n, 1, 1)
	catchUp(t, n, "f1")
	if got := commitIndex(n); got != 3 {
		t.Fatalf("expected commitIndex to stay 3, got %d", got)
	}
}

func TestCommitTwoNodesRequiresBoth(t *testing.T) {
	// Even cluster size: a 2-node cluster needs both nodes to commit.
	followers := map[string]*testFollower{"f1": {}}
	n := commitLeader(t, followers)
	preload(n, 1, 3)

	catchUp(t, n, "f1")
	if got := commitIndex(n); got != 3 {
		t.Fatalf("expected commitIndex 3 when both of 2 nodes agree, got %d", got)
	}

	// Only the leader has the entries: majority is 2 of 2, so nothing commits.
	followers2 := map[string]*testFollower{"f1": {}}
	n2 := commitLeader(t, followers2)
	preload(n2, 1, 3)
	if got := commitIndex(n2); got != 0 {
		t.Fatalf("expected commitIndex 0 with only 1 of 2 nodes, got %d", got)
	}
}

func TestCommitDoesNotLeapOverOlderTermEntries(t *testing.T) {
	n := commitLeader(t, fiveFollowers())

	// Log holds only term-1 entries, but the leader's current term is 2.
	// Without a current-term entry committed, commitIndex must not advance.
	n.mu.Lock()
	n.term = 2
	for i := 0; i < 3; i++ {
		n.log = append(n.log, Entry{Term: 1, Cmd: storage.Command{
			Op: storage.OpPut, Key: fmt.Sprintf("k%d", i), Value: "v",
		}})
	}
	n.mu.Unlock()

	catchUp(t, n, "f1", "f2", "f3", "f4")
	if got := commitIndex(n); got != 0 {
		t.Fatalf("expected commitIndex 0 for term-1 entries at term 2, got %d", got)
	}

	// A current-term entry behind them lets the whole prefix commit.
	if err := n.Put("new", "v"); err != nil {
		t.Fatal(err)
	}
	catchUp(t, n, "f1", "f2")
	if got := commitIndex(n); got != 4 {
		t.Fatalf("expected commitIndex 4 after current-term entry, got %d", got)
	}
}