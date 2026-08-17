package raft

import (
	"errors"
	"log"

	"github.com/Raj-Jai/kv-store/pkg/storage"
)

// Engine implementation and leader-side replication — Developer A
// (M1.2/M1.3). raft.Node wraps a storage.Engine and proposes mutations
// through consensus, so Developer B's HTTP handlers keep talking to an
// Engine unchanged.

// maxReplBackoff caps the exponential rewind step used when a follower keeps
// rejecting a log prefix.
const maxReplBackoff = 64

// replicationState is the per-follower leader bookkeeping (M1.3). It lives
// off the shared Node struct so the contract file stays minimal.
type replicationState struct {
	nextIndex  map[string]int // next log index to send to each peer
	matchIndex map[string]int // highest log index known replicated to each peer
	inFlight   map[string]bool
	backoff    map[string]int // exponential rewind step on repeated rejection
}

// replState lazily initializes the leader replication state. Callers must
// hold n.mu.
func (n *Node) replState() *replicationState {
	if n.repl == nil {
		n.repl = &replicationState{
			nextIndex:  make(map[string]int),
			matchIndex: make(map[string]int),
			inFlight:   make(map[string]bool),
			backoff:    make(map[string]int),
		}
		next := n.lastLogIndex() + 1
		for _, p := range n.peers {
			n.repl.nextIndex[p] = next
		}
	}
	return n.repl
}

// Leader returns the current leader's address, or "" when none is known.
func (n *Node) Leader() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.leaderID == nil {
		return ""
	}
	return *n.leaderID
}

// IsLeader reports whether this node is the current leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == RoleLeader
}

// Get serves reads from the local state machine.
func (n *Node) Get(key string) (string, error) {
	return n.store.Get(key)
}

func (n *Node) Put(key, value string) error {
	return n.propose(storage.Command{Op: storage.OpPut, Key: key, Value: value})
}

func (n *Node) Delete(key string) error {
	return n.propose(storage.Command{Op: storage.OpDelete, Key: key})
}

func (n *Node) Clear() error {
	return n.propose(storage.Command{Op: storage.OpClear})
}

func (n *Node) Close() error {
	n.Stop()
	return n.store.Close()
}

// propose appends a command to the leader's log and kicks replication to
// every peer in the background. It returns storage.NotLeaderError{LeaderAddr}
// when this node is not the leader. For a single-node cluster the entry is
// committed and applied immediately; for a multi-node cluster the leader
// returns once the entry is on its own log — commit-index advancement and the
// synchronous return-to-client land in M1.4.
func (n *Node) propose(cmd storage.Command) error {
	n.mu.Lock()
	if n.role != RoleLeader {
		addr := ""
		if n.leaderID != nil {
			addr = *n.leaderID
		}
		n.mu.Unlock()
		return &storage.NotLeaderError{LeaderAddr: addr}
	}
	n.log = append(n.log, Entry{Term: n.term, Cmd: cmd})
	n.dirty = true
	singleNode := len(n.peers) == 0
	if singleNode {
		n.commitIndex = n.lastLogIndex()
	}
	if singleNode {
		n.mu.Unlock()
		return n.applyCmd(cmd)
	}
	peers := append([]string(nil), n.peers...)
	n.mu.Unlock()

	// The leader's own copy of the entry must be durable before followers can
	// be told about it.
	if err := n.persist(); err != nil {
		log.Printf("raft: persist log entry failed: %v", err)
	}

	for _, peer := range peers {
		go n.replicateToPeer(peer)
	}
	return nil
}

// replicateToPeer sends any pending log entries (batched) to one follower as
// an AppendEntries and drives the follower to catch up: it advances
// nextIndex/matchIndex on success, rewinds with exponential backoff and
// retries immediately on rejection, and steps the leader down on a higher
// term. It returns whether the peer is acknowledged at the current term.
func (n *Node) replicateToPeer(peer string) bool {
	for {
		n.mu.Lock()
		if n.role != RoleLeader {
			n.mu.Unlock()
			return false
		}
		st := n.replState()
		if st.inFlight[peer] {
			// Another goroutine is already driving this peer to catch up.
			n.mu.Unlock()
			return false
		}
		next := st.nextIndex[peer]
		if next < 1 {
			next = 1
		}
		if next < n.firstIndex() {
			// The follower needs an entry that has been compacted into a
			// snapshot; resync it via InstallSnapshot.
			n.mu.Unlock()
			return n.installSnapshot(peer)
		}
		prevLogIndex := next - 1
		prevLogTerm := n.logTermAt(prevLogIndex)
		var entries []Entry
		if next <= n.lastLogIndex() {
			off := next - n.firstIndex()
			entries = append([]Entry(nil), n.log[off:]...)
		}
		req := AppendEntriesRequest{
			Term:         n.term,
			LeaderID:     n.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: n.commitIndex,
		}
		st.inFlight[peer] = true
		n.mu.Unlock()

		resp, err := n.transport.AppendEntries(peer, req)
		if err != nil {
			n.mu.Lock()
			st.inFlight[peer] = false
			n.mu.Unlock()
			return false
		}

		n.mu.Lock()
		st.inFlight[peer] = false

		if resp.Term > n.term {
			n.becomeFollower(resp.Term, nil)
			n.mu.Unlock()
			return false
		}
		if resp.Term < n.term {
			n.mu.Unlock()
			return false
		}
		if !resp.Success {
			step := st.backoff[peer]
			if step == 0 {
				step = 1
			}
			if st.nextIndex[peer] > step {
				st.nextIndex[peer] -= step
			} else {
				st.nextIndex[peer] = 1
			}
			if step < maxReplBackoff {
				st.backoff[peer] = step * 2
			}
			gaveUp := step >= maxReplBackoff
			n.mu.Unlock()
			if gaveUp {
				return false // retried on the next heartbeat tick
			}
			continue // re-send immediately at the rewound index
		}

		st.backoff[peer] = 0
		matched := prevLogIndex + len(entries)
		if matched > st.matchIndex[peer] {
			st.matchIndex[peer] = matched
		}
		if matched+1 > st.nextIndex[peer] {
			st.nextIndex[peer] = matched + 1
		}
		n.maybeCommit()
		n.mu.Unlock()
		return true
	}
}

// applyCmd executes a single command against the local state machine. The
// general commit-index apply loop is Developer B's M1.4 deliverable.
func (n *Node) applyCmd(cmd storage.Command) error {
	switch cmd.Op {
	case storage.OpPut:
		return n.store.Put(cmd.Key, cmd.Value)
	case storage.OpDelete:
		return n.store.Delete(cmd.Key)
	case storage.OpClear:
		return n.store.Clear()
	default:
		return errors.New("raft: unknown command op")
	}
}