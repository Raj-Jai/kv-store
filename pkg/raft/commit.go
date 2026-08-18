package raft

import "sort"

// Commit-index advancement — Developer A (M1.4). The leader advances
// commitIndex once a majority of matchIndexes reach a log index, committing
// only entries from the current term. The apply loop that drains
// commitIndex→lastApplied into the store is Developer B's deliverable.

// maybeCommit advances commitIndex to the highest log index replicated on a
// majority, restricted to entries from the leader's current term. Callers
// must hold n.mu.
func (n *Node) maybeCommit() {
	if n.role != RoleLeader {
		return
	}
	st := n.replState()

	match := make([]int, 0, len(n.peers)+1)
	match = append(match, n.lastLogIndex()) // the leader has its own log
	for _, peer := range n.peers {
		match = append(match, st.matchIndex[peer])
	}
	sort.Ints(match)

	// The highest index replicated on a majority is the value at position
	// N-majority (0-indexed) in ascending order: that many nodes lag behind
	// it, so exactly majority nodes have reached it.
	threshold := match[len(match)-majority(n.peers)]
	if threshold <= n.commitIndex {
		return
	}
	// Raft commits older-term entries only via a committed current-term
	// entry, so never leap past a non-current term.
	if threshold > 0 {
		if e, ok := n.entryAt(threshold); !ok || e.Term != n.term {
			return
		}
	}
	n.commitIndex = threshold
}
