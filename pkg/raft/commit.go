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
	match = append(match, len(n.log)) // the leader has its own log
	for _, peer := range n.peers {
		match = append(match, st.matchIndex[peer])
	}
	sort.Ints(match)

	median := match[len(match)/2] // floor(N/2)+1 nodes replicate up to here
	if median <= n.commitIndex {
		return
	}
	// Raft commits older-term entries only via a committed current-term
	// entry, so never leap past a non-current term.
	if median > 0 && n.log[median-1].Term != n.term {
		return
	}
	n.commitIndex = median
}