package raft

// Follower-side log handling — Developer B (M1.3). Log positions are implicit
// (entry i lives at index i+1), matching the leader-side contract.

// checkPrevLog reports whether the follower's log contains an entry at
// prevLogIndex with term prevLogTerm. A request whose prefix is missing or
// mismatched is rejected so the leader can rewind nextIndex.
func (n *Node) checkPrevLog(prevLogIndex, prevLogTerm int) bool {
	if prevLogIndex > len(n.log) {
		return false
	}
	if prevLogIndex > 0 && n.log[prevLogIndex-1].Term != prevLogTerm {
		return false
	}
	return true
}

// mergeEntries reconciles the follower's log with an accepted AppendEntries:
// any suffix beyond what the leader sent is dropped (stale, uncommitted
// entries from a previous term), conflicting entries at the same index are
// overwritten, and missing entries are appended. The result always shares a
// prefix with the leader's log up to prevLogIndex+len(entries).
func (n *Node) mergeEntries(req AppendEntriesRequest) {
	end := req.PrevLogIndex + len(req.Entries)
	if end < len(n.log) {
		n.log = n.log[:end]
	}
	for i, e := range req.Entries {
		idx := req.PrevLogIndex + 1 + i
		if idx <= len(n.log) {
			if n.log[idx-1].Term == e.Term {
				continue
			}
			n.log = n.log[:idx-1]
		}
		n.log = append(n.log, req.Entries[i:]...)
		return
	}
}
