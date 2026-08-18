package raft

// Follower-side log handling — Developer B (M1.3), made compaction-aware by
// Developer A (M1.6). Raft indices are implicit and offset from the
// compaction base lastIncludedIndex; with base 0 the behavior is identical to
// the original index i → slice i-1 mapping.

// checkPrevLog reports whether the follower's log contains an entry at
// prevLogIndex with term prevLogTerm. A request whose prefix is missing or
// mismatched is rejected so the leader can rewind nextIndex.
func (n *Node) checkPrevLog(prevLogIndex, prevLogTerm int) bool {
	if prevLogIndex > n.lastLogIndex() {
		return false
	}
	if prevLogIndex > 0 && n.logTermAt(prevLogIndex) != prevLogTerm {
		return false
	}
	return true
}

// mergeEntries reconciles the follower's log with an accepted AppendEntries
// and reports whether the log changed: any suffix beyond what the leader sent
// is dropped (stale, uncommitted entries from a previous term), conflicting
// entries at the same index are overwritten, and missing entries are
// appended. The result always shares a prefix with the leader's log up to
// prevLogIndex+len(entries).
func (n *Node) mergeEntries(req AppendEntriesRequest) bool {
	end := req.PrevLogIndex + len(req.Entries)
	changed := false
	if end < n.lastLogIndex() {
		keep := end - n.lastIncludedIndex
		if keep < 0 {
			keep = 0
		}
		n.log = n.log[:keep]
		changed = true
	}
	for i, e := range req.Entries {
		idx := req.PrevLogIndex + 1 + i
		if idx <= n.lastIncludedIndex {
			// Already covered by the compaction base; ignore.
			continue
		}
		if idx <= n.lastLogIndex() {
			if n.logTermAt(idx) == e.Term {
				continue
			}
			n.log = n.log[:idx-n.lastIncludedIndex-1]
			changed = true
		}
		n.log = append(n.log, req.Entries[i:]...)
		return true
	}
	return changed
}
