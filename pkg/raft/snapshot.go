package raft

import "log"

// Snapshot metadata and compaction — Developer A (M1.6). The leader compacts
// committed log entries once the state machine is snapshotted, remembers the
// compaction base, and resyncs followers whose next needed entry is older
// than the first retained one by sending InstallSnapshot.

// Snapshot is a compacted log prefix plus the serialized state machine at
// that point.
type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// SnapshotProvider supplies the leader's current snapshot to send to lagging
// followers. Developer B wires this to the storage-side snapshot files.
type SnapshotProvider interface {
	Snapshot() (Snapshot, error)
}

// SnapshotSink consumes an incoming snapshot's state-machine data on a
// follower. Developer B wires this to the storage-side snapshot restore.
type SnapshotSink interface {
	ApplySnapshot(data []byte) error
}

// SetSnapshotter wires the leader's snapshot provider.
func (n *Node) SetSnapshotter(p SnapshotProvider) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.snapshotter = p
}

// SetSnapshotSink wires the follower's snapshot consumer.
func (n *Node) SetSnapshotSink(s SnapshotSink) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.snapshotSink = s
}

// CompactLog trims all log entries up to and including lastIncludedIndex and
// records the compaction base. Call once the state machine has been
// snapshotted (Developer B's storage side) at that index.
func (n *Node) CompactLog(lastIncludedIndex, lastIncludedTerm int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if lastIncludedIndex <= n.lastIncludedIndex {
		return
	}
	if lastIncludedIndex >= n.lastLogIndex() {
		lastIncludedTerm = n.lastLogTerm()
		n.log = nil
	} else {
		n.log = n.log[lastIncludedIndex-n.lastIncludedIndex:]
	}
	n.lastIncludedIndex = lastIncludedIndex
	n.lastIncludedTerm = lastIncludedTerm
	if n.commitIndex < lastIncludedIndex {
		n.commitIndex = lastIncludedIndex
	}
	if n.lastApplied < lastIncludedIndex {
		n.lastApplied = lastIncludedIndex
	}
	n.dirty = true
}

// installSnapshot sends the leader's current snapshot to a follower whose
// next needed entry has been compacted away. On success the follower is
// considered caught up to the snapshot's lastIncludedIndex. Returns whether
// the follower acknowledged at the current term.
func (n *Node) installSnapshot(peer string) bool {
	n.mu.Lock()
	if n.role != RoleLeader || n.snapshotter == nil {
		n.mu.Unlock()
		return false
	}
	st := n.replState()
	if st.inFlight[peer] {
		n.mu.Unlock()
		return false
	}
	n.mu.Unlock()

	snap, err := n.snapshotter.Snapshot()
	if err != nil {
		return false
	}

	n.mu.Lock()
	st = n.replState()
	if st.inFlight[peer] {
		n.mu.Unlock()
		return false
	}
	req := InstallSnapshotRequest{
		Term:              n.term,
		LeaderID:          n.id,
		LastIncludedIndex: snap.LastIncludedIndex,
		LastIncludedTerm:  snap.LastIncludedTerm,
		Data:              snap.Data,
	}
	st.inFlight[peer] = true
	n.mu.Unlock()

	resp, err := n.transport.InstallSnapshot(peer, req)
	if err != nil {
		n.mu.Lock()
		st.inFlight[peer] = false
		n.mu.Unlock()
		return false
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	st.inFlight[peer] = false
	if resp.Term > n.term {
		n.becomeFollower(resp.Term, nil)
		return false
	}
	if resp.Term < n.term || !resp.Success {
		return false
	}
	if snap.LastIncludedIndex > st.matchIndex[peer] {
		st.matchIndex[peer] = snap.LastIncludedIndex
	}
	if snap.LastIncludedIndex+1 > st.nextIndex[peer] {
		st.nextIndex[peer] = snap.LastIncludedIndex + 1
	}
	st.backoff[peer] = 0
	n.maybeCommit()
	return true
}

// HandleInstallSnapshot implements the receiving side of InstallSnapshot: a
// lower-term request is rejected; otherwise the node becomes a follower of
// the sender and adopts the snapshot's compaction base, trimming any log
// entries already covered by it. The state-machine data is handed to the
// snapshot sink (Developer B's storage side).
func (n *Node) HandleInstallSnapshot(req InstallSnapshotRequest) InstallSnapshotResponse {
	n.mu.Lock()
	if req.Term < n.term {
		resp := InstallSnapshotResponse{Term: n.term, Success: false}
		n.mu.Unlock()
		return resp
	}
	leader := req.LeaderID
	if req.Term > n.term || n.role != RoleFollower {
		n.becomeFollower(req.Term, &leader)
	} else {
		n.leaderID = &leader
	}
	n.resetElectionTimer()

	installed := req.LastIncludedIndex > n.lastIncludedIndex
	if installed {
		drop := req.LastIncludedIndex - n.lastIncludedIndex
		if drop > len(n.log) {
			drop = len(n.log)
		}
		n.log = n.log[drop:]
		n.lastIncludedIndex = req.LastIncludedIndex
		n.lastIncludedTerm = req.LastIncludedTerm
		if n.commitIndex < req.LastIncludedIndex {
			n.commitIndex = req.LastIncludedIndex
		}
		if n.lastApplied < req.LastIncludedIndex {
			n.lastApplied = req.LastIncludedIndex
		}
		n.dirty = true
	}
	data := req.Data
	resp := InstallSnapshotResponse{Term: n.term, Success: true}
	n.mu.Unlock()

	if installed && n.snapshotSink != nil {
		if err := n.snapshotSink.ApplySnapshot(data); err != nil {
			log.Printf("raft: apply snapshot data failed: %v", err)
		}
	}
	// Persist the new compaction base AFTER the snapshot data is durable: a
	// crash in between leaves the old (lower) base with a state machine that
	// already includes the snapshot, which recovers idempotently. Persisting
	// first would let a crash discard every entry up to the base.
	if err := n.persist(); err != nil {
		log.Printf("raft: persist snapshot failed: %v", err)
	}
	return resp
}
