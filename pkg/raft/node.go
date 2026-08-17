package raft

import "log"

// Inbound RPC handlers — Developer B (M1.1/M1.3), with durability hooks from
// Developer A (M1.5). These run on every node and implement the receiving
// side of the contract: vote-granting, term step-down, follower log matching
// and truncation, commit-index advance from the leader, and snapshot
// installation.

// RaftHandler is the inbound side of the Transport seam. A node's handlers
// are registered with an in-memory or HTTP transport so peers can reach it.
type RaftHandler interface {
	HandleRequestVote(req VoteRequest) VoteResponse
	HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse
	HandleInstallSnapshot(req InstallSnapshotRequest) InstallSnapshotResponse
}

// resetElectionTimer resets the election timer on a valid leader heartbeat or
// a granted vote so this node does not start a competing election. It is a
// non-blocking signal; the Loop selects on the channel.
func (n *Node) resetElectionTimer() {
	select {
	case n.resetElection <- struct{}{}:
	default:
	}
}

// HandleRequestVote implements the receiving side of RequestVote:
//   - a request from a lower term is rejected (and the responder's own term is
//     returned so the caller steps down);
//   - a request from a higher term steps this node down to follower;
//   - the vote is granted only when this node has not already voted in the
//     term and the candidate's log is at least as fresh as our own.
//
// A granted vote is persisted before the response is returned, so a crash
// between the decision and the response cannot produce a second vote in the
// same term. Granting a vote also resets the election timer so a likely
// leader is not disrupted by a premature election on this node.
func (n *Node) HandleRequestVote(req VoteRequest) VoteResponse {
	n.mu.Lock()
	if req.Term < n.term {
		resp := VoteResponse{Term: n.term, VoteGranted: false}
		n.mu.Unlock()
		return resp
	}
	if req.Term > n.term {
		n.becomeFollower(req.Term, nil)
	}

	notVotedYet := n.votedFor == nil || *n.votedFor == req.CandidateID
	logFresh := req.LastLogTerm > n.lastLogTerm() ||
		(req.LastLogTerm == n.lastLogTerm() && req.LastLogIndex >= n.lastLogIndex())

	if notVotedYet && logFresh {
		n.votedFor = &req.CandidateID
		n.dirty = true
		n.resetElectionTimer()
		resp := VoteResponse{Term: n.term, VoteGranted: true}
		n.mu.Unlock()
		if err := n.persist(); err != nil {
			log.Printf("raft: persist vote failed: %v", err)
		}
		return resp
	}
	resp := VoteResponse{Term: n.term, VoteGranted: false}
	n.mu.Unlock()
	return resp
}

// HandleAppendEntries implements the receiving side of AppendEntries:
//   - a request from a lower term is rejected;
//   - any request at or above our term makes us a follower of that leader and
//     resets the election timer;
//   - the leader's prevLogIndex/prevLogTerm must match our log, otherwise the
//     request is rejected so the leader can rewind;
//   - on match we truncate any divergent or stale suffix and append the
//     leader's entries, then advance commitIndex up to leaderCommit.
//
// Any log change is persisted before the success response is returned.
func (n *Node) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	if req.Term < n.term {
		resp := AppendEntriesResponse{Term: n.term, Success: false}
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

	if !n.checkPrevLog(req.PrevLogIndex, req.PrevLogTerm) {
		resp := AppendEntriesResponse{Term: n.term, Success: false}
		n.mu.Unlock()
		return resp
	}

	if n.mergeEntries(req) {
		n.dirty = true
	}
	if req.LeaderCommit > n.commitIndex {
		if last := n.lastLogIndex(); req.LeaderCommit < last {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = last
		}
		n.dirty = true
	}

	resp := AppendEntriesResponse{Term: n.term, Success: true}
	n.mu.Unlock()
	if err := n.persist(); err != nil {
		log.Printf("raft: persist log append failed: %v", err)
	}
	return resp
}