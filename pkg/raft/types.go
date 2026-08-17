// Package raft implements the Raft consensus protocol for the KV store.
//
// The package is split by direction of travel:
//   - types.go  (this file) — shared contract: state, messages, transitions
//   - transport.go          — the Transport seam
//   - election.go           — outbound election engine (Developer A)
//   - node.go               — inbound RPC handlers (Developer B)
//
// Contract changes land only by agreement, in their own PR.
package raft

import (
	"sync"
	"time"

	"github.com/Raj-Jai/kv-store/pkg/storage"
)

const (
	electionTimeoutMin = 500 * time.Millisecond
	electionTimeoutMax = 1000 * time.Millisecond
	heartbeatInterval  = 100 * time.Millisecond
)

// Role is the raft role of a node.
type Role int

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader
)

// Entry is one entry in the replicated log. Position is implicit: entry i
// lives at log index i+1.
type Entry struct {
	Term int
	Cmd  storage.Command
}

type VoteRequest struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

type VoteResponse struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         int
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesResponse struct {
	Term    int
	Success bool
}

// Node is the shared raft state. Both the outbound engine (election.go) and
// the inbound handlers (node.go) operate on it under n.mu.
type Node struct {
	mu sync.Mutex

	id       string
	peers    []string
	role     Role
	term     int
	votedFor *string
	leaderID *string

	log         []Entry
	commitIndex int
	lastApplied int

	lastQuorumAck time.Time

	transport Transport
	store     storage.Engine

	resetElection chan struct{}
	stop          chan struct{}
}

// NewNode creates a raft node. With no peers it starts as leader (single
// node mode); otherwise it starts as follower.
func NewNode(id string, peers []string, transport Transport, store storage.Engine) *Node {
	n := &Node{
		id:            id,
		peers:         peers,
		role:          RoleFollower,
		transport:     transport,
		store:         store,
		resetElection: make(chan struct{}, 1),
		stop:          make(chan struct{}),
	}
	if len(peers) == 0 {
		n.role = RoleLeader
		n.leaderID = &n.id
	}
	return n
}

// Stop signals the election loop to exit.
func (n *Node) Stop() {
	select {
	case <-n.stop:
	default:
		close(n.stop)
	}
}

// LeaderID returns the known leader's address and whether this node is the
// leader.
func (n *Node) LeaderID() (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.leaderID == nil {
		return "", false
	}
	return *n.leaderID, n.role == RoleLeader
}

// lastLogIndex returns the index of the last log entry (0 when empty).
func (n *Node) lastLogIndex() int {
	return len(n.log)
}

// lastLogTerm returns the term of the last log entry (0 when empty).
func (n *Node) lastLogTerm() int {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

// becomeCandidate starts a new election term and votes for itself.
// Callers must hold n.mu.
func (n *Node) becomeCandidate() {
	n.role = RoleCandidate
	n.term++
	n.votedFor = &n.id
	n.leaderID = nil
}

// becomeLeader marks the node as the leader for its current term.
// Callers must hold n.mu.
func (n *Node) becomeLeader() {
	n.role = RoleLeader
	n.leaderID = &n.id
}

// becomeFollower sets the node as follower at the given term, clearing any
// vote. Callers must hold n.mu.
func (n *Node) becomeFollower(term int, leaderID *string) {
	n.role = RoleFollower
	n.term = term
	n.votedFor = nil
	n.leaderID = leaderID
}