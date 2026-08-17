package raft

import (
	"sync"
	"testing"
	"time"

	"github.com/Raj-Jai/kv-store/pkg/storage"
)

// Leader-hostile cross tests — Developer B grades Developer A's leader-side
// replication (M1.3) against a slow, flapping, or rewound follower. A never
// grades its own leader here.

// hostileFollower stands in for a misbehaving follower.
type hostileFollower struct {
	mu        sync.Mutex
	log       []Entry
	delay     time.Duration
	flap      bool // reject every other AppendEntries
	flapCount int
	reverts   int // number of accepts to revert the log to a shorter prefix
}

func (f *hostileFollower) handle(req AppendEntriesRequest) AppendEntriesResponse {
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.flap {
		f.flapCount++
		if f.flapCount%2 == 0 {
			return AppendEntriesResponse{Term: req.Term, Success: false}
		}
	}
	if f.reverts > 0 {
		f.reverts--
		if len(f.log) > 2 {
			f.log = f.log[:2]
		}
	}
	if req.PrevLogIndex > len(f.log) {
		return AppendEntriesResponse{Term: req.Term, Success: false}
	}
	if req.PrevLogIndex > 0 && f.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
		return AppendEntriesResponse{Term: req.Term, Success: false}
	}
	f.log = append(f.log[:req.PrevLogIndex], req.Entries...)
	return AppendEntriesResponse{Term: req.Term, Success: true}
}

func (f *hostileFollower) snapshot() []Entry {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Entry(nil), f.log...)
}

type hostileTransport struct {
	follower *hostileFollower
}

func (t *hostileTransport) RequestVote(_ string, req VoteRequest) (VoteResponse, error) {
	return VoteResponse{Term: req.Term, VoteGranted: true}, nil
}

func (t *hostileTransport) AppendEntries(_ string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	return t.follower.handle(req), nil
}

func runHostileConvergence(t *testing.T, f *hostileFollower, want int) {
	t.Helper()
	trans := &hostileTransport{follower: f}
	n := NewNode("leader", []string{"f1"}, trans, storage.NewFakeEngine())
	n.mu.Lock()
	n.becomeLeader()
	n.mu.Unlock()
	preloadLeaderLog(n, 2, want)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		n.sendHeartbeats()
		if len(f.snapshot()) == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("leader never converged follower (len=%d, want %d)", len(f.snapshot()), want)
}

func TestLeaderSlowFollowerConverges(t *testing.T) {
	runHostileConvergence(t, &hostileFollower{delay: 20 * time.Millisecond}, 5)
}

func TestLeaderFlappingFollowerConverges(t *testing.T) {
	runHostileConvergence(t, &hostileFollower{flap: true}, 5)
}

func TestLeaderRewoundFollowerConverges(t *testing.T) {
	runHostileConvergence(t, &hostileFollower{reverts: 3}, 5)
}
