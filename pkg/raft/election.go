package raft

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"
)

// electionTimeout returns a randomized timeout in [min, max). Randomized so
// concurrent candidates do not always collide.
func (n *Node) electionTimeout() time.Duration {
	spread := int(electionTimeoutMax - electionTimeoutMin)
	return electionTimeoutMin + time.Duration(rand.IntN(spread))*time.Millisecond
}

// Loop drives the election state machine: it starts elections when the
// randomized timer fires (as long as the node is not the leader) and sends
// heartbeats on a fixed interval while leader. It resets the election timer
// whenever a valid leader is heard from.
func (n *Node) Loop(ctx context.Context) {
	election := time.NewTimer(n.electionTimeout())
	defer election.Stop()

	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stop:
			return
		case <-n.resetElection:
			resetTimer(election, n.electionTimeout())
		case <-election.C:
			n.mu.Lock()
			isLeader := n.role == RoleLeader
			n.mu.Unlock()
			if !isLeader {
				n.startElection()
			}
			election.Reset(n.electionTimeout())
		case <-heartbeat.C:
			n.mu.Lock()
			if n.role != RoleLeader {
				n.mu.Unlock()
				continue
			}
			n.mu.Unlock()

			n.sendHeartbeats()

			// A leader that cannot reach a majority steps down so it stops
			// serving stale writes during a partition.
			n.mu.Lock()
			if n.role == RoleLeader && time.Since(n.lastQuorumAck) > electionTimeoutMin {
				n.becomeFollower(n.term, nil)
			}
			n.mu.Unlock()
		}
	}
}

// startElection advances the term, votes for itself, fans out RequestVote to
// every peer, and tallies the responses. A majority elects this node leader;
// a higher term anywhere steps it down.
func (n *Node) startElection() {
	n.mu.Lock()
	n.becomeCandidate()
	req := VoteRequest{
		Term:         n.term,
		CandidateID:  n.id,
		LastLogIndex: n.lastLogIndex(),
		LastLogTerm:  n.lastLogTerm(),
	}
	quorum := majority(n.peers)
	peers := append([]string(nil), n.peers...)
	n.mu.Unlock()

	results := make(chan VoteResponse, len(peers))
	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			resp, err := n.transport.RequestVote(peer, req)
			if err != nil {
				return
			}
			results <- resp
		}(peer)
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	votes := 1 // self vote
	for resp := range results {
		n.mu.Lock()
		if resp.Term > n.term {
			n.becomeFollower(resp.Term, nil)
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()

		if resp.VoteGranted {
			votes++
		}
		if votes >= quorum {
			break
		}
	}

	n.mu.Lock()
	if votes >= quorum && n.role == RoleCandidate && n.term == req.Term {
		n.becomeLeader()
	} else {
		n.becomeFollower(n.term, nil)
	}
	n.mu.Unlock()
}

// sendHeartbeats broadcasts an AppendEntries (carrying any pending log
// entries) to every peer to hold the term, replicate, and record the last
// time a majority acknowledged the leader.
func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return
	}
	peers := append([]string(nil), n.peers...)
	quorum := majority(n.peers)
	n.mu.Unlock()

	var (
		acks = 1 // self
		mu   sync.Mutex
		wg   sync.WaitGroup
	)
	for _, peer := range peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			if n.replicateToPeer(peer) {
				mu.Lock()
				acks++
				mu.Unlock()
			}
		}(peer)
	}
	wg.Wait()

	n.mu.Lock()
	if n.role == RoleLeader && acks >= quorum {
		n.lastQuorumAck = time.Now()
	}
	n.mu.Unlock()
}

// majority returns the quorum for a cluster of len(peers)+1 nodes
// (floor(N/2)+1).
func majority(peers []string) int {
	return (len(peers)+1)/2 + 1
}

// resetTimer safely restarts a timer, draining an already-fired event.
func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}