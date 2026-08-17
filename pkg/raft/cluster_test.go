package raft

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Raj-Jai/kv-store/pkg/storage"
)

// cluster is a 5-node in-process raft cluster on the MemTransport: the
// deterministic harness for the Phase 1 gates.
type cluster struct {
	t      *testing.T
	nodes  []*Node
	trans  *MemTransport
	ctx    context.Context
	cancel context.CancelFunc
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	trans := NewMemTransport()
	ctx, cancel := context.WithCancel(context.Background())

	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("n%d", i)
	}

	nodes := make([]*Node, n)
	for i := 0; i < n; i++ {
		var peers []string
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, ids[j])
			}
		}
		node := NewNode(ids[i], peers, trans, storage.NewFakeEngine())
		trans.Register(ids[i], node)
		nodes[i] = node
	}

	c := &cluster{t: t, nodes: nodes, trans: trans, ctx: ctx, cancel: cancel}
	for _, node := range nodes {
		go node.Loop(ctx)
		node.StartApply(ctx)
	}
	t.Cleanup(func() {
		cancel()
		for _, node := range nodes {
			node.Stop()
		}
	})
	return c
}

func (c *cluster) waitLeader(timeout time.Duration) *Node {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, node := range c.nodes {
			if node.IsLeader() {
				return node
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no leader elected within %s", timeout)
	return nil
}

func (c *cluster) kill(node *Node) {
	node.Stop()
	c.trans.Unregister(node.id)
}

// assertLeadersPerTerm samples the cluster for d and fails if any term ever
// reports two distinct leaders.
func (c *cluster) assertLeadersPerTerm(d time.Duration) {
	c.t.Helper()
	leaders := map[int]string{}
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		for _, node := range c.nodes {
			node.mu.Lock()
			if node.role == RoleLeader {
				prev, seen := leaders[node.term]
				if seen && prev != node.id {
					c.t.Errorf("two leaders in term %d: %s and %s", node.term, prev, node.id)
				}
				leaders[node.term] = node.id
			}
			node.mu.Unlock()
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitStore polls every node until its state machine holds key=value.
func (c *cluster) waitStore(key, value string, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for _, node := range c.nodes {
			got, err := node.Get(key)
			if err != nil || got != value {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("value %s=%s never replicated+applied to every node", key, value)
}

func TestClusterElectsSingleLeader(t *testing.T) {
	c := newCluster(t, 5)
	leader := c.waitLeader(5 * time.Second)

	for _, node := range c.nodes {
		if node != leader && node.IsLeader() {
			t.Fatalf("more than one leader in the cluster: %s and %s", leader.id, node.id)
		}
	}

	// The leadership must stay stable under the continuous invariant.
	c.assertLeadersPerTerm(1500 * time.Millisecond)
}

func TestClusterAtMostOneLeaderPerTerm(t *testing.T) {
	// The invariant is asserted from the very start, through the initial
	// election and any subsequent re-elections.
	c := newCluster(t, 5)
	c.assertLeadersPerTerm(2500 * time.Millisecond)
	c.waitLeader(3 * time.Second)
}

func TestClusterLeaderReplacement(t *testing.T) {
	c := newCluster(t, 5)
	leader := c.waitLeader(5 * time.Second)

	// Simulate kill -9: stop the loop and remove it from the transport so
	// peers stop hearing from it.
	c.kill(leader)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, node := range c.nodes {
			if node != leader && node.IsLeader() {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no new leader elected after killing the leader")
}

func TestClusterWriteReplicatesAndApplies(t *testing.T) {
	c := newCluster(t, 5)
	leader := c.waitLeader(5 * time.Second)

	if err := leader.Put("k", "v"); err != nil {
		t.Fatalf("leader write failed: %v", err)
	}
	c.waitStore("k", "v", 5*time.Second)
}

func TestClusterDeleteReplicatesAndApplies(t *testing.T) {
	c := newCluster(t, 5)
	leader := c.waitLeader(5 * time.Second)

	if err := leader.Put("k", "v"); err != nil {
		t.Fatal(err)
	}
	c.waitStore("k", "v", 5*time.Second)

	if err := leader.Delete("k"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, node := range c.nodes {
			if _, err := node.Get("k"); err == nil {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("delete never applied on every node")
}

func TestClusterFollowerWriteReturnsNotLeaderError(t *testing.T) {
	c := newCluster(t, 5)
	leader := c.waitLeader(5 * time.Second)

	// Wait until a follower has recorded the leader, then confirm writes on it
	// fail with NotLeaderError carrying the leader's address.
	var follower *Node
	for _, node := range c.nodes {
		if node != leader {
			follower = node
			break
		}
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if follower.Leader() == leader.id {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	err := follower.Put("x", "y")
	var nle *storage.NotLeaderError
	if !errors.As(err, &nle) {
		t.Fatalf("follower write = %v, want NotLeaderError", err)
	}
	if nle.LeaderAddr != leader.id {
		t.Fatalf("NotLeaderError.LeaderAddr = %q, want %q", nle.LeaderAddr, leader.id)
	}
}
