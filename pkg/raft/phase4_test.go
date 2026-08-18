package raft

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Raj-Jai/kv-store/pkg/storage"
)

func TestSingleNodeIncrCasExpire(t *testing.T) {
	store := storage.NewFakeEngine()
	n := NewNode("solo", nil, FakeTransport{}, store)

	if v, err := n.Incr("n"); err != nil || v != 1 {
		t.Fatalf("Incr = %d, %v; want 1", v, err)
	}
	if v, err := n.Incr("n"); err != nil || v != 2 {
		t.Fatalf("Incr = %d, %v; want 2", v, err)
	}

	ok, err := n.CAS("n", "2", "9")
	if err != nil || !ok {
		t.Fatalf("CAS(match) = %v, %v; want true", ok, err)
	}
	ok, err = n.CAS("n", "2", "0")
	if err != nil || ok {
		t.Fatalf("CAS(mismatch) = %v, %v; want false", ok, err)
	}
	if got, _ := n.Get("n"); got != "9" {
		t.Fatalf("Get = %q, want 9", got)
	}

	if err := n.Expire("n", time.Now().Add(time.Hour).UnixNano()); err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if got, _ := n.Get("n"); got != "9" {
		t.Fatalf("Get after Expire = %q, want 9", got)
	}
	if err := n.Expire("missing", time.Now().UnixNano()); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Expire(missing) = %v, want ErrNotFound", err)
	}
}

func TestSingleNodeScan(t *testing.T) {
	store := storage.NewFakeEngine()
	n := NewNode("solo", nil, FakeTransport{}, store)

	n.Put("a", "1")
	n.Put("b", "2")
	items, _, err := n.Scan("", 10, "a*")
	if err != nil || len(items) != 1 || items[0].Key != "a" {
		t.Fatalf("Scan = %+v, %v", items, err)
	}
}

func TestClusterIncrReplicatesAndReturnsValue(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitLeader(5 * time.Second)

	if v, err := leader.Incr("n"); err != nil || v != 1 {
		t.Fatalf("Incr = %d, %v; want 1", v, err)
	}
	if v, err := leader.Incr("n"); err != nil || v != 2 {
		t.Fatalf("Incr = %d, %v; want 2", v, err)
	}
	c.waitStore("n", "2", 5*time.Second)
}

func TestClusterCasResult(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitLeader(5 * time.Second)

	if err := leader.Put("k", "a"); err != nil {
		t.Fatal(err)
	}
	c.waitStore("k", "a", 5*time.Second)

	ok, err := leader.CAS("k", "a", "b")
	if err != nil || !ok {
		t.Fatalf("CAS(match) = %v, %v; want true", ok, err)
	}
	c.waitStore("k", "b", 5*time.Second)

	ok, err = leader.CAS("k", "a", "c")
	if err != nil || ok {
		t.Fatalf("CAS(stale) = %v, %v; want false", ok, err)
	}
	if _, err := leader.CAS("missing", "a", "b"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("CAS(missing) = %v, want ErrNotFound", err)
	}
}

func TestClusterExpireReplicates(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitLeader(5 * time.Second)

	if err := leader.Put("k", "v"); err != nil {
		t.Fatal(err)
	}
	c.waitStore("k", "v", 5*time.Second)

	if err := leader.Expire("k", time.Now().Add(time.Hour).UnixNano()); err != nil {
		t.Fatalf("Expire: %v", err)
	}
	c.waitStore("k", "v", 5*time.Second) // still live under the far-future TTL
}

func TestApplyRecordsResult(t *testing.T) {
	n := newTestNode([]Entry{
		{Term: 1, Cmd: storage.Command{Op: storage.OpIncr, Key: "n"}},
	})
	n.mu.Lock()
	n.commitIndex = 1
	n.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr := n.StartApply(ctx)
	defer n.Stop()

	waitApplied(t, tr, 1)

	res, err := tr.Result(1)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := res.(int64); !ok || v != 1 {
		t.Fatalf("Result = %v (%T), want int64(1)", res, res)
	}
}

func TestFollowerIncrReturnsNotLeader(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitLeader(5 * time.Second)

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

	_, err := follower.Incr("x")
	var nle *storage.NotLeaderError
	if !errors.As(err, &nle) {
		t.Fatalf("follower Incr = %v, want NotLeaderError", err)
	}
}