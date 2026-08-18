package main

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/Raj-Jai/kv-store/pkg/raft"
	"github.com/Raj-Jai/kv-store/pkg/storage"
)

// Recovery matrix — Developer B (M1.5/M1.6). The same 5-node in-process
// harness as pkg/raft, but every node runs on real storage: a DiskStore for
// the state machine, a fileRaftStore for term/vote/log, and the snapshot
// bridge. These tests prove acked writes survive restarts and that a lagging
// follower is resynced through an installed snapshot.
const clusterSize = 5

type diskNode struct {
	dir    string
	id     string
	node   *raft.Node
	store  *storage.DiskStore
	cancel context.CancelFunc
}

type diskCluster struct {
	t     *testing.T
	trans *raft.MemTransport
	nodes []*diskNode // currently alive nodes
	ids   []string
	key   []byte // at-rest encryption key; nil keeps nodes in plaintext mode
}

func newDiskCluster(t *testing.T) *diskCluster {
	t.Helper()
	c := &diskCluster{t: t, trans: raft.NewMemTransport()}
	for i := 0; i < clusterSize; i++ {
		c.ids = append(c.ids, fmt.Sprintf("c%d", i))
	}
	for _, id := range c.ids {
		c.startNode(t.TempDir(), id)
	}
	t.Cleanup(func() {
		for _, d := range c.nodes {
			d.node.Stop()
			d.store.Close()
		}
	})
	return c
}

// newEncryptedDiskCluster is newDiskCluster with at-rest encryption on every
// node: the DiskStore WAL/snapshots and the raft log are all sealed with the
// same key, mirroring cmd/server wiring.
func newEncryptedDiskCluster(t *testing.T, key []byte) *diskCluster {
	t.Helper()
	c := &diskCluster{t: t, trans: raft.NewMemTransport(), key: key}
	for i := 0; i < clusterSize; i++ {
		c.ids = append(c.ids, fmt.Sprintf("e%d", i))
	}
	for _, id := range c.ids {
		c.startNode(t.TempDir(), id)
	}
	t.Cleanup(func() {
		for _, d := range c.nodes {
			d.node.Stop()
			d.store.Close()
		}
	})
	return c
}

// startNode boots a node on real storage in dir and registers it with the
// shared transport, appending it to the alive set.
func (c *diskCluster) startNode(dir, id string) *diskNode {
	c.t.Helper()
	var peers []string
	for _, p := range c.ids {
		if p != id {
			peers = append(peers, p)
		}
	}

	store, err := storage.OpenDiskStore(filepath.Join(dir, "data"))
	if c.key != nil {
		store, err = storage.OpenDiskStoreWithKey(filepath.Join(dir, "data"), c.key)
	}
	if err != nil {
		c.t.Fatalf("open store for %s: %v", id, err)
	}
	node := raft.NewNode(id, peers, c.trans, store)
	if c.key == nil {
		if err := node.SetRaftStore(raft.NewFileRaftStore(filepath.Join(dir, "raft.json"))); err != nil {
			c.t.Fatalf("set raft store for %s: %v", id, err)
		}
	} else {
		enc, err := storage.NewAtRestCipher(c.key)
		if err != nil {
			c.t.Fatalf("cipher for %s: %v", id, err)
		}
		if err := node.SetRaftStore(raft.NewEncryptedFileRaftStore(filepath.Join(dir, "raft.json"), enc)); err != nil {
			c.t.Fatalf("set raft store for %s: %v", id, err)
		}
	}
	bridge := &snapshotBridge{node: node, store: store}
	node.SetSnapshotter(bridge)
	node.SetSnapshotSink(bridge)
	c.trans.Register(id, node)

	ctx, cancel := context.WithCancel(context.Background())
	go node.Loop(ctx)
	node.StartApply(ctx)

	d := &diskNode{dir: dir, id: id, node: node, store: store, cancel: cancel}
	c.nodes = append(c.nodes, d)
	return d
}

// stop shuts a node down and removes it from the alive set, releasing its
// files so the same directory can be reused by a restarted instance.
func (c *diskCluster) stop(d *diskNode) {
	d.cancel()
	d.node.Stop()
	c.trans.Unregister(d.id)
	if err := d.store.Close(); err != nil {
		c.t.Errorf("close store for %s: %v", d.id, err)
	}
	for i, n := range c.nodes {
		if n == d {
			c.nodes = append(c.nodes[:i], c.nodes[i+1:]...)
			break
		}
	}
}

func (c *diskCluster) waitLeader(timeout time.Duration) *diskNode {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, d := range c.nodes {
			if d.node.IsLeader() {
				return d
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no leader elected within %s", timeout)
	return nil
}

func (c *diskCluster) waitStore(key, value string, timeout time.Duration) {
	c.t.Helper()
	if c.storeHas(key, value, timeout) {
		return
	}
	c.t.Fatalf("value %s=%s never present on every node", key, value)
}

// storeHas reports whether every alive node holds key=value within timeout.
func (c *diskCluster) storeHas(key, value string, timeout time.Duration) bool {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for _, d := range c.nodes {
			if got, err := d.node.Get(key); err != nil || got != value {
				all = false
				break
			}
		}
		if all {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func (c *diskCluster) waitNodeStore(d *diskNode, key, value string, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got, err := d.node.Get(key); err == nil && got == value {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("node %s never reached %s=%s", d.id, key, value)
}

// writeAcked proposes a write on the current leader and waits until it is
// applied on every alive node. A proposed-but-uncommitted entry can be
// stranded by a re-election (Raft commits older-term entries only via a later
// current-term write), so the write is re-proposed until it durably lands —
// the same retry a real client performs. Writes are idempotent.
func (c *diskCluster) writeAcked(key, value string) {
	c.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		leader := c.waitLeader(5 * time.Second)
		if err := leader.node.Put(key, value); err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if c.storeHas(key, value, 5*time.Second) {
			return
		}
		// Stranded: re-propose.
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("write %s=%s never acked and applied on every node", key, value)
}

func TestAckedWriteSurvivesLeaderRestart(t *testing.T) {
	c := newDiskCluster(t)
	c.waitLeader(5 * time.Second)

	c.writeAcked("k", "v")

	leader := c.waitLeader(5 * time.Second)
	c.stop(leader)
	c.waitLeader(5 * time.Second)

	restarted := c.startNode(leader.dir, leader.id)
	c.waitNodeStore(restarted, "k", "v", 5*time.Second)

	// The rejoin must actually rejoin: a write issued after the restart is
	// eventually applied on the restarted node too.
	c.writeAcked("k2", "v2")
	c.waitNodeStore(restarted, "k2", "v2", 5*time.Second)
}

func TestAckedWriteSurvivesFollowerRestart(t *testing.T) {
	c := newDiskCluster(t)
	leader := c.waitLeader(5 * time.Second)

	var follower *diskNode
	for _, d := range c.nodes {
		if d != leader {
			follower = d
			break
		}
	}

	c.writeAcked("k", "v")

	c.stop(follower)
	restarted := c.startNode(follower.dir, follower.id)
	c.waitNodeStore(restarted, "k", "v", 5*time.Second)

	// Writes after the rejoin reach the restarted follower too.
	c.writeAcked("k2", "v2")
	c.waitNodeStore(restarted, "k2", "v2", 5*time.Second)
}

func TestFullClusterRestartPreservesAckedWrites(t *testing.T) {
	c := newDiskCluster(t)
	c.waitLeader(5 * time.Second)

	for i := 0; i < 3; i++ {
		c.writeAcked(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}

	dirs := make(map[string]string, len(c.nodes))
	for _, d := range c.nodes {
		dirs[d.id] = d.dir
		c.stop(d)
	}

	for _, id := range c.ids {
		c.startNode(dirs[id], id)
	}

	c.waitLeader(10 * time.Second)
	for i := 0; i < 3; i++ {
		c.waitStore(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), 10*time.Second)
	}
}

func TestLaggingFollowerResyncedViaSnapshot(t *testing.T) {
	c := newDiskCluster(t)
	leader := c.waitLeader(5 * time.Second)

	var straggler *diskNode
	for _, d := range c.nodes {
		if d != leader {
			straggler = d
			break
		}
	}
	// Kill the follower BEFORE the writes: its raft log and state machine are
	// far behind what the cluster commits.
	c.stop(straggler)

	for i := 0; i < 20; i++ {
		c.writeAcked(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}

	// Compact the current leader's log: the straggler's next needed entry is
	// gone, so it can only be resynced via InstallSnapshot.
	leader = c.waitLeader(5 * time.Second)
	idx := leader.node.ApplyIndex()
	if idx < 20 {
		t.Fatalf("leader applied %d entries, want >= 20", idx)
	}
	term := leader.node.LogTerm(idx)
	if err := leader.store.Compact(); err != nil {
		t.Fatalf("storage compaction failed: %v", err)
	}
	leader.node.CompactLog(idx, term)
	if err := leader.node.Flush(); err != nil {
		t.Fatalf("raft base flush failed: %v", err)
	}
	if base := leader.node.SnapshotBase(); base != idx {
		t.Fatalf("leader compaction base = %d, want %d", base, idx)
	}

	// The straggler rejoins with an empty log; a new write forces the leader
	// to replicate to it, which must go through the snapshot path.
	restarted := c.startNode(straggler.dir, straggler.id)
	c.writeAcked("after", "snap")

	for i := 0; i < 20; i++ {
		c.waitNodeStore(restarted, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), 15*time.Second)
	}
	c.waitNodeStore(restarted, "after", "snap", 15*time.Second)

	if base := restarted.node.SnapshotBase(); base < idx {
		t.Fatalf("rejoin adopted base %d, want >= %d", base, idx)
	}
}

func TestCompactionBaseSurvivesRestart(t *testing.T) {
	c := newDiskCluster(t)
	leader := c.waitLeader(5 * time.Second)

	for i := 0; i < 5; i++ {
		c.writeAcked(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	leader = c.waitLeader(5 * time.Second)

	idx := leader.node.ApplyIndex()
	term := leader.node.LogTerm(idx)
	if err := leader.store.Compact(); err != nil {
		t.Fatalf("storage compaction failed: %v", err)
	}
	leader.node.CompactLog(idx, term)
	if err := leader.node.Flush(); err != nil {
		t.Fatalf("raft base flush failed: %v", err)
	}

	c.stop(leader)
	restarted := c.startNode(leader.dir, leader.id)

	for i := 0; i < 5; i++ {
		c.waitNodeStore(restarted, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), 5*time.Second)
	}
	if base := restarted.node.SnapshotBase(); base != idx {
		t.Fatalf("compaction base not durable across restart: got %d, want %d", base, idx)
	}
}

// TestEncryptedClusterNoPlaintextOnDisk proves the at-rest guarantee end to
// end: with a key configured, an acked write, its raft log entry, and any
// snapshot must never appear in plaintext in any node's data directory — and
// the acked write must survive a leader restart on the same key.
func TestEncryptedClusterNoPlaintextOnDisk(t *testing.T) {
	key := bytes.Repeat([]byte{0x2b}, storage.AtRestKeySize)
	c := newEncryptedDiskCluster(t, key)
	c.waitLeader(5 * time.Second)

	c.writeAcked("secret", "classified-73")
	c.waitStore("secret", "classified-73", 5*time.Second)

	for _, d := range c.nodes {
		hits, err := findStrings(d.dir, "classified-73")
		if err != nil {
			t.Fatalf("scan %s: %v", d.id, err)
		}
		if len(hits) > 0 {
			t.Fatalf("node %s leaked plaintext value in %v", d.id, hits)
		}
	}

	leader := c.waitLeader(5 * time.Second)
	c.stop(leader)
	restarted := c.startNode(leader.dir, leader.id)
	c.waitNodeStore(restarted, "secret", "classified-73", 15*time.Second)

	hits, err := findStrings(leader.dir, "classified-73")
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) > 0 {
		t.Fatalf("restarted node leaked plaintext value in %v", hits)
	}
}
