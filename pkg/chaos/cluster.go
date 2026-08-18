package chaos

import (
	"context"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Raj-Jai/kv-store/pkg/raft"
	"github.com/Raj-Jai/kv-store/pkg/storage"
)

// Cluster is the Phase 2 shared harness (Developers A + B): an N-node
// in-process raft cluster on the MemTransport where every RPC passes through
// a fault-injecting transport and every node has a real DiskStore plus
// durable raft state on disk. Faults are driven by a seeded RNG so a fixed
// seed reproduces a schedule exactly.

// snapshotBridge adapts a node's DiskStore to raft's snapshot provider/sink,
// mirroring cmd/server. The compactor folds applied entries into a storage
// snapshot before trimming the raft log, so snapshot resync is exercised
// during chaos runs.
type snapshotBridge struct {
	node  *raft.Node
	store *storage.DiskStore
}

func (b *snapshotBridge) Snapshot() (raft.Snapshot, error) {
	idx := b.node.ApplyIndex()
	data, err := b.store.SerializeSnapshot()
	if err != nil {
		return raft.Snapshot{}, err
	}
	return raft.Snapshot{LastIncludedIndex: idx, LastIncludedTerm: b.node.LogTerm(idx), Data: data}, nil
}

func (b *snapshotBridge) ApplySnapshot(data []byte) error {
	return b.store.RestoreSnapshot(data)
}

type Cluster struct {
	t      *testing.T
	ids    []string
	base   *raft.MemTransport
	cfg    *FaultConfig
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	trans   map[string]*faultTransport
	nodes   map[string]*raft.Node
	stores  map[string]*storage.DiskStore
	dirs    map[string]string
	started map[string]bool

	hist *history
}

// NewCluster creates an N-node cluster under temp dirs. Call Start to bring
// nodes up.
func NewCluster(t *testing.T, n int, seed uint64) *Cluster {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	c := &Cluster{
		t:       t,
		base:    raft.NewMemTransport(),
		cfg:     newFaultConfig(seed),
		ctx:     ctx,
		cancel:  cancel,
		trans:   make(map[string]*faultTransport),
		nodes:   make(map[string]*raft.Node),
		stores:  make(map[string]*storage.DiskStore),
		dirs:    make(map[string]string),
		started: make(map[string]bool),
		hist:    newHistory(),
	}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("n%d", i)
		c.ids = append(c.ids, id)
		c.dirs[id] = filepath.Join(t.TempDir(), id)
	}
	t.Cleanup(func() {
		cancel()
		for id := range c.nodes {
			c.stopNode(id)
		}
	})
	return c
}

func (c *Cluster) peers(id string) []string {
	var peers []string
	for _, other := range c.ids {
		if other != id {
			peers = append(peers, other)
		}
	}
	return peers
}

// Start brings a node up: fresh or after a restart it reloads its DiskStore
// and durable raft state from disk.
func (c *Cluster) Start(id string) {
	c.mu.Lock()
	if c.started[id] {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	store, err := storage.OpenDiskStore(c.dirs[id])
	if err != nil {
		c.t.Fatalf("open store for %s: %v", id, err)
	}
	ft := &faultTransport{id: id, base: c.base, cfg: c.cfg}
	node := raft.NewNode(id, c.peers(id), ft, store)
	if err := node.SetRaftStore(&faultStore{RaftStore: raft.NewFileRaftStore(filepath.Join(c.dirs[id], "raft.json")), id: id, cfg: c.cfg}); err != nil {
		c.t.Fatalf("raft store for %s: %v", id, err)
	}
	bridge := &snapshotBridge{node: node, store: store}
	node.SetSnapshotter(bridge)
	node.SetSnapshotSink(bridge)

	c.base.Register(id, node)
	go node.Loop(c.ctx)
	node.StartApply(c.ctx)

	c.mu.Lock()
	c.trans[id] = ft
	c.nodes[id] = node
	c.stores[id] = store
	c.started[id] = true
	c.mu.Unlock()
}

// Stop brings a node down (loop, apply, transport registration, store).
func (c *Cluster) Stop(id string) {
	c.stopNode(id)
}

func (c *Cluster) stopNode(id string) {
	c.mu.Lock()
	if !c.started[id] {
		c.mu.Unlock()
		return
	}
	c.started[id] = false
	node := c.nodes[id]
	store := c.stores[id]
	c.mu.Unlock()

	node.Stop()
	c.base.Unregister(id)
	if store != nil {
		_ = store.Close()
	}
}

// Restart stops and starts a node so it recovers from disk.
func (c *Cluster) Restart(id string) {
	c.Stop(id)
	c.Start(id)
}

// StartAll brings every node up and waits for a leader.
func (c *Cluster) StartAll(timeout time.Duration) {
	for _, id := range c.ids {
		c.Start(id)
	}
	c.waitLeader(timeout)
}

func (c *Cluster) leaderID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, n := range c.nodes {
		if c.started[id] && n.IsLeader() {
			return id
		}
	}
	return ""
}

// Leader returns the current leader node, or nil if none.
func (c *Cluster) Leader() *raft.Node {
	id := c.leaderID()
	if id == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

func (c *Cluster) waitLeader(timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.Leader() != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no leader elected within %s", timeout)
}

// startedNodes snapshots the currently running nodes.
func (c *Cluster) startedNodes() map[string]*raft.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]*raft.Node)
	for id, n := range c.nodes {
		if c.started[id] {
			out[id] = n
		}
	}
	return out
}

func (c *Cluster) randomStartedNode() (string, *raft.Node) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var ids []string
	for id := range c.nodes {
		if c.started[id] {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return "", nil
	}
	sort.Strings(ids)
	id := ids[rand.IntN(len(ids))]
	return id, c.nodes[id]
}

// history records client-visible operations for the linearizability oracle.
type history struct {
	mu  sync.Mutex
	ops []op
}

type op struct {
	kind   string // "write" or "read"
	seq    uint64
	key    string
	value  string // write: value; read: value observed
	start  time.Time
	end    time.Time
	acked  bool   // write reached a majority; read succeeded
	readOn string // node id that served a read
}

func newHistory() *history { return &history{} }

func (h *history) add(o op) {
	h.mu.Lock()
	h.ops = append(h.ops, o)
	h.mu.Unlock()
}

func (h *history) snapshot() []op {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]op(nil), h.ops...)
}

// waitMajorityValue reports whether key=value has been applied on a quorum of
// the FULL cluster membership, which is what makes a write committed and thus
// durable. A majority of the currently started nodes is not enough: during a
// crash window that majority can be a minority of the cluster, and an entry
// applied only there may never commit and could be lost.
func (c *Cluster) waitMajorityValue(key, value string, timeout time.Duration) bool {
	quorum := len(c.ids)/2 + 1
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		count := 0
		for _, n := range c.startedNodes() {
			if v, err := n.Get(key); err == nil && v == value {
				count++
			}
		}
		if count >= quorum {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// RunWorkload issues versioned writes through the current leader and reads
// from random nodes until ctx is done, recording every op into the history.
func (c *Cluster) RunWorkload(ctx context.Context, every time.Duration) {
	seq := uint64(0)
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		leader := c.Leader()
		if leader == nil {
			continue
		}
		seq++
		value := fmt.Sprintf("v%d", seq)
		start := time.Now()
		err := leader.Put("k", value)
		if err != nil {
			continue
		}
		acked := c.waitMajorityValue("k", value, 2*time.Second)
		c.hist.add(op{kind: "write", seq: seq, key: "k", value: value, start: start, end: time.Now(), acked: acked})

		if id, node := c.randomStartedNode(); node != nil {
			rs := time.Now()
			v, rerr := node.Get("k")
			c.hist.add(op{kind: "read", seq: readSeq(v), key: "k", value: v, start: rs, end: time.Now(), acked: rerr == nil, readOn: id})
		}
	}
}

// readSeq extracts the version from a value string "v<seq>"; 0 when absent.
func readSeq(value string) uint64 {
	var seq uint64
	if _, err := fmt.Sscanf(value, "v%d", &seq); err != nil {
		return 0
	}
	return seq
}

// Run executes a fault schedule while the workload runs, then clears every
// fault and sleeps `settle` so the cluster recovers before oracles run.
func (c *Cluster) Run(faults []Fault, total, settle, every time.Duration) {
	workCtx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	go c.RunWorkload(workCtx, every)

	type ev struct {
		at    time.Duration
		f     Fault
		apply bool
	}
	var events []ev
	for _, f := range faults {
		events = append(events, ev{f.At, f, true}, ev{f.At + f.For, f, false})
	}
	sort.Slice(events, func(i, j int) bool { return events[i].at < events[j].at })

	start := time.Now()
	applied := map[Fault]bool{}
	for idx := 0; ; {
		elapsed := time.Since(start)
		if elapsed >= total {
			break
		}
		for idx < len(events) && events[idx].at <= elapsed {
			e := events[idx]
			if e.apply {
				c.applyFault(e.f)
				applied[e.f] = true
			} else {
				c.clearFault(e.f)
				delete(applied, e.f)
			}
			idx++
		}
		time.Sleep(5 * time.Millisecond)
	}
	for f := range applied {
		c.clearFault(f)
	}
	cancel()
	time.Sleep(settle)
}

func (c *Cluster) applyFault(f Fault) {
	switch f.Kind {
	case FaultCrash:
		c.Stop(f.Target)
	case FaultLeaderIsolation:
		if id := c.leaderID(); id != "" {
			c.cfg.setPartition([]string{id}, c.ids)
		}
	case FaultMinorityPartition:
		c.cfg.setPartition(c.ids[:len(c.ids)/2], c.ids)
	case FaultMajorityPartition:
		c.cfg.setPartition(c.ids[:(len(c.ids)+1)/2], c.ids)
	case FaultAsymmetricPartition:
		c.cfg.setEdgeDrop(f.Target, f.Peer)
	case FaultPacketLoss:
		c.cfg.setLoss(f.Rate)
	case FaultDuplication:
		c.cfg.setDup(f.Rate)
	case FaultReordering:
		c.cfg.setReorder(f.Rate, f.Delay)
	case FaultDelay:
		c.cfg.setDelay(f.Delay)
	case FaultClockSkew:
		c.cfg.setSkew(f.Target, f.Delay)
	case FaultFsError:
		c.cfg.setFsync(f.Target, true)
	}
}

func (c *Cluster) clearFault(f Fault) {
	switch f.Kind {
	case FaultCrash:
		c.Start(f.Target)
	case FaultLeaderIsolation, FaultMinorityPartition, FaultMajorityPartition:
		c.cfg.clearPartition()
	case FaultAsymmetricPartition:
		c.cfg.clearEdgeDrop(f.Target, f.Peer)
	case FaultPacketLoss:
		c.cfg.setLoss(0)
	case FaultDuplication:
		c.cfg.setDup(0)
	case FaultReordering:
		c.cfg.setReorder(0, 0)
	case FaultDelay:
		c.cfg.setDelay(0)
	case FaultClockSkew:
		c.cfg.clearSkew(f.Target)
	case FaultFsError:
		c.cfg.setFsync(f.Target, false)
	}
}

// AssertInvariants runs every oracle and fails the test on any violation.
func (c *Cluster) AssertInvariants() {
	c.t.Helper()
	var errs []error
	for _, o := range defaultOracles() {
		errs = append(errs, o.Check(c)...)
	}
	if len(errs) == 0 {
		return
	}
	for _, err := range errs {
		c.t.Errorf("invariant violated: %v", err)
	}
}
