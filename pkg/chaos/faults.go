package chaos

import (
	"math/rand/v2"
	"sync"
	"time"
)

// Fault model and injectors — Developer A (Phase 2). A Fault is applied and
// later cleared by the harness schedule; message-level faults (loss, dup,
// reorder, delay, skew, partitions) are enforced by the fault transports,
// node-level faults (crash) by the cluster, and storage faults (fsync) by
// wrapping each node's raft store.

// Kind identifies the injected fault.
type Kind uint8

const (
	FaultCrash               Kind = iota + 1 // stop a node (no restart)
	FaultLeaderIsolation                     // cut the leader off from every other node
	FaultMinorityPartition                   // cut a minority off from the majority
	FaultMajorityPartition                   // cut a majority off from the minority
	FaultAsymmetricPartition                 // drop only the Target->Peer direction
	FaultPacketLoss                          // probabilistically drop messages
	FaultDuplication                         // probabilistically deliver messages twice
	FaultReordering                          // probabilistic reordering via jitter
	FaultDelay                               // uniform latency on every message
	FaultClockSkew                           // constant extra latency from one node
	FaultFsError                             // raft store Save (fsync) fails
)

// Fault is a single scheduled injection. Target is a node id, or "" for the
// whole cluster. Peer is the far side of an asymmetric partition.
type Fault struct {
	Kind   Kind
	Target string
	Peer   string
	At     time.Duration // start offset from the schedule start
	For    time.Duration // active duration
	Rate   float64       // probability for loss/dup/reorder
	Delay  time.Duration // latency for Delay/ClockSkew/Reordering
}

type edge struct{ from, to string }

// FaultConfig is the shared mutable fault state consulted by every fault
// transport and by the cluster. All decisions draw from one seeded RNG so a
// fixed seed reproduces a schedule exactly.
type FaultConfig struct {
	mu        sync.Mutex
	rng       *rand.Rand
	isolated  map[string]bool
	edgeDrop  map[edge]bool
	lossRate  float64
	dupRate   float64
	reorder   float64
	delayMin  time.Duration
	delayMax  time.Duration
	skew      map[string]time.Duration
	failFsync map[string]bool
}

func newFaultConfig(seed uint64) *FaultConfig {
	return &FaultConfig{
		rng:       rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		isolated:  make(map[string]bool),
		edgeDrop:  make(map[edge]bool),
		skew:      make(map[string]time.Duration),
		failFsync: make(map[string]bool),
	}
}

// route decides, for a message from a node to a peer, whether it is dropped
// and how long delivery is delayed. Deterministic for a fixed seed.
func (c *FaultConfig) route(from, to string) (drop bool, latency time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isolated[from] || c.isolated[to] {
		return true, 0
	}
	if c.edgeDrop[edge{from, to}] {
		return true, 0
	}
	if c.lossRate > 0 && c.rng.Float64() < c.lossRate {
		return true, 0
	}
	if c.delayMax > c.delayMin {
		latency = c.delayMin + time.Duration(c.rng.IntN(int(c.delayMax-c.delayMin)))
	} else {
		latency = c.delayMin
	}
	latency += c.skew[from]
	if c.reorder > 0 && c.rng.Float64() < c.reorder {
		jitter := 100 * time.Millisecond
		if c.delayMax > jitter {
			jitter = c.delayMax
		}
		latency += time.Duration(c.rng.IntN(int(jitter)))
	}
	return false, latency
}

// shouldDuplicate reports whether an outbound message should be sent twice.
func (c *FaultConfig) shouldDuplicate() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dupRate > 0 && c.rng.Float64() < c.dupRate
}

// fsyncFails reports whether the raft store of node id should fail its Save.
func (c *FaultConfig) fsyncFails(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failFsync[""] || c.failFsync[id]
}

// setPartition drops every edge (both directions) between the subset and its
// complement. Intra-side edges stay intact.
func (c *FaultConfig) setPartition(subset []string, all []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	in := make(map[string]bool, len(subset))
	for _, s := range subset {
		in[s] = true
	}
	for _, a := range all {
		for _, b := range all {
			if in[a] != in[b] {
				c.edgeDrop[edge{a, b}] = true
				c.edgeDrop[edge{b, a}] = true
			}
		}
	}
}

// clearPartition removes every partition edge.
func (c *FaultConfig) clearPartition() {
	c.mu.Lock()
	defer c.mu.Unlock()
	clear(c.edgeDrop)
}

// setEdgeDrop isolates a single directional edge (asymmetric partition).
func (c *FaultConfig) setEdgeDrop(from, to string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.edgeDrop[edge{from, to}] = true
}

func (c *FaultConfig) clearEdgeDrop(from, to string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.edgeDrop, edge{from, to})
}

func (c *FaultConfig) setLoss(rate float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lossRate = rate
}

func (c *FaultConfig) setDup(rate float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dupRate = rate
}

func (c *FaultConfig) setReorder(rate float64, jitter time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reorder = rate
	if jitter > 0 {
		c.delayMax = jitter
	}
}

func (c *FaultConfig) setDelay(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.delayMin = d
	c.delayMax = d
}

func (c *FaultConfig) setSkew(id string, d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.skew[id] = d
}

func (c *FaultConfig) clearSkew(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.skew, id)
}

func (c *FaultConfig) setFsync(id string, on bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if on {
		c.failFsync[id] = true
	} else {
		delete(c.failFsync, id)
	}
}
