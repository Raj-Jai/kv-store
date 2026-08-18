package chaos

import (
	"fmt"

	"github.com/Raj-Jai/kv-store/pkg/linearizability"
)

// Invariant oracles — Developer B owns this file (provisional versions by
// Developer A so the shared harness is gated end to end). Each oracle checks
// one raft invariant over the live cluster.

// Oracle checks an invariant over a cluster and returns the violations found.
type Oracle interface {
	Name() string
	Check(c *Cluster) []error
}

func defaultOracles() []Oracle {
	return []Oracle{
		electionSafetyOracle{},
		logMatchingOracle{},
		stateMachineSafetyOracle{},
		durabilityOracle{},
		linearizabilityOracle{},
	}
}

// electionSafetyOracle: at most one leader per term. During a partition a
// minority cannot reach quorum, so only the majority elects — in a NEW term;
// an isolated old-term leader must step down. Two leaders may never share a
// term.
type electionSafetyOracle struct{}

func (electionSafetyOracle) Name() string { return "election-safety" }

func (electionSafetyOracle) Check(c *Cluster) []error {
	var errs []error
	leaders := map[int]string{}
	for id, n := range c.startedNodes() {
		if !n.IsLeader() {
			continue
		}
		term := n.Term()
		if prev, ok := leaders[term]; ok && prev != id {
			errs = append(errs, fmt.Errorf("two leaders in term %d: %s and %s", term, prev, id))
		}
		leaders[term] = id
	}
	return errs
}

// logMatchingOracle: if two nodes hold an entry at the same index with the
// same term, it must be the same command. Different terms at the same index
// are allowed transiently (uncommitted conflicts); they are resolved by the
// leader.
type logMatchingOracle struct{}

func (logMatchingOracle) Name() string { return "log-matching" }

func (logMatchingOracle) Check(c *Cluster) []error {
	nodes := c.startedNodes()
	ids := make([]string, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	var errs []error
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			a, b := nodes[ids[i]], nodes[ids[j]]
			for idx := 1; ; idx++ {
				ea, oka := a.LogEntry(idx)
				eb, okb := b.LogEntry(idx)
				if !oka || !okb {
					break
				}
				if ea.Term == eb.Term && ea.Cmd != eb.Cmd {
					errs = append(errs, fmt.Errorf("log divergence at index %d between %s and %s: %+v vs %+v", idx, ids[i], ids[j], ea.Cmd, eb.Cmd))
					break
				}
			}
		}
	}
	return errs
}

// stateMachineSafetyOracle: no two nodes apply different commands at the same
// log index. Since each node applies its log prefix in order, we compare the
// commands of every commonly applied index.
type stateMachineSafetyOracle struct{}

func (stateMachineSafetyOracle) Name() string { return "state-machine-safety" }

func (stateMachineSafetyOracle) Check(c *Cluster) []error {
	nodes := c.startedNodes()
	ids := make([]string, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	var errs []error
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			a, b := nodes[ids[i]], nodes[ids[j]]
			limit := a.ApplyIndex()
			if b.ApplyIndex() < limit {
				limit = b.ApplyIndex()
			}
			for idx := 1; idx <= limit; idx++ {
				ea, oka := a.LogEntry(idx)
				eb, okb := b.LogEntry(idx)
				if !oka || !okb {
					break
				}
				if ea.Cmd != eb.Cmd {
					errs = append(errs, fmt.Errorf("applied different commands at index %d on %s and %s: %+v vs %+v", idx, ids[i], ids[j], ea.Cmd, eb.Cmd))
					break
				}
			}
		}
	}
	return errs
}

// durabilityOracle: every acked write (replicated + applied on a majority)
// must be readable on every started node after the run has healed.
type durabilityOracle struct{}

func (durabilityOracle) Name() string { return "durability" }

func (durabilityOracle) Check(c *Cluster) []error {
	var errs []error
	acked := map[uint64]string{} // seq -> value
	for _, o := range c.hist.snapshot() {
		if o.kind == "write" && o.acked {
			acked[o.seq] = o.value
		}
	}
	nodes := c.startedNodes()
	for seq, value := range acked {
		for id, n := range nodes {
			v, err := n.Get("k")
			if err != nil {
				errs = append(errs, fmt.Errorf("acked write %s (seq %d) unreadable on %s: %v", value, seq, id, err))
				continue
			}
			if readSeq(v) < seq {
				errs = append(errs, fmt.Errorf("acked write %s (seq %d) rolled back to %s on %s", value, seq, v, id))
			}
		}
	}
	return errs
}

// linearizabilityOracle delegates to the single-key checker: each node's
// state machine only moves forward, so reads served by a single node must
// never roll back. Cross-node rollback is expected and allowed — followers
// legitimately serve stale data until this store adds linearizable (leader +
// read-index) reads. Write linearizability is covered by the durability
// oracle plus the consensus invariants.
type linearizabilityOracle struct{}

func (linearizabilityOracle) Name() string { return "linearizability" }

func (linearizabilityOracle) Check(c *Cluster) []error {
	var ops []linearizability.Op
	for _, o := range c.hist.snapshot() {
		if o.kind != "read" || o.readOn == "" {
			continue
		}
		ops = append(ops, linearizability.Op{Kind: linearizability.Read, Seq: o.seq, Reader: o.readOn, End: o.end.UnixNano()})
	}
	return linearizability.Check(ops)
}
