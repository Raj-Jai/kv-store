package chaos

import (
	"testing"
	"time"
)

// Fault matrix — Phase 2 gate. Each scenario runs the 5-node cluster under a
// single fault kind with a fixed seed and asserts every invariant holds after
// the fault heals. The seeded RNG makes each scenario reproducible.
const (
	seed     uint64 = 42
	startUp         = 1 * time.Second
	baseline        = 400 * time.Millisecond
	active          = 1200 * time.Millisecond
	settle          = 1200 * time.Millisecond
	every           = 100 * time.Millisecond
)

// runScenario starts a fresh cluster, lets it elect a leader, runs the
// workload while the fault is active, then heals and asserts invariants.
func runScenario(t *testing.T, f Fault) {
	t.Helper()
	c := NewCluster(t, 5, seed)
	c.StartAll(startUp)

	c.Run([]Fault{f}, baseline+active, settle, every)
	c.AssertInvariants()
}

func TestFaultCrashLeader(t *testing.T) {
	runScenario(t, Fault{Kind: FaultCrash, Target: "n0", At: baseline, For: active})
}

func TestFaultCrashFollower(t *testing.T) {
	runScenario(t, Fault{Kind: FaultCrash, Target: "n1", At: baseline, For: active})
}

func TestFaultCrashMultiple(t *testing.T) {
	c := NewCluster(t, 5, seed)
	c.StartAll(startUp)
	// Crash two nodes at once; quorum (3 of 5) still holds.
	c.Run([]Fault{
		{Kind: FaultCrash, Target: "n1", At: baseline, For: active},
		{Kind: FaultCrash, Target: "n2", At: baseline, For: active},
	}, baseline+active, settle, every)
	c.AssertInvariants()
}

func TestFaultCrashQuorum(t *testing.T) {
	c := NewCluster(t, 5, seed)
	c.StartAll(startUp)
	// Crash three nodes: quorum is lost. When they return, the survivors must
	// re-elect and durability of previously acked writes must hold.
	c.Run([]Fault{
		{Kind: FaultCrash, Target: "n1", At: baseline, For: active},
		{Kind: FaultCrash, Target: "n2", At: baseline, For: active},
		{Kind: FaultCrash, Target: "n3", At: baseline, For: active},
	}, baseline+active, 2*settle, every)
	c.AssertInvariants()
}

func TestFaultLeaderIsolation(t *testing.T) {
	runScenario(t, Fault{Kind: FaultLeaderIsolation, At: baseline, For: active})
}

func TestFaultMinorityPartition(t *testing.T) {
	runScenario(t, Fault{Kind: FaultMinorityPartition, At: baseline, For: active})
}

func TestFaultMajorityPartition(t *testing.T) {
	runScenario(t, Fault{Kind: FaultMajorityPartition, At: baseline, For: active})
}

func TestFaultAsymmetricPartition(t *testing.T) {
	runScenario(t, Fault{Kind: FaultAsymmetricPartition, Target: "n1", Peer: "n2", At: baseline, For: active})
}

func TestFaultPacketLoss(t *testing.T) {
	runScenario(t, Fault{Kind: FaultPacketLoss, Rate: 0.3, At: baseline, For: active})
}

func TestFaultDuplication(t *testing.T) {
	runScenario(t, Fault{Kind: FaultDuplication, Rate: 0.2, At: baseline, For: active})
}

func TestFaultReordering(t *testing.T) {
	runScenario(t, Fault{Kind: FaultReordering, Rate: 0.4, Delay: 150 * time.Millisecond, At: baseline, For: active})
}

func TestFaultDelay(t *testing.T) {
	runScenario(t, Fault{Kind: FaultDelay, Delay: 500 * time.Millisecond, At: baseline, For: active})
}

func TestFaultClockSkew(t *testing.T) {
	runScenario(t, Fault{Kind: FaultClockSkew, Target: "n0", Delay: 600 * time.Millisecond, At: baseline, For: active})
}

func TestFaultFsError(t *testing.T) {
	runScenario(t, Fault{Kind: FaultFsError, Target: "", At: baseline, For: active})
}

// TestFaultMultiFaultSchedule overlaps several fault kinds in one run with the
// fixed seed, exercising the shared seeded RNG across faults and the oracle
// set over a combined failure window (Developer B).
func TestFaultMultiFaultSchedule(t *testing.T) {
	c := NewCluster(t, 5, seed)
	c.StartAll(startUp)
	c.Run([]Fault{
		{Kind: FaultPacketLoss, Rate: 0.2, At: baseline, For: active},
		{Kind: FaultCrash, Target: "n1", At: baseline + 300*time.Millisecond, For: active - 300*time.Millisecond},
		{Kind: FaultLeaderIsolation, At: baseline + 600*time.Millisecond, For: active - 600*time.Millisecond},
		{Kind: FaultDelay, Delay: 300 * time.Millisecond, At: baseline + 900*time.Millisecond, For: active - 900*time.Millisecond},
	}, baseline+active, 2*settle, every)
	c.AssertInvariants()
}
