package chaos

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

// Fault matrix — Phase 2 gate. Each scenario runs the 5-node cluster under a
// single fault kind and asserts every invariant holds after the fault heals.
// The seeded RNG makes each scenario reproducible: the default fixed seed is
// used by CI, and CHAOS_SEED overrides it so a nightly randomized run can pick
// any seed (Phase 3).
const (
	startUp  = 1 * time.Second
	baseline = 400 * time.Millisecond
	active   = 1200 * time.Millisecond
	settle   = 1200 * time.Millisecond
	every    = 100 * time.Millisecond
)

// gateSeed returns the RNG seed for the fault matrix: CHAOS_SEED if set
// (parsed as uint64), otherwise the default fixed seed.
func gateSeed() uint64 {
	const def = 42
	if s := os.Getenv("CHAOS_SEED"); s != "" {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			panic(fmt.Sprintf("invalid CHAOS_SEED %q: %v", s, err))
		}
		return v
	}
	return def
}

// runScenario starts a fresh cluster, lets it elect a leader, runs the
// workload while the fault is active, then heals and asserts invariants.
func runScenario(t *testing.T, f Fault) {
	t.Helper()
	c := NewCluster(t, 5, gateSeed())
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
	c := NewCluster(t, 5, gateSeed())
	c.StartAll(startUp)
	// Crash two nodes at once; quorum (3 of 5) still holds.
	c.Run([]Fault{
		{Kind: FaultCrash, Target: "n1", At: baseline, For: active},
		{Kind: FaultCrash, Target: "n2", At: baseline, For: active},
	}, baseline+active, settle, every)
	c.AssertInvariants()
}

func TestFaultCrashQuorum(t *testing.T) {
	c := NewCluster(t, 5, gateSeed())
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
	c := NewCluster(t, 5, gateSeed())
	c.StartAll(startUp)
	c.Run([]Fault{
		{Kind: FaultPacketLoss, Rate: 0.2, At: baseline, For: active},
		{Kind: FaultCrash, Target: "n1", At: baseline + 300*time.Millisecond, For: active - 300*time.Millisecond},
		{Kind: FaultLeaderIsolation, At: baseline + 600*time.Millisecond, For: active - 600*time.Millisecond},
		{Kind: FaultDelay, Delay: 300 * time.Millisecond, At: baseline + 900*time.Millisecond, For: active - 900*time.Millisecond},
	}, baseline+active, 2*settle, every)
	c.AssertInvariants()
}

// TestFaultAlternateSeed proves the gate does not depend on seed 42: a
// representative slice of the fault matrix is run under a second seed with the
// same schedule timings. CI can point the nightly randomized-seed run at any
// seed (Developer B).
func TestFaultAlternateSeed(t *testing.T) {
	altSeed := uint64(7)
	scenarios := []struct {
		name string
		f    Fault
	}{
		{"crash-leader", Fault{Kind: FaultCrash, Target: "n0", At: baseline, For: active}},
		{"majority-partition", Fault{Kind: FaultMajorityPartition, At: baseline, For: active}},
		{"fsync-error", Fault{Kind: FaultFsError, Target: "", At: baseline, For: active}},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			c := NewCluster(t, 5, altSeed)
			c.StartAll(startUp)
			c.Run([]Fault{sc.f}, baseline+active, settle, every)
			c.AssertInvariants()
		})
	}
}
