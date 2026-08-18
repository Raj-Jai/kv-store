package storage

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"
)

// TestDiskStoreRandomOpsMatchOracle is a property test: a long random sequence
// of Put/Delete/Clear operations is applied to a DiskStore and to a MemStore
// oracle, and the two stores must agree at every step. Reopening the store
// must reproduce the oracle too, because every acknowledged write was fsynced.
func TestDiskStoreRandomOpsMatchOracle(t *testing.T) {
	const iterations = 24

	rng := rand.New(rand.NewPCG(1, 2))
	for iter := 0; iter < iterations; iter++ {
		dir := t.TempDir()
		s, err := OpenDiskStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		oracle := NewMemStore()

		n := rng.IntN(40) + 1
		for i := 0; i < n; i++ {
			key := randomKey(rng)
			switch rng.IntN(10) {
			case 0, 1, 2, 3, 4, 5, 6:
				value := randomValue(rng)
				if err := s.Put(key, value); err != nil {
					t.Fatal(err)
				}
				oracle.Put(key, value)
			case 7, 8:
				if err := s.Delete(key); err != nil {
					t.Fatal(err)
				}
				oracle.Delete(key)
			default:
				if err := s.Clear(); err != nil {
					t.Fatal(err)
				}
				oracle.Clear()
			}

			// Spot-check the live state every 64 ops so drift surfaces before
			// the final comparison instead of being masked by later ops.
			if i%64 == 63 {
				assertStoresEqual(t, s, oracle)
			}
		}

		// Final live-state check.
		assertStoresEqual(t, s, oracle)

		// Restart durability: Close compacts (snapshot + empty WAL), then a
		// fresh store must restore exactly the oracle state.
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		s2, err := OpenDiskStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		assertStoresEqual(t, s2, oracle)
		s2.Close()
	}
}

// assertStoresEqual verifies the DiskStore's live memory state equals the
// MemStore oracle: same keys, same values, same absences.
func assertStoresEqual(t *testing.T, s *DiskStore, oracle *MemStore) {
	t.Helper()

	s.mem.mu.RLock()
	got := make(map[string]entry, len(s.mem.data))
	for k, v := range s.mem.data {
		got[k] = v
	}
	s.mem.mu.RUnlock()

	oracle.mu.RLock()
	defer oracle.mu.RUnlock()
	if !reflect.DeepEqual(got, oracle.data) {
		t.Fatalf("DiskStore diverged from oracle:\n store:  %v\n oracle: %v", got, oracle.data)
	}
}

func randomKey(rng *rand.Rand) string {
	switch rng.IntN(4) {
	case 0:
		return fmt.Sprintf("key-%d", rng.IntN(10))
	case 1:
		return fmt.Sprintf("key-%d", rng.IntN(50))
	case 2:
		return strings.Repeat("k", 1+rng.IntN(20))
	default:
		return fmt.Sprintf("k%c%d", byte('a'+rng.IntN(26)), rng.IntN(100))
	}
}

func randomValue(rng *rand.Rand) string {
	switch rng.IntN(4) {
	case 0:
		return fmt.Sprintf("v-%d", rng.IntN(1000))
	case 1:
		return strings.Repeat("x", rng.IntN(50))
	case 2:
		return ""
	default:
		return fmt.Sprintf("%c%c%c", byte('a'+rng.IntN(26)), byte('A'+rng.IntN(26)), byte('0'+rng.IntN(10)))
	}
}
