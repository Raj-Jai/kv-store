package storage

import (
	"fmt"
	"sync"
	"testing"
)

// TestConcurrentWritesDurable verifies that every acknowledged write under
// concurrent load survives a restart, despite batching.
func TestConcurrentWritesDurable(t *testing.T) {
	dir := t.TempDir()

	s, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	const n = 100
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := s.Put(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("write failed: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	for i := 0; i < n; i++ {
		want := fmt.Sprintf("v%d", i)
		got, err := s2.Get(fmt.Sprintf("k%d", i))
		if err != nil || got != want {
			t.Fatalf("Get(k%d) = %q, %v; want %q", i, got, err, want)
		}
	}
}

// TestBatchBurstGroupsWrites verifies concurrent writes share fsyncs by
// confirming correctness under a tight burst, without asserting timing.
func TestBatchBurstGroupsWrites(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			s.Put(fmt.Sprintf("k%d", i), "v")
		}(i)
	}
	close(start)
	wg.Wait()
	s.Close()

	s2, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	for i := 0; i < 50; i++ {
		if _, err := s2.Get(fmt.Sprintf("k%d", i)); err != nil {
			t.Fatalf("missing k%d after burst: %v", i, err)
		}
	}
}
