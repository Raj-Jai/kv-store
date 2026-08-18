package storage

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// TestConcurrentReadsAndWrites hammers a store with mixed readers and writers
// on shared keys and verifies every acknowledged write survives a restart.
func TestConcurrentReadsAndWrites(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	const (
		writers = 20
		readers = 40
		perKey  = 10
	)
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	var writeErrs int64

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < perKey; i++ {
				key := keys[(w*5+i)%len(keys)]
				if err := s.Put(key, fmt.Sprintf("v%d", w)); err != nil {
					atomic.AddInt64(&writeErrs, 1)
				}
			}
		}(w)
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			<-start
			for i := 0; i < perKey; i++ {
				s.Get(keys[(r+i)%len(keys)])
			}
		}(r)
	}

	close(start)
	wg.Wait()
	if writeErrs > 0 {
		t.Fatalf("%d writes failed", writeErrs)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	for _, key := range keys {
		if _, err := s2.Get(key); err != nil {
			t.Fatalf("Get(%q) failed after restart: %v", key, err)
		}
	}
}

// TestDeletesDuringReads runs concurrent deleters against a set of keys while
// readers read them; the race detector must find no data races.
func TestDeletesDuringReads(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 50; i++ {
		if err := s.Put(fmt.Sprintf("k%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}

	start := make(chan struct{})
	var wg sync.WaitGroup

	for d := 0; d < 5; d++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 50; i++ {
				s.Delete(fmt.Sprintf("k%d", i))
			}
		}()
	}
	for r := 0; r < 20; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 50; i++ {
				s.Get(fmt.Sprintf("k%d", i))
			}
		}()
	}

	close(start)
	wg.Wait()
	s.Close()
}

// TestGracefulShutdownDuringWrites closes the store while writers are still
// issuing writes. Every write either succeeds (and must be durable) or
// returns ErrClosed; nothing may panic or lose an acknowledged write.
func TestGracefulShutdownDuringWrites(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	const writers = 10
	var wg sync.WaitGroup
	var acked, rejected int64

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				err := s.Put(fmt.Sprintf("w%d-k%d", w, i), "v")
				switch {
				case err == nil:
					atomic.AddInt64(&acked, 1)
				case errors.Is(err, ErrClosed):
					atomic.AddInt64(&rejected, 1)
					return
				default:
					t.Errorf("unexpected write error: %v", err)
					return
				}
			}
		}(w)
	}

	s.Close()
	wg.Wait()

	if acked == 0 {
		t.Fatal("no writes were acknowledged before close")
	}

	s2, err := OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	readable := 0
	for w := 0; w < writers; w++ {
		for i := 0; i < 100; i++ {
			if _, err := s2.Get(fmt.Sprintf("w%d-k%d", w, i)); err == nil {
				readable++
			}
		}
	}
	if int64(readable) < acked {
		t.Fatalf("lost acknowledged writes: acked=%d readable=%d", acked, readable)
	}
	t.Logf("acked=%d rejected=%d readable=%d", acked, rejected, readable)
}
