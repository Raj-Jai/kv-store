package storage

import (
	"errors"
	"sync"
	"testing"
)

func TestMemStoreCRUD(t *testing.T) {
	s := NewMemStore()

	if _, err := s.Get("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	if err := s.Put("foo", "bar"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	value, err := s.Get("foo")
	if err != nil || value != "bar" {
		t.Fatalf("Get = %q, %v; want %q", value, err, "bar")
	}

	if err := s.Delete("foo"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, err := s.Get("foo"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestMemStoreClear(t *testing.T) {
	s := NewMemStore()

	for i := 0; i < 10; i++ {
		s.Put("k", string(rune('a'+i)))
	}

	if err := s.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}

	if _, err := s.Get("k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after Clear, got %v", err)
	}
}

func TestMemStoreConcurrent(t *testing.T) {
	s := NewMemStore()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := string(rune('a' + i%26))
			s.Put(key, "v")
			s.Get(key)
			s.Delete(key)
		}(i)
	}
	wg.Wait()
}

func TestMemStoreCloseNoop(t *testing.T) {
	s := NewMemStore()
	if err := s.Close(); err != nil {
		t.Fatalf("MemStore.Close = %v", err)
	}
}
