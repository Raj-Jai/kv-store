package storage

import "sync"

// FakeEngine is the contract fake for Engine. Both developers build against
// it before a concrete store is wired up, so it must stay dependency-free.
type FakeEngine struct {
	mu      sync.Mutex
	data    map[string]string
	cleared bool
}

func NewFakeEngine() *FakeEngine {
	return &FakeEngine{data: make(map[string]string)}
}

func (f *FakeEngine) Get(key string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.data[key]
	if !ok {
		return "", ErrNotFound
	}
	return v, nil
}

func (f *FakeEngine) Put(key, value string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[key] = value
	return nil
}

func (f *FakeEngine) Delete(key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.data, key)
	return nil
}

func (f *FakeEngine) Clear() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = make(map[string]string)
	f.cleared = true
	return nil
}

func (f *FakeEngine) Close() error { return nil }

func (f *FakeEngine) WasCleared() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cleared
}
