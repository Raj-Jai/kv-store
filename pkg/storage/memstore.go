package storage

import "sync"

// MemStore is a thread-safe in-memory implementation of Engine.
type MemStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewMemStore creates an empty in-memory store.
func NewMemStore() *MemStore {
	return &MemStore{data: make(map[string]string)}
}

func (m *MemStore) Get(key string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, ok := m.data[key]
	if !ok {
		return "", ErrNotFound
	}
	return value, nil
}

func (m *MemStore) Put(key, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = value
	return nil
}

func (m *MemStore) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, key)
	return nil
}

func (m *MemStore) Clear() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	clear(m.data)
	return nil
}

func (m *MemStore) Close() error {
	return nil
}
