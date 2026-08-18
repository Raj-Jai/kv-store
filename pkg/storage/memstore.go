package storage

import (
	"errors"
	"sort"
	"strconv"
	"sync"
	"time"
)

// entry is one stored key with its optional absolute expiry deadline in unix
// nanoseconds. Exp == 0 means the key never expires.
type entry struct {
	Value string `json:"value"`
	Exp   int64  `json:"exp,omitempty"`
}

// MemStore is a thread-safe in-memory implementation of Engine with lazy and
// active expiry.
type MemStore struct {
	mu      sync.RWMutex
	data    map[string]entry
	expired uint64
	now     func() time.Time
}

// NewMemStore creates an empty in-memory store.
func NewMemStore() *MemStore {
	return &MemStore{data: make(map[string]entry), now: time.Now}
}

func (m *MemStore) entryExpired(e entry, now int64) bool {
	return e.Exp != 0 && now >= e.Exp
}

func (m *MemStore) Get(key string) (string, error) {
	m.mu.RLock()
	e, ok := m.data[key]
	if !ok || !m.entryExpired(e, m.now().UnixNano()) {
		m.mu.RUnlock()
		if !ok {
			return "", ErrNotFound
		}
		return e.Value, nil
	}
	m.mu.RUnlock()

	// Lazy expiry: drop the dead key once.
	m.mu.Lock()
	e, ok = m.data[key]
	if ok && m.entryExpired(e, m.now().UnixNano()) {
		delete(m.data, key)
		m.expired++
	}
	m.mu.Unlock()
	return "", ErrNotFound
}

func (m *MemStore) Put(key, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = entry{Value: value}
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

// Size returns the number of live (non-expired) keys.
func (m *MemStore) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	now := m.now().UnixNano()
	n := 0
	for _, e := range m.data {
		if !m.entryExpired(e, now) {
			n++
		}
	}
	return n
}

func (m *MemStore) Incr(key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.data[key]
	if ok && m.entryExpired(e, m.now().UnixNano()) {
		delete(m.data, key)
		m.expired++
		ok = false
	}
	if !ok {
		m.data[key] = entry{Value: "1"}
		return 1, nil
	}
	v, err := strconv.ParseInt(e.Value, 10, 64)
	if err != nil {
		return 0, ErrNotNumeric
	}
	nv := v + 1
	m.data[key] = entry{Value: strconv.FormatInt(nv, 10)}
	return nv, nil
}

func (m *MemStore) CAS(key, old, new string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.data[key]
	if ok && m.entryExpired(e, m.now().UnixNano()) {
		delete(m.data, key)
		m.expired++
		ok = false
	}
	if !ok {
		return false, ErrNotFound
	}
	if e.Value != old {
		return false, nil
	}
	m.data[key] = entry{Value: new}
	return true, nil
}

func (m *MemStore) Expire(key string, expiresAt int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.data[key]
	if ok && m.entryExpired(e, m.now().UnixNano()) {
		delete(m.data, key)
		m.expired++
		ok = false
	}
	if !ok {
		return ErrNotFound
	}
	m.data[key] = entry{Value: e.Value, Exp: expiresAt}
	return nil
}

// SweepExpired removes every expired key and returns how many were dropped.
// It is called periodically by the store's active-expiration loop.
func (m *MemStore) SweepExpired() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now().UnixNano()
	n := 0
	for k, e := range m.data {
		if m.entryExpired(e, now) {
			delete(m.data, k)
			n++
		}
	}
	m.expired += uint64(n)
	return n
}

// ExpiredCount reports how many keys have been dropped by expiry since the
// store was created.
func (m *MemStore) ExpiredCount() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.expired
}

func (m *MemStore) Scan(cursor string, count int, pattern string) ([]KeyValue, string, error) {
	if count <= 0 {
		return nil, "", errors.New("scan: count must be positive")
	}
	m.mu.RLock()
	now := m.now().UnixNano()
	all := make([]KeyValue, 0, len(m.data))
	for k, e := range m.data {
		if m.entryExpired(e, now) {
			continue
		}
		if cursor != "" && k <= cursor {
			continue
		}
		if !matchGlob(pattern, k) {
			continue
		}
		all = append(all, KeyValue{Key: k, Value: e.Value})
	}
	m.mu.RUnlock()

	sort.Slice(all, func(i, j int) bool { return all[i].Key < all[j].Key })
	if len(all) > count {
		all = all[:count]
	}
	next := ""
	if len(all) == count {
		next = all[len(all)-1].Key
	}
	return all, next, nil
}
