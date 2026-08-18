package storage

import (
	"sort"
	"strconv"
	"sync"
	"time"
)

// FakeEngine is the contract fake for Engine. Both developers build against
// it before a concrete store is wired up, so it must stay dependency-free.
type FakeEngine struct {
	mu      sync.Mutex
	data    map[string]string
	expires map[string]int64 // key -> unix-nano deadline
	cleared bool
	now     func() time.Time
}

func NewFakeEngine() *FakeEngine {
	return &FakeEngine{data: make(map[string]string), expires: make(map[string]int64), now: time.Now}
}

func (f *FakeEngine) Get(key string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.expiredLocked(key) {
		delete(f.data, key)
		delete(f.expires, key)
		return "", ErrNotFound
	}
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
	delete(f.expires, key)
	return nil
}

func (f *FakeEngine) Delete(key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.data, key)
	delete(f.expires, key)
	return nil
}

func (f *FakeEngine) Clear() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = make(map[string]string)
	f.expires = make(map[string]int64)
	f.cleared = true
	return nil
}

func (f *FakeEngine) Close() error { return nil }

func (f *FakeEngine) WasCleared() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cleared
}

// Incr implements the Engine contract for the fake.
func (f *FakeEngine) Incr(key string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.expiredLocked(key) {
		delete(f.data, key)
		delete(f.expires, key)
	}
	cur := f.data[key]
	if cur == "" {
		f.data[key] = "1"
		return 1, nil
	}
	v, err := strconv.ParseInt(cur, 10, 64)
	if err != nil {
		return 0, ErrNotNumeric
	}
	f.data[key] = strconv.FormatInt(v+1, 10)
	return v + 1, nil
}

func (f *FakeEngine) CAS(key, old, new string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.expiredLocked(key) {
		delete(f.data, key)
		delete(f.expires, key)
	}
	cur, ok := f.data[key]
	if !ok {
		return false, ErrNotFound
	}
	if cur != old {
		return false, nil
	}
	f.data[key] = new
	delete(f.expires, key)
	return true, nil
}

func (f *FakeEngine) Expire(key string, expiresAt int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.expiredLocked(key) {
		delete(f.data, key)
		delete(f.expires, key)
	}
	if _, ok := f.data[key]; !ok {
		return ErrNotFound
	}
	f.expires[key] = expiresAt
	return nil
}

func (f *FakeEngine) Scan(cursor string, count int, pattern string) ([]KeyValue, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.now().UnixNano()
	keys := make([]string, 0, len(f.data))
	for k := range f.data {
		if exp, ok := f.expires[k]; ok && now >= exp {
			continue
		}
		if cursor != "" && k <= cursor {
			continue
		}
		if !matchGlob(pattern, k) {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) > count {
		keys = keys[:count]
	}
	items := make([]KeyValue, 0, len(keys))
	for _, k := range keys {
		items = append(items, KeyValue{Key: k, Value: f.data[k]})
	}
	next := ""
	if len(items) == count {
		next = items[len(items)-1].Key
	}
	return items, next, nil
}

func (f *FakeEngine) expiredLocked(key string) bool {
	exp, ok := f.expires[key]
	if !ok {
		return false
	}
	return f.now().UnixNano() >= exp
}

// matchGlob reports whether s matches the * glob pattern. '*' matches any run
// of characters (including the empty run).
func matchGlob(pattern, s string) bool {
	if pattern == "" {
		return true
	}
	var pi, si int
	star, mark := -1, 0
	for si < len(s) {
		if pi < len(pattern) && (pattern[pi] == s[si]) {
			pi++
			si++
		} else if pi < len(pattern) && pattern[pi] == '*' {
			star = pi
			mark = si
			pi++
		} else if star != -1 {
			pi = star + 1
			mark++
			si = mark
		} else {
			return false
		}
	}
	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}
	return pi == len(pattern)
}
