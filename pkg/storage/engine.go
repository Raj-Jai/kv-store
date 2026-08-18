package storage

import "errors"

// Engine is the storage contract between the API layer and the store.
// The API layer must never depend on a concrete implementation.
type Engine interface {
	Get(key string) (string, error)
	Put(key, value string) error
	Delete(key string) error
	Clear() error
	Close() error

	// Incr atomically increments the base-10 integer stored at key by 1,
	// creating it as "1" when absent, and returns the new value. The value
	// must be a valid base-10 int64, otherwise ErrNotNumeric is returned.
	Incr(key string) (int64, error)
	// CAS swaps key to new only when its current value equals old. It
	// returns (true, nil) on a swap, (false, nil) on a mismatch, and
	// (false, ErrNotFound) when the key does not exist.
	CAS(key, old, new string) (bool, error)
	// Expire sets an absolute expiry deadline (unix nanoseconds) for key.
	// The key is treated as gone once time.Now().UnixNano() >= expiresAt.
	// It returns ErrNotFound when the key does not exist.
	Expire(key string, expiresAt int64) error
	// Scan returns up to count key/value pairs whose keys sort after cursor
	// ("" starts a new scan) and match the * glob pattern. The returned
	// cursor is opaque and must be passed back unchanged; "" means the scan
	// is complete. Expired keys are skipped.
	Scan(cursor string, count int, pattern string) ([]KeyValue, string, error)
}

// KeyValue is one key/value pair returned by Scan.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ErrNotFound is returned by Get when the key does not exist.
var ErrNotFound = errors.New("key not found")

// ErrClosed is returned by write operations after the store is closed.
var ErrClosed = errors.New("store closed")

// ErrNotNumeric is returned by Incr when the stored value is not a base-10
// integer.
var ErrNotNumeric = errors.New("value is not a base-10 integer")

// ErrInvalidCursor is returned by Scan when the cursor is malformed.
var ErrInvalidCursor = errors.New("invalid scan cursor")

// Op identifies a storage mutation.
type Op uint8

const (
	OpPut Op = iota + 1
	OpDelete
	OpClear
	OpIncr
	OpCAS
	OpExpire
)

// Command is a serializable storage mutation carried by the consensus log.
// For OpCAS, Old is the expected value and Value the replacement. For
// OpExpire, ExpiresAt is the absolute deadline in unix nanoseconds. Incr,
// OpCAS and OpExpire are deterministic conditional ops: they are re-evaluated
// at apply time, which is safe because raft guarantees every node applies the
// identical command prefix.
type Command struct {
	Op        Op
	Key       string
	Old       string
	Value     string
	ExpiresAt int64
}

// NotLeaderError is returned by write operations on a non-leader node. It
// carries the current leader's address when one is known.
type NotLeaderError struct {
	LeaderAddr string
}

func (e *NotLeaderError) Error() string {
	if e.LeaderAddr == "" {
		return "no leader known"
	}
	return "not the leader; leader is " + e.LeaderAddr
}
