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
}

// ErrNotFound is returned by Get when the key does not exist.
var ErrNotFound = errors.New("key not found")

// ErrClosed is returned by write operations after the store is closed.
var ErrClosed = errors.New("store closed")

// Op identifies a storage mutation.
type Op uint8

const (
	OpPut Op = iota + 1
	OpDelete
	OpClear
)

// Command is a serializable storage mutation carried by the consensus log.
type Command struct {
	Op    Op
	Key   string
	Value string
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
