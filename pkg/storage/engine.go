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
