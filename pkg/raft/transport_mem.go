package raft

import (
	"errors"
	"sync"
)

// In-memory transport — Developer B (M1.1). Deterministic, in-process routing
// for unit tests and the 5-node harness: RPCs dispatch synchronously to the
// handler registered for the peer id, so there is no network timing to
// reason about.

// ErrUnknownPeer is returned when an RPC targets a peer that is not
// registered.
var ErrUnknownPeer = errors.New("raft: unknown peer")

// MemTransport routes RPCs to handlers registered by node id. One instance is
// shared by every node in an in-process cluster.
type MemTransport struct {
	mu       sync.Mutex
	handlers map[string]RaftHandler
}

// NewMemTransport creates an empty in-process transport.
func NewMemTransport() *MemTransport {
	return &MemTransport{handlers: make(map[string]RaftHandler)}
}

// Register binds a node's inbound handlers to its id.
func (m *MemTransport) Register(id string, h RaftHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers[id] = h
}

// Unregister removes a node, simulating a dead or unreachable peer.
func (m *MemTransport) Unregister(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.handlers, id)
}

// RequestVote dispatches a vote request to the peer's handler.
func (m *MemTransport) RequestVote(peer string, req VoteRequest) (VoteResponse, error) {
	h, err := m.handler(peer)
	if err != nil {
		return VoteResponse{}, err
	}
	return h.HandleRequestVote(req), nil
}

// AppendEntries dispatches an AppendEntries to the peer's handler.
func (m *MemTransport) AppendEntries(peer string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	h, err := m.handler(peer)
	if err != nil {
		return AppendEntriesResponse{}, err
	}
	return h.HandleAppendEntries(req), nil
}

func (m *MemTransport) handler(peer string) (RaftHandler, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.handlers[peer]
	if !ok {
		return nil, ErrUnknownPeer
	}
	return h, nil
}
