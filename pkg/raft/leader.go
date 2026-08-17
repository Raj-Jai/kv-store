package raft

import (
	"errors"

	"github.com/Raj-Jai/kv-store/pkg/storage"
)

// Engine implementation — Developer A (M1.2). raft.Node wraps a
// storage.Engine and proposes mutations through consensus, so Developer B's
// HTTP handlers keep talking to an Engine unchanged.

// Leader returns the current leader's address, or "" when none is known.
func (n *Node) Leader() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.leaderID == nil {
		return ""
	}
	return *n.leaderID
}

// IsLeader reports whether this node is the current leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == RoleLeader
}

// Get serves reads from the local state machine.
func (n *Node) Get(key string) (string, error) {
	return n.store.Get(key)
}

func (n *Node) Put(key, value string) error {
	return n.propose(storage.Command{Op: storage.OpPut, Key: key, Value: value})
}

func (n *Node) Delete(key string) error {
	return n.propose(storage.Command{Op: storage.OpDelete, Key: key})
}

func (n *Node) Clear() error {
	return n.propose(storage.Command{Op: storage.OpClear})
}

func (n *Node) Close() error {
	n.Stop()
	return n.store.Close()
}

// propose appends a command to the leader's log. It returns
// storage.NotLeaderError{LeaderAddr} when this node is not the leader. For a
// single-node cluster the entry is committed and applied immediately; for a
// multi-node cluster the leader returns once the entry is on its own log —
// replication, commit, and synchronous return-to-client land in M1.3/M1.4.
func (n *Node) propose(cmd storage.Command) error {
	n.mu.Lock()
	if n.role != RoleLeader {
		addr := ""
		if n.leaderID != nil {
			addr = *n.leaderID
		}
		n.mu.Unlock()
		return &storage.NotLeaderError{LeaderAddr: addr}
	}
	n.log = append(n.log, Entry{Term: n.term, Cmd: cmd})
	singleNode := len(n.peers) == 0
	if singleNode {
		n.commitIndex = len(n.log)
	}
	n.mu.Unlock()

	if singleNode {
		return n.applyCmd(cmd)
	}
	return nil
}

// applyCmd executes a single command against the local state machine. The
// general commit-index apply loop is Developer B's M1.4 deliverable.
func (n *Node) applyCmd(cmd storage.Command) error {
	switch cmd.Op {
	case storage.OpPut:
		return n.store.Put(cmd.Key, cmd.Value)
	case storage.OpDelete:
		return n.store.Delete(cmd.Key)
	case storage.OpClear:
		return n.store.Clear()
	default:
		return errors.New("raft: unknown command op")
	}
}