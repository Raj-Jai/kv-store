package raft

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// Raft state persistence — Developer A (M1.5). currentTerm, votedFor, the
// log, and the compaction base are written and fsynced before any RPC
// response that depends on them, so a crash mid-decision cannot resurrect an
// old term or a second vote in the same term.
//
// Known simplification: Save rewrites the whole raft state on every change
// (copy + marshal + two fsyncs). Correct but not cheap; batching this into
// an append-only raft log is future work.

// RaftState is the durable subset of a node's raft state.
type RaftState struct {
	Term              int
	VotedFor          *string
	Log               []Entry
	LastIncludedIndex int
	LastIncludedTerm  int
}

// RaftStore is the durability seam for raft state.
type RaftStore interface {
	Save(RaftState) error
	Load() (RaftState, error)
}

// fileRaftStore persists raft state as an atomic JSON file: temp file, fsync,
// rename, fsync the directory.
type fileRaftStore struct {
	path string
}

// NewFileRaftStore creates a RaftStore backed by the given file path.
func NewFileRaftStore(path string) RaftStore {
	return &fileRaftStore{path: path}
}

func (s *fileRaftStore) Save(state RaftState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(s.path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (s *fileRaftStore) Load() (RaftState, error) {
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return RaftState{}, nil
	}
	if err != nil {
		return RaftState{}, err
	}
	var st RaftState
	if err := json.Unmarshal(data, &st); err != nil {
		return RaftState{}, err
	}
	return st, nil
}

// SetRaftStore wires durable raft state storage and restores any previously
// persisted term, vote, log, and compaction base. Call before Loop/StartApply.
func (n *Node) SetRaftStore(rs RaftStore) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.raftStore = rs
	if rs == nil {
		return nil
	}
	st, err := rs.Load()
	if err != nil {
		return err
	}
	n.term = st.Term
	n.votedFor = st.VotedFor
	n.log = append([]Entry(nil), st.Log...)
	n.lastIncludedIndex = st.LastIncludedIndex
	n.lastIncludedTerm = st.LastIncludedTerm
	if n.commitIndex < st.LastIncludedIndex {
		n.commitIndex = st.LastIncludedIndex
	}
	if n.lastApplied < st.LastIncludedIndex {
		n.lastApplied = st.LastIncludedIndex
	}
	return nil
}

// persist writes the current raft state to durable storage, but only when
// something stateful changed since the last save (dirty flag). It is safe to
// call without holding n.mu.
func (n *Node) persist() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.raftStore == nil || !n.dirty {
		return nil
	}
	err := n.raftStore.Save(RaftState{
		Term:              n.term,
		VotedFor:          n.votedFor,
		Log:               append([]Entry(nil), n.log...),
		LastIncludedIndex: n.lastIncludedIndex,
		LastIncludedTerm:  n.lastIncludedTerm,
	})
	if err == nil {
		n.dirty = false
	}
	return err
}
