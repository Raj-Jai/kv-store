package chaos

import (
	"errors"
	"time"

	"github.com/Raj-Jai/kv-store/pkg/raft"
)

// fault-injecting transport — Developer A (Phase 2). Each node in the cluster
// is given its own faultTransport that wraps the shared base transport and
// knows its own id, so directional edge faults can be applied per sender.

var errDropped = errors.New("chaos: message dropped")

type faultTransport struct {
	id   string
	base raft.Transport
	cfg  *FaultConfig
}

// RequestVote routes a vote request through the fault model.
func (t *faultTransport) RequestVote(peer string, req raft.VoteRequest) (raft.VoteResponse, error) {
	if drop, lat := t.cfg.route(t.id, peer); drop {
		return raft.VoteResponse{}, errDropped
	} else if lat > 0 {
		time.Sleep(lat)
	}
	if t.cfg.shouldDuplicate() {
		t.base.RequestVote(peer, req) // duplicate delivery is idempotent on the follower
	}
	return t.base.RequestVote(peer, req)
}

// AppendEntries routes an append through the fault model.
func (t *faultTransport) AppendEntries(peer string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	if drop, lat := t.cfg.route(t.id, peer); drop {
		return raft.AppendEntriesResponse{}, errDropped
	} else if lat > 0 {
		time.Sleep(lat)
	}
	if t.cfg.shouldDuplicate() {
		t.base.AppendEntries(peer, req)
	}
	return t.base.AppendEntries(peer, req)
}

// InstallSnapshot routes a snapshot through the fault model.
func (t *faultTransport) InstallSnapshot(peer string, req raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	if drop, lat := t.cfg.route(t.id, peer); drop {
		return raft.InstallSnapshotResponse{}, errDropped
	} else if lat > 0 {
		time.Sleep(lat)
	}
	if t.cfg.shouldDuplicate() {
		t.base.InstallSnapshot(peer, req)
	}
	return t.base.InstallSnapshot(peer, req)
}

// faultStore wraps a durable raft store so Save (fsync) failures can be
// injected. Load always succeeds so restarts are unaffected.
type faultStore struct {
	raft.RaftStore
	id  string
	cfg *FaultConfig
}

func (s *faultStore) Save(st raft.RaftState) error {
	if s.cfg.fsyncFails(s.id) {
		return errDropped
	}
	return s.RaftStore.Save(st)
}
