package raft

import (
	"testing"

	"github.com/Raj-Jai/kv-store/pkg/storage"
)

func TestMemTransportRoutesToRegisteredHandler(t *testing.T) {
	trans := NewMemTransport()
	node := NewNode("n1", nil, FakeTransport{}, storage.NewFakeEngine())
	trans.Register("n1", node)

	resp, err := trans.RequestVote("n1", VoteRequest{Term: 1, CandidateID: "c"})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.VoteGranted {
		t.Fatal("registered handler should grant the vote")
	}

	ae, err := trans.AppendEntries("n1", AppendEntriesRequest{Term: 1, LeaderID: "n1"})
	if err != nil {
		t.Fatal(err)
	}
	if !ae.Success {
		t.Fatal("expected success response from the registered handler")
	}
}

func TestMemTransportUnknownPeer(t *testing.T) {
	trans := NewMemTransport()

	if _, err := trans.RequestVote("ghost", VoteRequest{}); err != ErrUnknownPeer {
		t.Fatalf("RequestVote err = %v, want ErrUnknownPeer", err)
	}
	if _, err := trans.AppendEntries("ghost", AppendEntriesRequest{}); err != ErrUnknownPeer {
		t.Fatalf("AppendEntries err = %v, want ErrUnknownPeer", err)
	}
}

func TestMemTransportUnregister(t *testing.T) {
	trans := NewMemTransport()
	node := NewNode("n1", nil, FakeTransport{}, storage.NewFakeEngine())
	trans.Register("n1", node)
	trans.Unregister("n1")

	if _, err := trans.RequestVote("n1", VoteRequest{}); err != ErrUnknownPeer {
		t.Fatalf("err = %v, want ErrUnknownPeer after unregister", err)
	}
}
