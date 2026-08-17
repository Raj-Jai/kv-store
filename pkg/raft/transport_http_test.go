package raft

import (
	"net/http/httptest"
	"testing"

	"github.com/Raj-Jai/kv-store/pkg/storage"
)

func TestHTTPServerRoundTrip(t *testing.T) {
	node := NewNode("n1", nil, FakeTransport{}, storage.NewFakeEngine())
	srv := httptest.NewServer(ServeRaftHTTP(node))
	defer srv.Close()

	trans := &HTTPTransport{}

	resp, err := trans.RequestVote(srv.URL, VoteRequest{Term: 3, CandidateID: "c"})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.VoteGranted {
		t.Fatal("expected the vote to be granted over HTTP")
	}

	ae, err := trans.AppendEntries(srv.URL, AppendEntriesRequest{Term: 3, LeaderID: "L"})
	if err != nil {
		t.Fatal(err)
	}
	if !ae.Success {
		t.Fatal("expected AppendEntries success over HTTP")
	}
}

func TestHTTPServerRejectsUnknownPath(t *testing.T) {
	node := NewNode("n1", nil, FakeTransport{}, storage.NewFakeEngine())
	srv := httptest.NewServer(ServeRaftHTTP(node))
	defer srv.Close()

	resp, err := srv.Client().Post(srv.URL+"/nope", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}
