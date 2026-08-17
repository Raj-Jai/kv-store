package raft

// Transport is the seam between the outbound engine (Developer A) and the
// inbound handlers (Developer B). Nobody edits a Transport implementation and
// the election/replication engine in the same milestone.
//
// peers are addressed by their full transport address (e.g. "http://host:port"
// for the HTTP transport, or a node id for the in-memory transport).
type Transport interface {
	RequestVote(peer string, req VoteRequest) (VoteResponse, error)
	AppendEntries(peer string, req AppendEntriesRequest) (AppendEntriesResponse, error)
}

// FakeTransport is the contract fake: every call succeeds with a benign
// response so both developers can build against it before a real transport
// lands.
type FakeTransport struct{}

func (FakeTransport) RequestVote(peer string, req VoteRequest) (VoteResponse, error) {
	return VoteResponse{Term: req.Term, VoteGranted: true}, nil
}

func (FakeTransport) AppendEntries(peer string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	return AppendEntriesResponse{Term: req.Term, Success: true}, nil
}
