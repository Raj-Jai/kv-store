package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// HTTP transport — Developer B (M1.1). Peers are addressed by full URLs
// (e.g. "http://host:port"); the receiving side is served by ServeRaftHTTP.

const raftRPCTimeout = 2 * time.Second

// HTTPTransport is the outbound RPC client for a node.
type HTTPTransport struct {
	Client *http.Client
}

func (t *HTTPTransport) client() *http.Client {
	if t.Client != nil {
		return t.Client
	}
	return &http.Client{Timeout: raftRPCTimeout}
}

// RequestVote sends a vote request to a peer and decodes the response.
func (t *HTTPTransport) RequestVote(peer string, req VoteRequest) (VoteResponse, error) {
	var resp VoteResponse
	err := t.do(peer, "/raft/vote", req, &resp)
	return resp, err
}

// AppendEntries sends an AppendEntries to a peer and decodes the response.
func (t *HTTPTransport) AppendEntries(peer string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	var resp AppendEntriesResponse
	err := t.do(peer, "/raft/append", req, &resp)
	return resp, err
}

// InstallSnapshot sends an InstallSnapshot to a peer and decodes the response.
func (t *HTTPTransport) InstallSnapshot(peer string, req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	var resp InstallSnapshotResponse
	err := t.do(peer, "/raft/snapshot", req, &resp)
	return resp, err
}

func (t *HTTPTransport) do(peer, path string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("raft: marshal rpc: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, peer+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("raft: build rpc: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client().Do(req)
	if err != nil {
		return fmt.Errorf("raft: rpc %s: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("raft: rpc %s: status %d", path, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("raft: decode rpc: %w", err)
	}
	return nil
}

// ServeRaftHTTP serves the inbound RPC endpoints for a node's handlers. Mount
// it under POST /raft/vote and POST /raft/append.
func ServeRaftHTTP(h RaftHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/raft/vote":
			var req VoteRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			writeRPC(w, h.HandleRequestVote(req))
		case "/raft/append":
			var req AppendEntriesRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			writeRPC(w, h.HandleAppendEntries(req))
		case "/raft/snapshot":
			var req InstallSnapshotRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			writeRPC(w, h.HandleInstallSnapshot(req))
		default:
			http.NotFound(w, r)
		}
	})
}

func writeRPC(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, "encode failed", http.StatusInternalServerError)
	}
}
