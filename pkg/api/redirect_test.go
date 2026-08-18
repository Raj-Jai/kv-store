package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Raj-Jai/kv-store/pkg/storage"
)

// notLeaderEngine returns NotLeaderError for every write, so the redirect
// behavior can be tested without a real cluster.
type notLeaderEngine struct{ addr string }

func (e *notLeaderEngine) Get(key string) (string, error) { return "", storage.ErrNotFound }
func (e *notLeaderEngine) Put(key, value string) error {
	return &storage.NotLeaderError{LeaderAddr: e.addr}
}
func (e *notLeaderEngine) Delete(key string) error {
	return &storage.NotLeaderError{LeaderAddr: e.addr}
}
func (e *notLeaderEngine) Clear() error { return nil }
func (e *notLeaderEngine) Close() error { return nil }
func (e *notLeaderEngine) Incr(key string) (int64, error) {
	return 0, &storage.NotLeaderError{LeaderAddr: e.addr}
}
func (e *notLeaderEngine) CAS(key, old, new string) (bool, error) {
	return false, &storage.NotLeaderError{LeaderAddr: e.addr}
}
func (e *notLeaderEngine) Expire(key string, expiresAt int64) error {
	return &storage.NotLeaderError{LeaderAddr: e.addr}
}
func (e *notLeaderEngine) Scan(cursor string, count int, pattern string) ([]storage.KeyValue, string, error) {
	return nil, "", nil
}

func TestPutRedirectsToLeader(t *testing.T) {
	s := NewServer(&notLeaderEngine{addr: "http://leader:8081"}, nil)
	req := httptest.NewRequest(http.MethodPut, "/kv/k", strings.NewReader("v"))
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d, want 307", rr.Code)
	}
	if got := rr.Header().Get("Location"); got != "http://leader:8081/kv/k" {
		t.Fatalf("Location = %q, want %q", got, "http://leader:8081/kv/k")
	}
}

func TestPutNoLeaderReturns503(t *testing.T) {
	s := NewServer(&notLeaderEngine{addr: ""}, nil)
	req := httptest.NewRequest(http.MethodPut, "/kv/k", strings.NewReader("v"))
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
}

func TestDeleteRedirectsToLeader(t *testing.T) {
	s := NewServer(&notLeaderEngine{addr: "http://leader:8081"}, nil)
	req := httptest.NewRequest(http.MethodDelete, "/kv/k", nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d, want 307", rr.Code)
	}
	if got := rr.Header().Get("Location"); got != "http://leader:8081/kv/k" {
		t.Fatalf("Location = %q, want %q", got, "http://leader:8081/kv/k")
	}
}

func TestNewOpsRedirectToLeader(t *testing.T) {
	s := NewServer(&notLeaderEngine{addr: "http://leader:8081"}, nil)

	if rec := doRequest(t, s, http.MethodPost, "/kv/k/incr", ""); rec.Code != http.StatusTemporaryRedirect {
		t.Fatalf("incr = %d, want 307", rec.Code)
	}
	if rec := doRequest(t, s, http.MethodPut, "/kv/k/expire?ttl=1000", ""); rec.Code != http.StatusTemporaryRedirect {
		t.Fatalf("expire = %d, want 307", rec.Code)
	}
	if rec := doRequest(t, s, http.MethodPut, "/kv/k/cas", `{"old":"a","new":"b"}`); rec.Code != http.StatusTemporaryRedirect {
		t.Fatalf("cas = %d, want 307", rec.Code)
	}
}

func TestNewOpsNoLeaderReturns503(t *testing.T) {
	s := NewServer(&notLeaderEngine{addr: ""}, nil)
	if rec := doRequest(t, s, http.MethodPost, "/kv/k/incr", ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("incr = %d, want 503", rec.Code)
	}
}
