package client

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestClientFollowsLeaderRedirect(t *testing.T) {
	var (
		mu    sync.Mutex
		value string
	)

	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		value = string(body)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer leader.Close()

	// A follower node: 307-redirects every write to the leader.
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, leader.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer follower.Close()

	c := New(follower.URL)
	resp, err := c.Put("k", "hello")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("final status = %d, want 200", resp.StatusCode)
	}

	mu.Lock()
	defer mu.Unlock()
	if value != "hello" {
		t.Fatalf("leader received %q, want %q", value, "hello")
	}
}
