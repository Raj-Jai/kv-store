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

func TestClientGet(t *testing.T) {
	gotKey := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.URL.Path
		io.WriteString(w, "value")
	}))
	defer srv.Close()

	resp, err := New(srv.URL).Get("k")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "value" {
		t.Fatalf("body = %q, want %q", body, "value")
	}
	if gotKey != "/kv/k" {
		t.Fatalf("path = %q, want %q", gotKey, "/kv/k")
	}
}

func TestClientDelete(t *testing.T) {
	gotMethod, gotPath := "", ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	resp, err := New(srv.URL).Delete("k")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if gotMethod != http.MethodDelete || gotPath != "/kv/k" {
		t.Fatalf("saw %s %s, want DELETE /kv/k", gotMethod, gotPath)
	}
}

func TestClientGetNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "key not found", http.StatusNotFound)
	}))
	defer srv.Close()

	resp, err := New(srv.URL).Get("missing")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

// TestClientRejectsControlCharKey covers the NewRequest error path: a key with
// an ASCII control character cannot be encoded into a valid URL.
func TestClientRejectsControlCharKey(t *testing.T) {
	c := New("http://example.com")
	if _, err := c.Put("a\x00b", "v"); err == nil {
		t.Fatal("expected error for a control-character key")
	}
	if _, err := c.Delete("a\x00b"); err == nil {
		t.Fatal("expected error for a control-character key")
	}
}
