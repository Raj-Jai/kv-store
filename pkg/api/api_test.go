package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Raj-Jai/kv-store/pkg/storage"
	"github.com/Raj-Jai/kv-store/pkg/util"
)

func newTestServer() *Server {
	return NewServer(storage.NewMemStore(), util.NewLoggerTo(io.Discard))
}

func doRequest(t *testing.T, s *Server, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, r)
	return rec
}

func TestPutGetDelete(t *testing.T) {
	s := newTestServer()

	if rec := doRequest(t, s, http.MethodPut, "/kv/name", "Harsh"); rec.Code != http.StatusOK {
		t.Fatalf("PUT = %d, want %d", rec.Code, http.StatusOK)
	}

	if rec := doRequest(t, s, http.MethodGet, "/kv/name", ""); rec.Body.String() != "Harsh" {
		t.Fatalf("GET body = %q, want %q", rec.Body.String(), "Harsh")
	}

	if rec := doRequest(t, s, http.MethodDelete, "/kv/name", ""); rec.Code != http.StatusOK {
		t.Fatalf("DELETE = %d, want %d", rec.Code, http.StatusOK)
	}

	if rec := doRequest(t, s, http.MethodGet, "/kv/name", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("GET after delete = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestInvalidKey(t *testing.T) {
	s := newTestServer()
	rec := doRequest(t, s, http.MethodPut, "/kv/bad-key", "x")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestOversizeBody(t *testing.T) {
	s := newTestServer()
	big := strings.Repeat("a", maxBodyBytes+1)
	rec := doRequest(t, s, http.MethodPut, "/kv/big", big)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}
}

func TestCORS(t *testing.T) {
	s := newTestServer()
	r := httptest.NewRequest(http.MethodOptions, "/kv/name", nil)
	r.Header.Set("Origin", "http://localhost:3000")
	r.Header.Set("Access-Control-Request-Method", "PUT")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, r)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("preflight = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want %q", got, "*")
	}
}

func TestMetrics(t *testing.T) {
	s := newTestServer()

	doRequest(t, s, http.MethodPut, "/kv/a", "1")
	doRequest(t, s, http.MethodPut, "/kv/b", "2")
	doRequest(t, s, http.MethodGet, "/kv/a", "")
	doRequest(t, s, http.MethodGet, "/kv/missing", "")

	rec := doRequest(t, s, http.MethodGet, "/metrics", "")
	body := rec.Body.String()

	for _, want := range []string{
		"kvstore_requests_total 4",
		"kvstore_keys_total 2",
		`kvstore_status_total{code="200"}`,
		`kvstore_status_total{code="404"}`,
		"kvstore_latency_seconds_total",
		"kvstore_uptime_seconds",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q in:\n%s", want, body)
		}
	}
}

func TestKeyCountOnOverwrite(t *testing.T) {
	s := newTestServer()

	doRequest(t, s, http.MethodPut, "/kv/name", "first")
	doRequest(t, s, http.MethodPut, "/kv/name", "second")

	rec := doRequest(t, s, http.MethodGet, "/metrics", "")
	if !strings.Contains(rec.Body.String(), "kvstore_keys_total 1") {
		t.Fatalf("expected key count 1 after overwrite, got:\n%s", rec.Body.String())
	}
}