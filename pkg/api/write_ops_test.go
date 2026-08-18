package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestExpireValidation(t *testing.T) {
	s := newTestServer()

	if rec := doRequest(t, s, http.MethodPut, "/kv/k/expire", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("missing ttl = %d, want 400", rec.Code)
	}
	if rec := doRequest(t, s, http.MethodPut, "/kv/k/expire?ttl=abc", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad ttl = %d, want 400", rec.Code)
	}
	if rec := doRequest(t, s, http.MethodPut, "/kv/k/expire?ttl=0", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("zero ttl = %d, want 400", rec.Code)
	}
	if rec := doRequest(t, s, http.MethodPut, "/kv/k/expire?ttl=-5", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("negative ttl = %d, want 400", rec.Code)
	}
	if rec := doRequest(t, s, http.MethodPut, "/kv/k/expire?ttl=99999999999999999999999", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("overflow ttl = %d, want 400", rec.Code)
	}
	if rec := doRequest(t, s, http.MethodPut, "/kv/missing/expire?ttl=1000", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("expire missing key = %d, want 404", rec.Code)
	}
}

func TestExpireAndTTLEnforced(t *testing.T) {
	s := newTestServer()

	if rec := doRequest(t, s, http.MethodPut, "/kv/k", "v"); rec.Code != http.StatusOK {
		t.Fatal("seed PUT failed")
	}
	if rec := doRequest(t, s, http.MethodPut, "/kv/k/expire?ttl=20000000", ""); rec.Code != http.StatusOK {
		t.Fatalf("expire = %d, want 200", rec.Code)
	}
	if rec := doRequest(t, s, http.MethodGet, "/kv/k", ""); rec.Code != http.StatusOK {
		t.Fatalf("GET before ttl = %d, want 200", rec.Code)
	}

	time.Sleep(60 * time.Millisecond)
	if rec := doRequest(t, s, http.MethodGet, "/kv/k", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("GET after ttl = %d, want 404", rec.Code)
	}
}

func TestIncr(t *testing.T) {
	s := newTestServer()

	if rec := doRequest(t, s, http.MethodPut, "/kv/n", "5"); rec.Code != http.StatusOK {
		t.Fatal("seed PUT failed")
	}
	rec := doRequest(t, s, http.MethodPost, "/kv/n/incr", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("incr = %d, want 200", rec.Code)
	}
	var resp struct {
		Key   string `json:"key"`
		Value int64  `json:"value"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Value != 6 {
		t.Fatalf("incr value = %d, want 6", resp.Value)
	}
}

func TestIncrMissingStartsAtOne(t *testing.T) {
	s := newTestServer()
	rec := doRequest(t, s, http.MethodPost, "/kv/fresh/incr", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("incr = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"value":1`) {
		t.Fatalf("incr response = %s, want value 1", rec.Body.String())
	}
}

func TestIncrNonNumericReturns422(t *testing.T) {
	s := newTestServer()
	doRequest(t, s, http.MethodPut, "/kv/txt", "hello")
	if rec := doRequest(t, s, http.MethodPost, "/kv/txt/incr", ""); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("incr non-numeric = %d, want 422", rec.Code)
	}
}

func TestCAS(t *testing.T) {
	s := newTestServer()
	doRequest(t, s, http.MethodPut, "/kv/k", "a")

	if rec := doRequest(t, s, http.MethodPut, "/kv/k/cas", `{"old":"x","new":"z"}`); rec.Code != http.StatusConflict {
		t.Fatalf("cas mismatch = %d, want 409", rec.Code)
	}
	if rec := doRequest(t, s, http.MethodPut, "/kv/k/cas", `{"old":"a","new":"b"}`); rec.Code != http.StatusOK {
		t.Fatalf("cas match = %d, want 200", rec.Code)
	}
	if rec := doRequest(t, s, http.MethodGet, "/kv/k", ""); rec.Body.String() != "b" {
		t.Fatalf("value after cas = %q, want b", rec.Body.String())
	}
}

func TestCASValidation(t *testing.T) {
	s := newTestServer()
	if rec := doRequest(t, s, http.MethodPut, "/kv/k/cas", "not-json"); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad json = %d, want 400", rec.Code)
	}
	if rec := doRequest(t, s, http.MethodPut, "/kv/missing/cas", `{"old":"a","new":"b"}`); rec.Code != http.StatusNotFound {
		t.Fatalf("cas missing = %d, want 404", rec.Code)
	}
}

func TestMetricsExpiredCounter(t *testing.T) {
	s := newTestServer()
	doRequest(t, s, http.MethodPut, "/kv/k", "v")
	doRequest(t, s, http.MethodPut, "/kv/k/expire?ttl=10000000", "") // 10ms
	time.Sleep(30 * time.Millisecond)
	doRequest(t, s, http.MethodGet, "/kv/k", "") // lazy expiry bumps the counter

	rec := doRequest(t, s, http.MethodGet, "/metrics", "")
	if !strings.Contains(rec.Body.String(), "kvstore_expired_keys_total 1") {
		t.Fatalf("expected expired counter 1 in:\n%s", rec.Body.String())
	}
}
