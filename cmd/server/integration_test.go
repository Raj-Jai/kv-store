package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Raj-Jai/kv-store/pkg/api"
	"github.com/Raj-Jai/kv-store/pkg/storage"
	"github.com/Raj-Jai/kv-store/pkg/util"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	srv := api.NewServer(store, util.NewLogger())
	return httptest.NewServer(srv.Handler())
}

func request(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(data)
}

func TestHTTPWithDiskStore(t *testing.T) {
	s := newTestServer(t)
	defer s.Close()

	if code, _ := request(t, http.MethodPut, s.URL+"/kv/name", "Harsh"); code != http.StatusOK {
		t.Fatalf("PUT = %d, want 200", code)
	}
	if code, body := request(t, http.MethodGet, s.URL+"/kv/name", ""); code != http.StatusOK || body != "Harsh" {
		t.Fatalf("GET = %d %q, want 200 Harsh", code, body)
	}
	if code, _ := request(t, http.MethodGet, s.URL+"/kv/nope", ""); code != http.StatusNotFound {
		t.Fatalf("GET missing = %d, want 404", code)
	}
	if code, _ := request(t, http.MethodDelete, s.URL+"/kv/name", ""); code != http.StatusOK {
		t.Fatalf("DELETE = %d, want 200", code)
	}
	if code, _ := request(t, http.MethodGet, s.URL+"/kv/name", ""); code != http.StatusNotFound {
		t.Fatalf("GET after delete = %d, want 404", code)
	}
}