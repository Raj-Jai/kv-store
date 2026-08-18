package main

import (
	"bytes"
	"encoding/hex"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/Raj-Jai/kv-store/pkg/api"
	"github.com/Raj-Jai/kv-store/pkg/raft"
	"github.com/Raj-Jai/kv-store/pkg/storage"
	"github.com/Raj-Jai/kv-store/pkg/util"
)

// findStrings returns the paths of files under root that contain needle.
func findStrings(root, needle string) ([]string, error) {
	var hits []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(data, []byte(needle)) {
			hits = append(hits, path)
		}
		return nil
	})
	return hits, err
}

func TestDecodeEncryptionKey(t *testing.T) {
	// 64 hex chars decodes to a 32-byte key.
	hexKey := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	b, err := decodeEncryptionKey(hexKey)
	if err != nil || len(b) != storage.AtRestKeySize {
		t.Fatalf("hex key decode: %v len=%d", err, len(b))
	}
	if want, _ := hex.DecodeString(hexKey); !bytes.Equal(b, want) {
		t.Fatal("hex key decoded wrong")
	}
	// A raw 32-byte string is accepted as-is.
	raw := string(bytes.Repeat([]byte{'x'}, storage.AtRestKeySize))
	if b, err = decodeEncryptionKey(raw); err != nil || len(b) != storage.AtRestKeySize {
		t.Fatalf("raw key decode: %v len=%d", err, len(b))
	}
	// Anything else — wrong length, or hex of the wrong length — is rejected.
	for _, bad := range []string{"", "abc", "0123456789abcdef", "zzzz"} {
		if _, err := decodeEncryptionKey(bad); err == nil {
			t.Fatalf("expected error decoding %q", bad)
		}
	}
}

func TestHealthEndpoints(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.OpenDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	handler := withHealth(api.NewServer(store, util.NewLogger()).Handler(), nil)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	if code, body := request(t, http.MethodGet, srv.URL+"/healthz", ""); code != http.StatusOK || body != "ok\n" {
		t.Fatalf("healthz = %d %q, want 200 ok", code, body)
	}
	if code, body := request(t, http.MethodGet, srv.URL+"/readyz", ""); code != http.StatusOK || body != "ready\n" {
		t.Fatalf("readyz = %d %q, want 200 ready", code, body)
	}
}

func TestHealthReadyzWaitsForLeader(t *testing.T) {
	// A raft follower whose cluster has not elected a leader yet must report
	// 503 from /readyz so orchestrators do not route to it.
	store, err := storage.OpenDiskStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	node := raft.NewNode("a", []string{"http://peer:8081"}, &raft.HTTPTransport{}, store)
	handler := withHealth(api.NewServer(node, util.NewLogger()).Handler(), node)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	if code, _ := request(t, http.MethodGet, srv.URL+"/readyz", ""); code != http.StatusServiceUnavailable {
		t.Fatalf("readyz before leader = %d, want 503", code)
	}
}

func TestEncryptedStoreOverHTTP(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{0x7e}, storage.AtRestKeySize)
	store, err := storage.OpenDiskStoreWithKey(dir, key)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	srv := httptest.NewServer(api.NewServer(store, util.NewLogger()).Handler())
	defer srv.Close()

	if code, _ := request(t, http.MethodPut, srv.URL+"/kv/secret", "classified"); code != http.StatusOK {
		t.Fatalf("PUT = %d, want 200", code)
	}
	if code, body := request(t, http.MethodGet, srv.URL+"/kv/secret", ""); code != http.StatusOK || body != "classified" {
		t.Fatalf("GET = %d %q, want classified", code, body)
	}

	// The value must not appear in plaintext anywhere in the data dir.
	walk := func() {
		t.Helper()
		matches, err := findStrings(dir, "classified")
		if err != nil {
			t.Fatal(err)
		}
		if len(matches) > 0 {
			t.Fatalf("plaintext value leaked on disk: %v", matches)
		}
	}
	walk()

	// Reopen with the same key and confirm the value survives a restart.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = storage.OpenDiskStoreWithKey(dir, key)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if v, err := store.Get("secret"); err != nil || v != "classified" {
		t.Fatalf("after reopen = %q, %v", v, err)
	}
}
