package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestScanPagination(t *testing.T) {
	s := newTestServer()
	for i := 0; i < 5; i++ {
		doRequest(t, s, http.MethodPut, "/kv/k"+string(rune('0'+i)), "v")
	}

	var all []string
	cursor := ""
	for i := 0; i < 10; i++ {
		path := "/kv?count=2"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		rec := doRequest(t, s, http.MethodGet, path, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("scan = %d, want 200", rec.Code)
		}
		var resp struct {
			Items []struct {
				Key string `json:"key"`
			} `json:"items"`
			Cursor string `json:"cursor"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		for _, it := range resp.Items {
			all = append(all, it.Key)
		}
		if resp.Cursor == "" {
			break
		}
		cursor = resp.Cursor
	}

	if len(all) != 5 {
		t.Fatalf("scan returned %d keys, want 5", len(all))
	}
}

func TestScanPattern(t *testing.T) {
	s := newTestServer()
	doRequest(t, s, http.MethodPut, "/kv/user:1", "a")
	doRequest(t, s, http.MethodPut, "/kv/user:2", "b")
	doRequest(t, s, http.MethodPut, "/kv/post:1", "c")

	rec := doRequest(t, s, http.MethodGet, "/kv?pattern=user:*", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("scan = %d, want 200", rec.Code)
	}
	var resp struct {
		Items []struct {
			Key string `json:"key"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("pattern scan items = %+v, want 2", resp.Items)
	}
}

func TestScanValidation(t *testing.T) {
	s := newTestServer()
	if rec := doRequest(t, s, http.MethodGet, "/kv?count=0", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("count=0 = %d, want 400", rec.Code)
	}
	if rec := doRequest(t, s, http.MethodGet, "/kv?count=1001", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("count=1001 = %d, want 400", rec.Code)
	}
	longPattern := strings.Repeat("x", maxPatternLen+1)
	if rec := doRequest(t, s, http.MethodGet, "/kv?pattern="+longPattern, ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("long pattern = %d, want 400", rec.Code)
	}
}
