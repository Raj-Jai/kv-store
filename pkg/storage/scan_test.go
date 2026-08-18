package storage

import (
	"fmt"
	"testing"
	"time"
)

func TestMemStoreScanPagination(t *testing.T) {
	s := NewMemStore()
	for i := 0; i < 10; i++ {
		s.Put(fmt.Sprintf("k%02d", i), fmt.Sprintf("v%d", i))
	}

	var got []string
	cursor := ""
	pages := 0
	for {
		items, next, err := s.Scan(cursor, 3, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, it := range items {
			got = append(got, it.Key)
		}
		pages++
		if next == "" {
			break
		}
		cursor = next
		if pages > 10 {
			t.Fatal("scan did not terminate")
		}
	}

	if len(got) != 10 {
		t.Fatalf("scan returned %d keys, want 10", len(got))
	}
	for i := 0; i < 10; i++ {
		if got[i] != fmt.Sprintf("k%02d", i) {
			t.Fatalf("scan key %d = %q", i, got[i])
		}
	}
}

func TestMemStoreScanPattern(t *testing.T) {
	s := NewMemStore()
	s.Put("user:1", "a")
	s.Put("user:2", "b")
	s.Put("post:1", "c")
	s.Put("abc", "d")

	items, next, err := s.Scan("", 10, "user:*")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].Key != "user:1" || items[1].Key != "user:2" {
		t.Fatalf("pattern scan = %+v", items)
	}
	if next != "" {
		t.Fatalf("next = %q, want empty", next)
	}

	items, _, err = s.Scan("", 10, "*1")
	if err != nil || len(items) != 2 || items[0].Key != "post:1" || items[1].Key != "user:1" {
		t.Fatalf("suffix pattern scan = %+v, %v", items, err)
	}
}

func TestMemStoreScanSkipsExpired(t *testing.T) {
	s := NewMemStore()
	now := time.Unix(1_000_000, 0)
	s.now = func() time.Time { return now }

	s.Put("alive", "1")
	s.Put("dead", "1")
	s.Expire("dead", now.Add(time.Hour).UnixNano())

	now = now.Add(2 * time.Hour)
	items, _, err := s.Scan("", 10, "")
	if err != nil || len(items) != 1 || items[0].Key != "alive" {
		t.Fatalf("scan = %+v, %v; want only alive", items, err)
	}
}

func TestMemStoreScanBadCount(t *testing.T) {
	s := NewMemStore()
	if _, _, err := s.Scan("", 0, ""); err == nil {
		t.Fatal("Scan(count=0) = nil, want error")
	}
}

func TestMatchGlob(t *testing.T) {
	cases := []struct {
		pat, s string
		want   bool
	}{
		{"", "anything", true},
		{"*", "", true},
		{"*", "x", true},
		{"a", "a", true},
		{"a", "b", false},
		{"a*", "abc", true},
		{"a*", "cba", false},
		{"*b", "ab", true},
		{"*b", "ba", false},
		{"a*c", "abc", true},
		{"a*c", "ac", true},
		{"a*c", "ab", false},
		{"user:*", "user:1", true},
		{"user:*", "users", false},
		{"*1*", "post:1:x", true},
		{"*1*", "post2", false},
	}
	for _, c := range cases {
		if got := matchGlob(c.pat, c.s); got != c.want {
			t.Fatalf("matchGlob(%q, %q) = %v, want %v", c.pat, c.s, got, c.want)
		}
	}
}

func TestDiskStoreScan(t *testing.T) {
	s, err := OpenDiskStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 5; i++ {
		s.Put(fmt.Sprintf("k%d", i), "v")
	}
	items, next, err := s.Scan("", 2, "k*")
	if err != nil || len(items) != 2 {
		t.Fatalf("scan = %+v, %v", items, err)
	}
	if next != "k1" {
		t.Fatalf("next = %q, want k1", next)
	}
}
