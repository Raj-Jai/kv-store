package storage

import "testing"

// TestNotLeaderError covers the consensus-redirect error surfaced by write
// operations on a follower.
func TestNotLeaderError(t *testing.T) {
	if got := (&NotLeaderError{}).Error(); got != "no leader known" {
		t.Fatalf("Error() = %q, want %q", got, "no leader known")
	}
	if got := (&NotLeaderError{LeaderAddr: "http://a:1"}).Error(); got != "not the leader; leader is http://a:1" {
		t.Fatalf("Error() = %q, want %q", got, "not the leader; leader is http://a:1")
	}
}
