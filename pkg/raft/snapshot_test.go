package raft

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Raj-Jai/kv-store/pkg/storage"
)

type fixedSnapshotter struct {
	snap Snapshot
}

func (s fixedSnapshotter) Snapshot() (Snapshot, error) { return s.snap, nil }

type recSink struct {
	mu   sync.Mutex
	data []byte
}

func (s *recSink) ApplySnapshot(data []byte) error {
	s.mu.Lock()
	s.data = append([]byte(nil), data...)
	s.mu.Unlock()
	return nil
}

func (s *recSink) got() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.data...)
}

func TestCompactLogTrimsAndTracksBase(t *testing.T) {
	trans := &followerTransport{followers: map[string]*testFollower{"f1": {}}}
	n := NewNode("leader", []string{"f1"}, trans, storage.NewFakeEngine())
	n.mu.Lock()
	n.becomeLeader()
	n.mu.Unlock()
	preload(n, 2, 5)

	n.CompactLog(3, 2)

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.lastIncludedIndex != 3 || n.lastIncludedTerm != 2 {
		t.Fatalf("base not recorded: %d/%d", n.lastIncludedIndex, n.lastIncludedTerm)
	}
	if n.firstIndex() != 4 || n.lastLogIndex() != 5 || n.lastLogTerm() != 2 {
		t.Fatalf("index math off: first=%d last=%d term=%d", n.firstIndex(), n.lastLogIndex(), n.lastLogTerm())
	}
	if len(n.log) != 2 {
		t.Fatalf("expected 2 retained entries, got %d", len(n.log))
	}
	if _, ok := n.entryAt(3); ok {
		t.Fatal("compacted entry should not be reachable")
	}
	if e, ok := n.entryAt(4); !ok || e.Cmd.Key != "k3" {
		t.Fatalf("entry at 4 wrong: %+v ok=%v", e, ok)
	}
}

func TestCompactLogIdempotent(t *testing.T) {
	trans := &followerTransport{followers: map[string]*testFollower{"f1": {}}}
	n := NewNode("leader", []string{"f1"}, trans, storage.NewFakeEngine())
	n.mu.Lock()
	n.becomeLeader()
	n.mu.Unlock()
	preload(n, 2, 5)

	n.CompactLog(3, 2)
	n.CompactLog(3, 2) // second call must be a no-op
	n.CompactLog(9, 99)

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.lastIncludedIndex != 9 || len(n.log) != 0 {
		t.Fatalf("expected full compaction to base 9, got base=%d len=%d", n.lastIncludedIndex, len(n.log))
	}
	if n.lastLogIndex() != 9 || n.lastLogTerm() != 2 {
		t.Fatalf("full compaction should keep lastLogTerm, got last=%d term=%d", n.lastLogIndex(), n.lastLogTerm())
	}
}

func TestInstallSnapshotToLaggingFollower(t *testing.T) {
	follower := &testFollower{}
	trans := &followerTransport{followers: map[string]*testFollower{"f1": follower}}
	n := NewNode("leader", []string{"f1"}, trans, storage.NewFakeEngine())
	n.SetSnapshotter(fixedSnapshotter{snap: Snapshot{
		LastIncludedIndex: 3,
		LastIncludedTerm:  2,
		Data:              []byte("snapdata"),
	}})
	n.mu.Lock()
	n.becomeLeader()
	n.mu.Unlock()
	preload(n, 2, 5)
	n.CompactLog(3, 2)

	// The follower is empty and needs raft index 1, which is compacted away:
	// the leader must install the snapshot first, then stream entries 4-5.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n.sendHeartbeats()
		if len(follower.snapshots) == 1 && len(follower.snapshot()) == 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if len(follower.snapshots) != 1 {
		t.Fatalf("expected 1 snapshot install, got %d", len(follower.snapshots))
	}
	if !bytes.Equal(follower.snapshots[0], []byte("snapdata")) {
		t.Fatalf("snapshot data mismatch: %q", follower.snapshots[0])
	}
	if follower.lastIncludedIndex != 3 {
		t.Fatalf("follower base not adopted: %d", follower.lastIncludedIndex)
	}
	got := follower.snapshot()
	if len(got) != 2 || got[0].Cmd.Key != "k3" || got[1].Cmd.Key != "k4" {
		t.Fatalf("follower did not receive entries after snapshot: %+v", got)
	}

	// The follower must be marked caught up to the snapshot base.
	if next, match := replIndexes(n, "f1"); next != 6 || match != 5 {
		t.Fatalf("expected next=6 match=5, got next=%d match=%d", next, match)
	}
}

func TestCaughtUpFollowerGetsNoSnapshot(t *testing.T) {
	follower := &testFollower{}
	trans := &followerTransport{followers: map[string]*testFollower{"f1": follower}}
	n := NewNode("leader", []string{"f1"}, trans, storage.NewFakeEngine())
	n.SetSnapshotter(fixedSnapshotter{snap: Snapshot{LastIncludedIndex: 3, LastIncludedTerm: 2}})
	n.mu.Lock()
	n.becomeLeader()
	n.mu.Unlock()
	preload(n, 2, 5)

	// Follower is fully caught up (nextIndex starts at 6); replication must
	// use AppendEntries, never a snapshot.
	for i := 0; i < 5; i++ {
		n.sendHeartbeats()
	}
	if len(follower.snapshots) != 0 {
		t.Fatalf("caught-up follower should not receive snapshots")
	}
}

func TestHandleInstallSnapshotAdoptsBaseAndAppliesData(t *testing.T) {
	sink := &recSink{}
	n := NewNode("a", []string{"p1"}, FakeTransport{}, storage.NewFakeEngine())
	n.SetSnapshotSink(sink)
	n.mu.Lock()
	n.term = 4
	n.mu.Unlock()

	resp := n.HandleInstallSnapshot(InstallSnapshotRequest{
		Term:              5,
		LeaderID:          "L",
		LastIncludedIndex: 3,
		LastIncludedTerm:  2,
		Data:              []byte("state"),
	})
	if !resp.Success {
		t.Fatal("expected snapshot accepted")
	}
	n.mu.Lock()
	if n.lastIncludedIndex != 3 || n.lastIncludedTerm != 2 || n.term != 5 {
		t.Fatalf("base/term not adopted: idx=%d term=%d nterm=%d", n.lastIncludedIndex, n.lastIncludedTerm, n.term)
	}
	if n.role != RoleFollower || n.leaderID == nil || *n.leaderID != "L" {
		t.Fatal("node should be follower of L")
	}
	n.mu.Unlock()
	if !bytes.Equal(sink.got(), []byte("state")) {
		t.Fatalf("snapshot data not applied: %q", sink.got())
	}
}

func TestHandleInstallSnapshotTrimsCoveredEntries(t *testing.T) {
	n := NewNode("a", []string{"p1"}, FakeTransport{}, storage.NewFakeEngine())
	n.mu.Lock()
	n.term = 1
	n.log = []Entry{
		{Term: 1, Cmd: storage.Command{Op: storage.OpPut, Key: "k1"}},
		{Term: 1, Cmd: storage.Command{Op: storage.OpPut, Key: "k2"}},
		{Term: 1, Cmd: storage.Command{Op: storage.OpPut, Key: "k3"}},
	}
	n.mu.Unlock()

	resp := n.HandleInstallSnapshot(InstallSnapshotRequest{
		Term:              2,
		LeaderID:          "L",
		LastIncludedIndex: 2,
		LastIncludedTerm:  1,
	})
	if !resp.Success {
		t.Fatal("expected snapshot accepted")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.log) != 1 || n.log[0].Cmd.Key != "k3" {
		t.Fatalf("covered entries not trimmed: %+v", n.log)
	}
	if n.lastLogIndex() != 3 {
		t.Fatalf("expected lastLogIndex 3, got %d", n.lastLogIndex())
	}
}

// TestHandleInstallSnapshotStaleSnapshotIgnored guards against rolling the
// state machine back: a snapshot whose base is at or behind the follower's
// applied index (a duplicate, reordered, or delayed copy of one the node
// already caught up past) must be acknowledged but must NOT restore its data
// or trim the log.
func TestHandleInstallSnapshotStaleSnapshotIgnored(t *testing.T) {
	sink := &recSink{}
	n := NewNode("a", []string{"p1"}, FakeTransport{}, storage.NewFakeEngine())
	n.SetSnapshotSink(sink)
	n.mu.Lock()
	// The node has already applied through index 100.
	n.lastApplied = 100
	n.lastIncludedIndex = 0
	n.log = []Entry{
		{Term: 2, Cmd: storage.Command{Op: storage.OpPut, Key: "k99"}},
		{Term: 2, Cmd: storage.Command{Op: storage.OpPut, Key: "k100"}},
	}
	n.mu.Unlock()

	resp := n.HandleInstallSnapshot(InstallSnapshotRequest{
		Term:              3,
		LeaderID:          "L",
		LastIncludedIndex: 50,
		LastIncludedTerm:  2,
		Data:              []byte("state-at-50"),
	})
	if !resp.Success {
		t.Fatal("a stale snapshot must still be acknowledged so the leader advances")
	}
	n.mu.Lock()
	if n.lastIncludedIndex != 0 {
		t.Fatalf("stale snapshot must not advance the base, got %d", n.lastIncludedIndex)
	}
	if len(n.log) != 2 {
		t.Fatalf("stale snapshot must not trim the log, got %d entries", len(n.log))
	}
	if n.lastApplied != 100 {
		t.Fatalf("stale snapshot must not move lastApplied, got %d", n.lastApplied)
	}
	n.mu.Unlock()
	if got := sink.got(); got != nil {
		t.Fatalf("stale snapshot must not apply data, got %q", got)
	}
}

// blockingEngine is a storage.Engine whose Put signals entry and then blocks
// until release is closed, so a test can hold the apply loop mid-apply. It
// also implements SnapshotSink: ApplySnapshot replaces the whole store, which
// is what the storage side does on restore. putDone fires after a released
// Put has actually written, and restores counts how many restores ran.
type blockingEngine struct {
	mu       sync.Mutex
	kv       map[string]string
	entered  chan struct{}
	release  chan struct{}
	putDone  chan struct{}
	restores int
}

func newBlockingEngine() *blockingEngine {
	return &blockingEngine{
		kv:      map[string]string{},
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
		putDone: make(chan struct{}, 1),
	}
}

func (e *blockingEngine) Get(key string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	v, ok := e.kv[key]
	if !ok {
		return "", storage.ErrNotFound
	}
	return v, nil
}

func (e *blockingEngine) Put(key, value string) error {
	select {
	case e.entered <- struct{}{}:
	default:
	}
	<-e.release
	e.mu.Lock()
	e.kv[key] = value
	e.mu.Unlock()
	select {
	case e.putDone <- struct{}{}:
	default:
	}
	return nil
}

func (e *blockingEngine) Delete(key string) error          { return nil }
func (e *blockingEngine) Clear() error                     { return nil }
func (e *blockingEngine) Close() error                     { return nil }
func (e *blockingEngine) Incr(string) (int64, error)       { return 0, nil }
func (e *blockingEngine) CAS(_, _, _ string) (bool, error) { return false, nil }
func (e *blockingEngine) Expire(string, int64) error       { return nil }
func (e *blockingEngine) Scan(string, int, string) ([]storage.KeyValue, string, error) {
	return nil, "", nil
}

func (e *blockingEngine) ApplySnapshot(data []byte) error {
	e.mu.Lock()
	e.kv = map[string]string{"k": "from-snapshot"}
	e.restores++
	e.mu.Unlock()
	return nil
}

func (e *blockingEngine) restoreCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.restores
}

// TestApplyLoopSnapshotRestoreNeverInterleaves reproduces the interleaving
// that rolled acked writes back: the apply loop picks entry 1 and blocks
// inside Put while a snapshot covering entry 1 arrives. The restore must be
// serialized against the in-flight apply; otherwise the restore sets the store
// to the snapshot state and the just-released Put then resurrects the older
// value ("v1"), which a reader observes as a rolled-back write.
func TestApplyLoopSnapshotRestoreNeverInterleaves(t *testing.T) {
	eng := newBlockingEngine()
	n := NewNode("a", []string{"p1"}, FakeTransport{}, eng)
	n.SetSnapshotSink(eng)
	n.mu.Lock()
	n.term = 1
	n.log = []Entry{
		{Term: 1, Cmd: storage.Command{Op: storage.OpPut, Key: "k", Value: "v1"}},
		{Term: 1, Cmd: storage.Command{Op: storage.OpPut, Key: "k", Value: "v2"}},
	}
	n.commitIndex = 2
	n.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n.StartApply(ctx)

	// The apply loop has picked entry 1 and is blocked in Put, holding
	// applyMu. A snapshot covering entries 1-2 now arrives.
	<-eng.entered
	done := make(chan InstallSnapshotResponse, 1)
	go func() {
		done <- n.HandleInstallSnapshot(InstallSnapshotRequest{
			Term:              2,
			LeaderID:          "L",
			LastIncludedIndex: 2,
			LastIncludedTerm:  1,
			Data:              []byte("state-at-2"),
		})
	}()

	// With the serialization the restore is excluded against the in-flight
	// apply, so it cannot run while Put is blocked. Without it, the restore
	// runs immediately. Poll for the restore so that, on a pre-fix build, the
	// restore is guaranteed to have landed before the apply's Put is released
	// (which would then resurrect the older value). On the fixed build nothing
	// fires in this window, so we release the apply and let the restore run
	// after it.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && eng.restoreCount() == 0 {
		time.Sleep(time.Millisecond)
	}
	close(eng.release) // let the in-flight apply finish
	<-eng.putDone      // its write has landed

	select {
	case resp := <-done:
		if !resp.Success {
			t.Fatal("expected snapshot accepted")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot handler did not return")
	}

	// The restore must be the final writer: the in-flight apply's older value
	// must never win after a restore.
	if got, _ := eng.Get("k"); got != "from-snapshot" {
		t.Fatalf("store rolled back to %q after snapshot restore", got)
	}
}

func TestHandleInstallSnapshotLowerTermRejected(t *testing.T) {
	n := NewNode("a", []string{"p1"}, FakeTransport{}, storage.NewFakeEngine())
	n.mu.Lock()
	n.term = 5
	n.mu.Unlock()
	resp := n.HandleInstallSnapshot(InstallSnapshotRequest{Term: 4, LeaderID: "L"})
	if resp.Success || resp.Term != 5 {
		t.Fatalf("expected rejection with term 5, got %+v", resp)
	}
}

// snapshotHigherTermTransport rejects AppendEntries at the same term (forcing
// the leader to rewind its nextIndex below the compaction base) and replies
// with a higher term only to InstallSnapshot — so the leader steps down
// specifically because of the snapshot exchange.
type snapshotHigherTermTransport struct {
	snapTerm int
}

func (snapshotHigherTermTransport) RequestVote(_ string, req VoteRequest) (VoteResponse, error) {
	return VoteResponse{Term: req.Term, VoteGranted: true}, nil
}

func (snapshotHigherTermTransport) AppendEntries(_ string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	return AppendEntriesResponse{Term: req.Term, Success: false}, nil
}

func (t snapshotHigherTermTransport) InstallSnapshot(_ string, req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	return InstallSnapshotResponse{Term: t.snapTerm, Success: false}, nil
}

func TestLeaderStepsDownOnHigherTermSnapshot(t *testing.T) {
	n := NewNode("leader", []string{"f1"}, snapshotHigherTermTransport{snapTerm: 99}, storage.NewFakeEngine())
	n.SetSnapshotter(fixedSnapshotter{snap: Snapshot{LastIncludedIndex: 3, LastIncludedTerm: 2}})
	n.mu.Lock()
	n.becomeLeader()
	n.mu.Unlock()
	preload(n, 2, 5)
	n.CompactLog(3, 2)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n.sendHeartbeats()
		n.mu.Lock()
		if n.role == RoleFollower && n.term == 99 {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	t.Fatalf("expected step-down to follower at 99, got role=%v term=%d", n.role, n.term)
}
