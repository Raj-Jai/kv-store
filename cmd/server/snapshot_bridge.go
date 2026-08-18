package main

import (
	"context"
	"time"

	"github.com/Raj-Jai/kv-store/pkg/raft"
	"github.com/Raj-Jai/kv-store/pkg/storage"
	"github.com/Raj-Jai/kv-store/pkg/util"
)

// snapshotBridge adapts the storage engine to raft's snapshot provider and
// sink (Developer B, M1.6). On the leader it serializes the DiskStore's
// memory for lagging followers; on a follower it restores that data before
// the leader's compaction base is adopted.
type snapshotBridge struct {
	node  *raft.Node
	store *storage.DiskStore
}

// Snapshot implements raft.SnapshotProvider.
func (b *snapshotBridge) Snapshot() (raft.Snapshot, error) {
	idx := b.node.ApplyIndex()
	term := b.node.LogTerm(idx)
	data, err := b.store.SerializeSnapshot()
	if err != nil {
		return raft.Snapshot{}, err
	}
	return raft.Snapshot{
		LastIncludedIndex: idx,
		LastIncludedTerm:  term,
		Data:              data,
	}, nil
}

// ApplySnapshot implements raft.SnapshotSink.
func (b *snapshotBridge) ApplySnapshot(data []byte) error {
	return b.store.RestoreSnapshot(data)
}

// startSnapshotCompactor periodically compacts the storage WAL and the raft
// log once the applied log has grown by threshold entries, so neither grows
// unbounded. Storage is compacted before the raft log is trimmed: a crash in
// between leaves the raft base behind the storage snapshot, which recovers
// idempotently.
func startSnapshotCompactor(ctx context.Context, b *snapshotBridge, logger *util.Logger, threshold int) {
	if threshold <= 0 {
		threshold = snapshotCompactThreshold
	}
	go func() {
		ticker := time.NewTicker(snapshotCompactorInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if b.node.ApplyIndex()-b.node.SnapshotBase() < threshold {
					continue
				}
				idx := b.node.ApplyIndex()
				term := b.node.LogTerm(idx)
				if err := b.store.Compact(); err != nil {
					logger.Error("storage compaction failed", map[string]any{"error": err.Error()})
					continue
				}
				b.node.CompactLog(idx, term)
				if err := b.node.Flush(); err != nil {
					logger.Error("raft compaction base flush failed", map[string]any{"error": err.Error()})
				}
			}
		}
	}()
}
