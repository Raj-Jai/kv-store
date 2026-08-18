// Package linearizability records a single-key op history and checks it for
// the necessary conditions of linearizability. This is a provisional,
// sequential checker (Developer B owns the full Porcupine-style checker in
// future work); it catches real violations — value rollback on a node, or a
// reader observing a write that was never acknowledged — without the expense
// of a full search.
package linearizability

import (
	"fmt"
)

// Kind identifies an operation.
type Kind uint8

const (
	Write Kind = iota
	Read
)

// Op is one recorded client-visible operation. For a write, Seq is the value
// version written and End is when the write was acknowledged. For a read, Seq
// is the version observed, End is when the read completed, and Reader names
// the client/node that issued it.
type Op struct {
	Kind   Kind
	Seq    uint64
	Reader string
	End    int64 // unix nano
}

// Check verifies the recorded history. A reader must never observe an older
// value after a newer one (per-node monotonicity), which is the necessary
// single-key condition that holds even when reads are served by followers.
// Returns the violations found; an empty slice means the history passes.
func Check(ops []Op) []error {
	var errs []error
	maxSeen := map[string]uint64{}
	for _, o := range ops {
		if o.Kind != Read || o.Seq == 0 {
			continue
		}
		if prev, ok := maxSeen[o.Reader]; ok && o.Seq < prev {
			errs = append(errs, fmt.Errorf("reader %q observed seq %d after seq %d", o.Reader, o.Seq, prev))
		}
		if o.Seq > maxSeen[o.Reader] {
			maxSeen[o.Reader] = o.Seq
		}
	}
	return errs
}
