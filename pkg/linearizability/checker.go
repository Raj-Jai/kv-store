// Package linearizability records a single-key op history and checks it for
// the necessary conditions of linearizability. This is a small sequential
// checker (Developer B owns it; a full Porcupine-style search is future work):
// it catches real violations without the expense of an exponential search.
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

// Op is one recorded client-visible operation on a single key. For a write,
// Seq is the value version written, Start when the write was issued and End
// when it was acknowledged. For a read, Seq is the version observed, Start and
// End bound the read, and Reader names the client/node that served it.
type Op struct {
	Kind   Kind
	Seq    uint64
	Reader string
	Start  int64 // unix nano; 0 when unknown
	End    int64 // unix nano; 0 when unknown
}

// Check verifies a single-key history against the necessary conditions of
// linearizability:
//
//   - no reader ever observes a value that rolls back to an older one (once a
//     node has served version s, it must never serve an older version);
//   - no read completes before the write whose value it observed even began
//     (a node can only serve a version once the leader has issued that write).
//
// A read that observes the empty value (Seq == 0) is always allowed. A read
// that overlaps a newer write and observes the older version is allowed too:
// the operations are concurrent, so linearizability permits either order —
// this is exactly the staleness a follower may legitimately serve. Writes that
// were issued but not recorded (e.g. a failed propose) may still briefly be
// observable, so unknown sequences are not flagged.
//
// Returns the violations found; an empty slice means the history passes.
func Check(ops []Op) []error {
	var errs []error
	writes := map[uint64]Op{}
	for _, o := range ops {
		if o.Kind == Write {
			writes[o.Seq] = o
		}
	}
	maxSeen := map[string]uint64{}
	for _, o := range ops {
		if o.Kind != Read || o.Seq == 0 {
			continue
		}
		if w, ok := writes[o.Seq]; ok && w.Start > 0 && o.End > 0 && o.End <= w.Start {
			errs = append(errs, fmt.Errorf("reader %q observed seq %d at %d before its write began at %d", o.Reader, o.Seq, o.End, w.Start))
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
