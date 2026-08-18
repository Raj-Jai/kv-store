package linearizability

import (
	"testing"
)

func TestCheckAcceptsMonotonicHistory(t *testing.T) {
	ops := []Op{
		{Kind: Write, Seq: 1, End: 100},
		{Kind: Read, Seq: 1, Reader: "n0", End: 150},
		{Kind: Write, Seq: 2, End: 200},
		{Kind: Read, Seq: 2, Reader: "n0", End: 250},
		{Kind: Read, Seq: 2, Reader: "n1", End: 260},
	}
	if errs := Check(ops); len(errs) != 0 {
		t.Fatalf("expected no violations, got %v", errs)
	}
}

func TestCheckDetectsRollback(t *testing.T) {
	ops := []Op{
		{Kind: Read, Seq: 5, Reader: "n0", End: 100},
		{Kind: Read, Seq: 3, Reader: "n0", End: 200},
	}
	errs := Check(ops)
	if len(errs) != 1 {
		t.Fatalf("expected 1 rollback violation, got %v", errs)
	}
}

func TestCheckIgnoresEmptyReads(t *testing.T) {
	ops := []Op{
		{Kind: Read, Seq: 0, Reader: "n0", End: 100},
		{Kind: Read, Seq: 4, Reader: "n0", End: 200},
		{Kind: Read, Seq: 1, Reader: "n1", End: 300},
		{Kind: Read, Seq: 1, Reader: "n1", End: 400},
	}
	if errs := Check(ops); len(errs) != 0 {
		t.Fatalf("expected no violations, got %v", errs)
	}
}

func TestCheckIsPerReader(t *testing.T) {
	// Different readers observing different values at different times is fine.
	ops := []Op{
		{Kind: Read, Seq: 7, Reader: "n0", End: 100},
		{Kind: Read, Seq: 2, Reader: "n1", End: 200},
	}
	if errs := Check(ops); len(errs) != 0 {
		t.Fatalf("expected no violations, got %v", errs)
	}
}

func TestCheckDetectsFutureRead(t *testing.T) {
	// A read that completes before the write it observed even began is
	// impossible: no node can serve a version the leader never issued.
	ops := []Op{
		{Kind: Write, Seq: 7, Start: 100, End: 200},
		{Kind: Read, Seq: 7, Reader: "n0", Start: 50, End: 90},
	}
	errs := Check(ops)
	if len(errs) != 1 {
		t.Fatalf("expected 1 future-read violation, got %v", errs)
	}
}

func TestCheckAllowsReadOverlappingNewerWrite(t *testing.T) {
	// A read concurrent with a newer write may observe the older version:
	// linearizability allows either order for overlapping operations.
	ops := []Op{
		{Kind: Write, Seq: 1, Start: 0, End: 100},
		{Kind: Write, Seq: 2, Start: 50, End: 150},
		{Kind: Read, Seq: 1, Reader: "n0", Start: 80, End: 140},
	}
	if errs := Check(ops); len(errs) != 0 {
		t.Fatalf("expected no violations, got %v", errs)
	}
}

func TestCheckAllowsUnrecordedWriteHole(t *testing.T) {
	// A propose that failed may still briefly make its value observable after
	// a later persist; reads of such a never-recorded sequence are allowed.
	ops := []Op{
		{Kind: Write, Seq: 1, Start: 0, End: 100},
		{Kind: Write, Seq: 3, Start: 200, End: 300},
		{Kind: Read, Seq: 2, Reader: "n0", Start: 250, End: 260},
	}
	if errs := Check(ops); len(errs) != 0 {
		t.Fatalf("expected no violations, got %v", errs)
	}
}

func TestCheckDetectsRollbackOnWriteTiming(t *testing.T) {
	// Rollback is caught even when both reads also fail the future-read check.
	ops := []Op{
		{Kind: Read, Seq: 5, Reader: "n0", Start: 100, End: 150},
		{Kind: Read, Seq: 3, Reader: "n0", Start: 200, End: 250},
	}
	errs := Check(ops)
	if len(errs) != 1 {
		t.Fatalf("expected 1 rollback violation, got %v", errs)
	}
}
