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
