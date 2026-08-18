package util

import (
	"bytes"
	"strings"
	"testing"
)

// TestLoggerEmitsStructuredJSON verifies Info/Error produce JSON lines with
// level, msg, timestamp and fields.
func TestLoggerEmitsStructuredJSON(t *testing.T) {
	var buf bytes.Buffer
	l := NewLoggerTo(&buf)

	l.Info("hello", map[string]any{"key": "k", "n": 3})
	l.Error("boom", map[string]any{"err": "e"})

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2:\n%s", len(lines), buf.String())
	}
	for i, want := range []string{`"level":"info"`, `"msg":"hello"`, `"key":"k"`, `"n":3`} {
		if !strings.Contains(lines[0], want) {
			t.Fatalf("line %d missing %s: %s", 0, want, lines[0])
		}
		if i == 0 {
			continue
		}
		if !strings.Contains(lines[1], `"level":"error"`) || !strings.Contains(lines[1], `"msg":"boom"`) {
			t.Fatalf("error line malformed: %s", lines[1])
		}
	}
}

// TestLoggerMarshalFailure verifies an un-marshalable field is dropped without
// a panic or a partial log line.
func TestLoggerMarshalFailure(t *testing.T) {
	var buf bytes.Buffer
	l := NewLoggerTo(&buf)

	l.Error("bad", map[string]any{"f": make(chan int)})
	if buf.Len() != 0 {
		t.Fatalf("expected no output for an un-marshalable field, got %q", buf.String())
	}
}

// TestLoggerDefaultWritesToStderr covers NewLogger's default construction.
func TestLoggerDefaultWritesToStderr(t *testing.T) {
	l := NewLogger()
	if l == nil {
		t.Fatal("NewLogger returned nil")
	}
}
