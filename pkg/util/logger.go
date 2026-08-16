package util

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

// Logger is a minimal structured JSON logger.
type Logger struct {
	mu  sync.Mutex
	log *log.Logger
}

// NewLogger creates a JSON logger writing to stderr by default.
func NewLogger() *Logger {
	return NewLoggerTo(os.Stderr)
}

// NewLoggerTo creates a JSON logger writing to the given writer.
func NewLoggerTo(w io.Writer) *Logger {
	return &Logger{log: log.New(w, "", 0)}
}

func (l *Logger) logf(level, msg string, fields map[string]any) {
	entry := make(map[string]any, len(fields)+3)
	entry["level"] = level
	entry["msg"] = msg
	entry["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	for k, v := range fields {
		entry[k] = v
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	b, err := json.Marshal(entry)
	if err != nil {
		return
	}
	l.log.Println(string(b))
}

// Info logs an informational message with optional key/value fields.
func (l *Logger) Info(msg string, fields map[string]any) {
	l.logf("info", msg, fields)
}

// Error logs an error message with optional key/value fields.
func (l *Logger) Error(msg string, fields map[string]any) {
	l.logf("error", msg, fields)
}