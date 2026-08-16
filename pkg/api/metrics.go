package api

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics tracks request and storage statistics for observability.
type Metrics struct {
	startedAt    time.Time
	totalReq     atomic.Uint64
	latencySumNs atomic.Uint64
	latencyCount atomic.Uint64
	totalKeys    atomic.Int64

	mu       sync.Mutex
	statuses map[int]*atomic.Uint64
}

// NewMetrics creates an empty metrics collector.
func NewMetrics() *Metrics {
	return &Metrics{
		startedAt: time.Now(),
		statuses:  make(map[int]*atomic.Uint64),
	}
}

// Record registers one completed request.
func (m *Metrics) Record(status int, d time.Duration) {
	m.totalReq.Add(1)
	m.latencySumNs.Add(uint64(d.Nanoseconds()))
	m.latencyCount.Add(1)

	m.mu.Lock()
	c, ok := m.statuses[status]
	if !ok {
		c = new(atomic.Uint64)
		m.statuses[status] = c
	}
	m.mu.Unlock()
	c.Add(1)
}

// IncrKeys increments the tracked key count (used when no Size() is available).
func (m *Metrics) IncrKeys() { m.totalKeys.Add(1) }

// DecrKeys decrements the tracked key count.
func (m *Metrics) DecrKeys() { m.totalKeys.Add(-1) }

// TotalKeys returns the locally tracked key count.
func (m *Metrics) TotalKeys() int64 { return m.totalKeys.Load() }

// handleMetrics serves the metrics in Prometheus text format.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s.metrics.render(w, s.keyCount())
}

// keyCount prefers the engine's Size() when available, falling back to the
// locally tracked count so the API layer does not depend on the engine.
func (s *Server) keyCount() int64 {
	if eng, ok := s.engine.(interface{ Size() int }); ok {
		return int64(eng.Size())
	}
	return s.metrics.TotalKeys()
}

func (m *Metrics) render(w http.ResponseWriter, keyCount int64) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	fmt.Fprintln(w, "# HELP kvstore_requests_total Total number of HTTP requests.")
	fmt.Fprintln(w, "# TYPE kvstore_requests_total counter")
	fmt.Fprintf(w, "kvstore_requests_total %d\n", m.totalReq.Load())

	fmt.Fprintln(w, "# HELP kvstore_status_total HTTP status code counts.")
	fmt.Fprintln(w, "# TYPE kvstore_status_total counter")
	m.mu.Lock()
	codes := make([]int, 0, len(m.statuses))
	for code := range m.statuses {
		codes = append(codes, code)
	}
	sort.Ints(codes)
	for _, code := range codes {
		fmt.Fprintf(w, "kvstore_status_total{code=\"%d\"} %d\n", code, m.statuses[code].Load())
	}
	m.mu.Unlock()

	fmt.Fprintln(w, "# HELP kvstore_latency_seconds_total Cumulative request latency.")
	fmt.Fprintln(w, "# TYPE kvstore_latency_seconds_total counter")
	fmt.Fprintf(w, "kvstore_latency_seconds_total %f\n", time.Duration(m.latencySumNs.Load()).Seconds())
	fmt.Fprintf(w, "# TYPE kvstore_latency_seconds_count counter\n")
	fmt.Fprintf(w, "kvstore_latency_seconds_count %d\n", m.latencyCount.Load())

	fmt.Fprintln(w, "# HELP kvstore_keys_total Number of keys in the store.")
	fmt.Fprintln(w, "# TYPE kvstore_keys_total gauge")
	fmt.Fprintf(w, "kvstore_keys_total %d\n", keyCount)

	fmt.Fprintln(w, "# HELP kvstore_uptime_seconds Server uptime.")
	fmt.Fprintln(w, "# TYPE kvstore_uptime_seconds gauge")
	fmt.Fprintf(w, "kvstore_uptime_seconds %d\n", int64(time.Since(m.startedAt).Seconds()))
}