package api

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/Raj-Jai/kv-store/pkg/storage"
	"github.com/Raj-Jai/kv-store/pkg/util"
)

// Server holds the storage engine and routes requests to it.
type Server struct {
	engine  storage.Engine
	logger  *util.Logger
	metrics *Metrics
}

// NewServer creates an API server backed by the given storage engine.
func NewServer(engine storage.Engine, logger *util.Logger) *Server {
	if logger == nil {
		logger = util.NewLogger()
	}
	return &Server{engine: engine, logger: logger, metrics: NewMetrics()}
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		s.logger.Error("failed to encode response", map[string]any{"error": err.Error()})
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	value, err := s.engine.Get(r.PathValue("key"))
	if errors.Is(err, storage.ErrNotFound) {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	if _, err := io.WriteString(w, value); err != nil {
		log.Printf("write response: %v", err)
	}
}

// handleWriteError maps a consensus "not the leader" error to an HTTP
// response: a 307 redirect to the current leader, or a 503 when no leader is
// known yet. It reports whether it consumed the error.
func (s *Server) handleWriteError(w http.ResponseWriter, r *http.Request, err error) bool {
	var nle *storage.NotLeaderError
	if !errors.As(err, &nle) {
		return false
	}
	if nle.LeaderAddr == "" {
		http.Error(w, "no leader elected", http.StatusServiceUnavailable)
		return true
	}
	http.Redirect(w, r, nle.LeaderAddr+r.URL.RequestURI(), http.StatusTemporaryRedirect)
	return true
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		s.logger.Error("failed to read body", map[string]any{"error": err.Error()})
		s.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
		return
	}

	if _, err := s.engine.Get(key); errors.Is(err, storage.ErrNotFound) {
		s.metrics.IncrKeys()
	}
	if err := s.engine.Put(key, string(body)); err != nil {
		if s.handleWriteError(w, r, err) {
			return
		}
		s.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"ok": "true", "key": key})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if _, err := s.engine.Get(key); err == nil {
		s.metrics.DecrKeys()
	}
	if err := s.engine.Delete(key); err != nil {
		if s.handleWriteError(w, r, err) {
			return
		}
		s.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	w.WriteHeader(http.StatusOK)
}

// maxTTL bounds an expire request's lifetime to 10 years.
const maxTTL = 10 * 365 * 24 * time.Hour

func (s *Server) handleExpire(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	ttlStr := r.URL.Query().Get("ttl")
	if ttlStr == "" {
		s.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing ttl query parameter"})
		return
	}
	ttl, err := strconv.ParseInt(ttlStr, 10, 64)
	if err != nil || ttl <= 0 || ttl > int64(maxTTL) {
		s.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "ttl must be a positive duration in nanoseconds"})
		return
	}

	err = s.engine.Expire(key, time.Now().Add(time.Duration(ttl)).UnixNano())
	if errors.Is(err, storage.ErrNotFound) {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	if s.handleWriteError(w, r, err) {
		return
	}
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"ok": "true", "key": key})
}

func (s *Server) handleIncr(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	value, err := s.engine.Incr(key)
	if errors.Is(err, storage.ErrNotNumeric) {
		s.writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "value is not a base-10 integer"})
		return
	}
	if errors.Is(err, storage.ErrOverflow) {
		s.writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "value overflows int64"})
		return
	}
	if s.handleWriteError(w, r, err) {
		return
	}
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]any{"key": key, "value": value})
}

func (s *Server) handleCAS(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		s.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
		return
	}

	var req struct {
		Old string `json:"old"`
		New string `json:"new"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		s.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body must be JSON with old and new fields"})
		return
	}

	ok, err := s.engine.CAS(key, req.Old, req.New)
	if errors.Is(err, storage.ErrNotFound) {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	if s.handleWriteError(w, r, err) {
		return
	}
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if !ok {
		s.writeJSON(w, http.StatusConflict, map[string]string{"error": "cas mismatch"})
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"ok": "true", "key": key})
}

// maxCount is the largest page Scan returns; maxCursorLen and maxPatternLen
// bound the otherwise-unbounded query inputs.
const (
	defaultCount  = 100
	maxCount      = 1000
	maxCursorLen  = 4096
	maxPatternLen = 256
)

func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	cursor := q.Get("cursor")
	if len(cursor) > maxCursorLen {
		s.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "cursor too long"})
		return
	}

	count := defaultCount
	if cs := q.Get("count"); cs != "" {
		n, err := strconv.Atoi(cs)
		if err != nil || n < 1 || n > maxCount {
			s.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "count must be between 1 and 1000"})
			return
		}
		count = n
	}

	pattern := q.Get("pattern")
	if len(pattern) > maxPatternLen {
		s.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "pattern too long"})
		return
	}

	items, next, err := s.engine.Scan(cursor, count, pattern)
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]any{"items": items, "cursor": next})
}
