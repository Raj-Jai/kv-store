package api

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"

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
