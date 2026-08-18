package api

import (
	"net/http"
	"regexp"
	"time"
)

const (
	// maxBodyBytes rejects payloads larger than 1 MB.
	maxBodyBytes = 1 << 20
)

// keyPattern allows alphanumeric keys plus ':' so namespaced keys like
// "user:1" can be paged with Scan patterns.
var keyPattern = regexp.MustCompile(`^[a-zA-Z0-9:]+$`)

// responseWriter captures the status code for request logging.
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if rw.status == 0 {
		rw.status = http.StatusOK
	}
	return rw.ResponseWriter.Write(b)
}

// limitBody wraps a handler with a maximum request body size.
func limitBody(next http.Handler) http.Handler {
	return http.MaxBytesHandler(next, maxBodyBytes)
}

// validateKey rejects keys that are not alphanumeric.
func validateKey(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !keyPattern.MatchString(r.PathValue("key")) {
			http.Error(w, "key must contain only alphanumeric characters", http.StatusBadRequest)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// logRequests logs method, path, status and duration for every request.
func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w}
		next.ServeHTTP(rw, r)
		status := rw.status
		if status == 0 {
			status = http.StatusOK
		}
		s.metrics.Record(status, time.Since(start))
		s.logger.Info("request", map[string]any{
			"method":   r.Method,
			"path":     r.URL.Path,
			"status":   status,
			"duration": time.Since(start).String(),
		})
	})
}

// recoverPanic converts panics into 500 responses.
func (s *Server) recoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				s.logger.Error("panic recovered", map[string]any{
					"error":  err,
					"method": r.Method,
					"path":   r.URL.Path,
				})
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// cors enables browser clients (e.g. Restfox) by answering preflight
// OPTIONS requests and adding CORS headers to every response.
func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Max-Age", "600")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
