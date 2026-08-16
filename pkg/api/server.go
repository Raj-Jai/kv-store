package api

import "net/http"

// Handler builds the HTTP router with middleware applied.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.Handle("GET /kv/{key}", validateKey(http.HandlerFunc(s.handleGet)))
	mux.Handle("PUT /kv/{key}", validateKey(limitBody(http.HandlerFunc(s.handlePut))))
	mux.Handle("DELETE /kv/{key}", validateKey(http.HandlerFunc(s.handleDelete)))
	mux.Handle("GET /metrics", http.HandlerFunc(s.handleMetrics))

	return s.cors(s.recoverPanic(s.logRequests(mux)))
}