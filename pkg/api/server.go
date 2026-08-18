package api

import "net/http"

// Handler builds the HTTP router with middleware applied.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.Handle("GET /kv/{key}", validateKey(http.HandlerFunc(s.handleGet)))
	mux.Handle("PUT /kv/{key}", validateKey(limitBody(http.HandlerFunc(s.handlePut))))
	mux.Handle("DELETE /kv/{key}", validateKey(http.HandlerFunc(s.handleDelete)))
	mux.Handle("PUT /kv/{key}/expire", validateKey(http.HandlerFunc(s.handleExpire)))
	mux.Handle("POST /kv/{key}/incr", validateKey(http.HandlerFunc(s.handleIncr)))
	mux.Handle("PUT /kv/{key}/cas", validateKey(limitBody(http.HandlerFunc(s.handleCAS))))
	mux.Handle("GET /kv", http.HandlerFunc(s.handleScan))
	mux.Handle("GET /metrics", http.HandlerFunc(s.handleMetrics))

	return s.cors(s.recoverPanic(s.logRequests(mux)))
}
