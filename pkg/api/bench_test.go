package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Benchmarks over the full handler stack (memstore-backed). CI compares these
// against the committed baseline in benchmarks/api.txt with benchstat and
// fails on a significant regression (phase 3, B-side).

func BenchmarkHandlerPut(b *testing.B) {
	s := newTestServer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPut, "/kv/key", strings.NewReader("value"))
		s.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("status = %d, want 200", rec.Code)
		}
	}
}

func BenchmarkHandlerGet(b *testing.B) {
	s := newTestServer()
	if err := s.engine.Put("key", "value"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/kv/key", nil)
		s.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("status = %d, want 200", rec.Code)
		}
	}
}

func BenchmarkHandlerDelete(b *testing.B) {
	s := newTestServer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := s.engine.Put("key", "value"); err != nil {
			b.Fatal(err)
		}
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodDelete, "/kv/key", nil)
		s.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("status = %d, want 200", rec.Code)
		}
	}
}

func BenchmarkHandlerIncr(b *testing.B) {
	s := newTestServer()
	if err := s.engine.Put("key", "1"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/kv/key/incr", nil)
		s.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("status = %d, want 200", rec.Code)
		}
	}
}

func BenchmarkHandlerCas(b *testing.B) {
	s := newTestServer()
	if err := s.engine.Put("key", "a"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPut, "/kv/key/cas", strings.NewReader(`{"old":"a","new":"b"}`))
		s.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("status = %d, want 200", rec.Code)
		}
	}
}

func BenchmarkHandlerScan(b *testing.B) {
	s := newTestServer()
	for i := 0; i < 100; i++ {
		if err := s.engine.Put("key"+string(rune('a'+i%26)), "value"); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/kv?count=100", nil)
		s.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("status = %d, want 200", rec.Code)
		}
	}
}
