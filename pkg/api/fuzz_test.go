package api

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// buildRawRequest assembles a request from fuzz-provided parts so that
// malformed methods, paths and Content-Length values are exercised against
// the real handler stack instead of being rejected by httptest.NewRequest.
func buildRawRequest(method, path, contentLength string, body []byte) (*http.Request, error) {
	var b strings.Builder
	b.WriteString(method)
	b.WriteByte(' ')
	b.WriteString(path)
	b.WriteString(" HTTP/1.1\r\nHost: test\r\n")
	if contentLength != "" {
		b.WriteString("Content-Length: ")
		b.WriteString(contentLength)
		b.WriteString("\r\n")
	}
	b.WriteString("\r\n")
	b.Write(body)
	return http.ReadRequest(bufio.NewReader(strings.NewReader(b.String())))
}

// FuzzKVRequest drives GET/PUT/DELETE /kv/{key} with fuzz-controlled method,
// path (invalid UTF-8 keys, malformed paths), body (partial/oversized) and a
// Content-Length header (mismatches, negatives, non-numeric). A 500 is never
// expected from the memstore-backed server: recoverPanic masks handler panics
// as 500, so any 500 found here is a real bug to open.
func FuzzKVRequest(f *testing.F) {
	f.Add("PUT", "/kv/a", "5", []byte("hello"))
	f.Add("PUT", "/kv/bad-key", "", []byte("x"))
	f.Add("GET", "/kv/missing", "", []byte(nil))
	f.Add("DELETE", "/kv/a", "", []byte(nil))
	f.Add("OPTIONS", "/kv/a", "", []byte(nil))
	f.Add("PUT", "/kv/clen", "5", []byte("abc"))
	f.Add("PUT", "/kv/huge", "99999999999999", []byte("x"))
	f.Add("PUT", "/kv/neg", "-1", []byte("x"))
	f.Add("PUT", "/kv/nonnum", "abc", []byte("x"))
	f.Add("PUT", "/kv/empty", "", []byte(nil))
	f.Add("GET", "/kv/a?q=1", "", []byte(nil))
	f.Add("GET", "/kv/%FF%FE", "", []byte(nil))

	s := newTestServer()
	f.Fuzz(func(t *testing.T, method, path, contentLength string, body []byte) {
		req, err := buildRawRequest(method, path, contentLength, body)
		if err != nil {
			return
		}
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		if rec.Code == http.StatusInternalServerError {
			t.Fatalf("handler returned 500 for %s %s (clen=%q, body=%d bytes)", method, path, contentLength, len(body))
		}
	})
}
