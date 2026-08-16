package main

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
)

func httpRequest(method, url, body string) (int, string, error) {
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		return 0, "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(data), nil
}

// TestHTTPServerConcurrentLoad bombards the full HTTP stack (handlers,
// middleware, batching engine, WAL) with concurrent writers and readers and
// verifies every acknowledged write is immediately readable.
func TestHTTPServerConcurrentLoad(t *testing.T) {
	s := newTestServer(t)
	defer s.Close()

	const workers = 20
	var wg sync.WaitGroup
	errs := make(chan string, workers)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				key := fmt.Sprintf("w%dk%d", w, i)
				if code, _, err := httpRequest(http.MethodPut, s.URL+"/kv/"+key, "v"); err != nil {
					errs <- fmt.Sprintf("PUT %s: %v", key, err)
					return
				} else if code != http.StatusOK {
					errs <- fmt.Sprintf("PUT %s = %d", key, code)
					return
				}
				if code, body, err := httpRequest(http.MethodGet, s.URL+"/kv/"+key, ""); err != nil {
					errs <- fmt.Sprintf("GET %s: %v", key, err)
					return
				} else if code != http.StatusOK || body != "v" {
					errs <- fmt.Sprintf("GET %s = %d %q", key, code, body)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
}