// Package client is a small HTTP client for the KV store that follows the
// leader-redirect protocol (M1.2): writing to a follower returns a 307 to the
// current leader, and this client replays the request there transparently.
package client

import (
	"io"
	"net/http"
	"strings"
	"time"
)

// Client talks to one node of the cluster. GetBody is set on requests so Go's
// default redirect handling can re-send PUT/DELETE bodies to the leader.
type Client struct {
	base string
	http *http.Client
}

// New creates a client rooted at a node address (e.g. "http://localhost:8081").
func New(base string) *Client {
	return &Client{
		base: strings.TrimRight(base, "/"),
		http: &http.Client{Timeout: 5 * time.Second},
	}
}

// Put writes value at key, following leader redirects.
func (c *Client) Put(key, value string) (*http.Response, error) {
	body := value
	req, err := http.NewRequest(http.MethodPut, c.url("/kv/"+key), strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(body)), nil
	}
	return c.http.Do(req)
}

// Get reads key from the local node.
func (c *Client) Get(key string) (*http.Response, error) {
	return c.http.Get(c.url("/kv/" + key))
}

// Delete removes key, following leader redirects.
func (c *Client) Delete(key string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodDelete, c.url("/kv/"+key), nil)
	if err != nil {
		return nil, err
	}
	return c.http.Do(req)
}

func (c *Client) url(path string) string {
	return c.base + path
}
