package main

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Raj-Jai/kv-store/pkg/api"
	"github.com/Raj-Jai/kv-store/pkg/raft"
	"github.com/Raj-Jai/kv-store/pkg/storage"
	"github.com/Raj-Jai/kv-store/pkg/util"
)

const (
	defaultPort    = "8081"
	defaultDataDir = "./data"

	// snapshotCompactThreshold is how many applied log entries may accumulate
	// before the compactor folds them into a storage snapshot.
	snapshotCompactThreshold  = 1000
	snapshotCompactorInterval = 5 * time.Second
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// resolveEncryptionKey returns the AES-256 at-rest key from ENCRYPTION_KEY
// (64 hex chars) or KEY_FILE (a file holding the key as hex or raw 32 bytes).
// It returns nil when neither is set, which keeps the store in plaintext mode.
// The key lives in an environment variable or a mounted file — never in
// source — so rotating it means rotating the file or variable.
func resolveEncryptionKey() ([]byte, error) {
	if kf := os.Getenv("KEY_FILE"); kf != "" {
		data, err := os.ReadFile(kf)
		if err != nil {
			return nil, fmt.Errorf("read KEY_FILE: %w", err)
		}
		return decodeEncryptionKey(strings.TrimSpace(string(data)))
	}
	if k := os.Getenv("ENCRYPTION_KEY"); k != "" {
		return decodeEncryptionKey(strings.TrimSpace(k))
	}
	return nil, nil
}

func decodeEncryptionKey(s string) ([]byte, error) {
	if b, err := hex.DecodeString(s); err == nil && len(b) == storage.AtRestKeySize {
		return b, nil
	}
	if b := []byte(s); len(b) == storage.AtRestKeySize {
		return b, nil
	}
	return nil, fmt.Errorf("encryption key must be %d bytes (64 hex chars or raw)", storage.AtRestKeySize)
}

// withHealth wraps api with liveness and readiness probes. /healthz reports
// process liveness; /readyz reports whether the node can serve traffic — in
// cluster mode it stays 503 until the node knows a leader, so orchestrators do
// not route to a partitioned node. api is a *http.ServeMux or the api handler.
func withHealth(api http.Handler, node *raft.Node) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /healthz", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok\n")
	}))
	mux.Handle("GET /readyz", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if node != nil && !node.HasLeader() {
			w.WriteHeader(http.StatusServiceUnavailable)
			io.WriteString(w, "no leader\n")
			return
		}
		io.WriteString(w, "ready\n")
	}))
	mux.Handle("/", api)
	return mux
}

func main() {
	port := envOr("PORT", defaultPort)
	dataDir := envOr("DATA_DIR", defaultDataDir)

	encKey, err := resolveEncryptionKey()
	if err != nil {
		log.Fatalf("encryption key: %v", err)
	}

	var store *storage.DiskStore
	if encKey != nil {
		store, err = storage.OpenDiskStoreWithKey(dataDir, encKey)
	} else {
		store, err = storage.OpenDiskStore(dataDir)
	}
	if err != nil {
		log.Fatalf("failed to open storage engine: %v", err)
	}

	logger := util.NewLogger()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Multi-node mode: when PEERS is set this node joins a raft cluster.
	// NODE_ID is this node's reachable address (peers route RPCs to it).
	var node *raft.Node
	var engine storage.Engine = store
	if peersRaw := os.Getenv("PEERS"); peersRaw != "" {
		nodeID := envOr("NODE_ID", "http://127.0.0.1:"+port)
		peers := strings.Split(peersRaw, ",")
		node = raft.NewNode(nodeID, peers, &raft.HTTPTransport{}, store)

		// Durable raft state (M1.5): term, votes and log survive restarts.
		// The raft log carries every proposed user command until compaction,
		// so under an encryption key it is sealed too — one key covers the
		// WAL, local snapshots, and raft state.
		var raftStore raft.RaftStore
		if encKey != nil {
			cipher, err := storage.NewAtRestCipher(encKey)
			if err != nil {
				logger.Error("failed to build at-rest cipher", map[string]any{"error": err.Error()})
				os.Exit(1)
			}
			raftStore = raft.NewEncryptedFileRaftStore(filepath.Join(dataDir, "raft-state.json"), cipher)
		} else {
			raftStore = raft.NewFileRaftStore(filepath.Join(dataDir, "raft-state.json"))
		}
		if err := node.SetRaftStore(raftStore); err != nil {
			logger.Error("failed to wire raft state store", map[string]any{"error": err.Error()})
		}
		// Storage <-> raft snapshot bridge (M1.6): resync lagging followers
		// and compact the log once it grows past the threshold.
		bridge := &snapshotBridge{node: node, store: store}
		node.SetSnapshotter(bridge)
		node.SetSnapshotSink(bridge)

		go node.Loop(ctx)
		node.StartApply(ctx)
		startSnapshotCompactor(ctx, bridge, logger, snapshotCompactThreshold)
		engine = node
	}

	server := api.NewServer(engine, logger)

	var handler http.Handler = server.Handler()
	if node != nil {
		// Serve raft RPCs alongside the client API so peers can reach us.
		raftHandler := raft.ServeRaftHTTP(node)
		mux := http.NewServeMux()
		mux.Handle("POST /raft/vote", raftHandler)
		mux.Handle("POST /raft/append", raftHandler)
		mux.Handle("POST /raft/snapshot", raftHandler)
		mux.Handle("/", handler)
		handler = withHealth(mux, node)
	} else {
		handler = withHealth(handler, nil)
	}

	certFile := os.Getenv("TLS_CERT")
	keyFile := os.Getenv("TLS_KEY")
	if (certFile == "") != (keyFile == "") {
		logger.Error("TLS_CERT and TLS_KEY must be set together", nil)
		os.Exit(1)
	}

	httpServer := &http.Server{
		Addr:      ":" + port,
		Handler:   handler,
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	}

	logger.Info("kv-store starting", map[string]any{
		"port": port, "data_dir": dataDir, "peers": os.Getenv("PEERS"),
		"node_id":   envOr("NODE_ID", ""),
		"tls":       certFile != "",
		"encrypted": encKey != nil,
	})

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		if certFile != "" {
			errCh <- httpServer.ListenAndServeTLS(certFile, keyFile)
		} else {
			errCh <- httpServer.ListenAndServe()
		}
	}()

	select {
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			logger.Error("server failed", map[string]any{"error": err.Error()})
			log.Fatal(err)
		}
	case <-sigCtx.Done():
		logger.Info("shutdown signal received", nil)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", map[string]any{"error": err.Error()})
	}

	logger.Info("stopping raft node", nil)
	cancel()
	if node != nil {
		node.Stop()
	}

	logger.Info("closing storage engine", nil)
	if err := store.Close(); err != nil {
		logger.Error("engine close failed", map[string]any{"error": err.Error()})
	}

	logger.Info("server stopped", nil)
}
