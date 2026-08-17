package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
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
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	port := envOr("PORT", defaultPort)
	dataDir := envOr("DATA_DIR", defaultDataDir)

	store, err := storage.OpenDiskStore(dataDir)
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
		go node.Loop(ctx)
		node.StartApply(ctx)
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
		mux.Handle("/", handler)
		handler = mux
	}

	httpServer := &http.Server{
		Addr:    ":" + port,
		Handler: handler,
	}

	logger.Info("kv-store starting", map[string]any{
		"port": port, "data_dir": dataDir, "peers": os.Getenv("PEERS"),
		"node_id": envOr("NODE_ID", ""),
	})

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		errCh <- httpServer.ListenAndServe()
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
