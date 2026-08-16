package main

import (
	"log"
	"net/http"
	"os"

	"github.com/Raj-Jai/kv-store/pkg/api"
	"github.com/Raj-Jai/kv-store/pkg/storage"
	"github.com/Raj-Jai/kv-store/pkg/util"
)

const (
	defaultPort     = "8080"
	defaultDataDir  = "./data"
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

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		log.Fatalf("failed to create data directory %q: %v", dataDir, err)
	}

	// Temporary backend while the persistent disk engine is under development.
	store := storage.NewMemStore()
	defer store.Close()

	logger := util.NewLogger()
	server := api.NewServer(store, logger)

	httpServer := &http.Server{
		Addr:    ":" + port,
		Handler: server.Handler(),
	}

	logger.Info("kv-store starting", map[string]any{"port": port, "data_dir": dataDir})
	logger.Info("using in-memory storage engine (persistent engine pending)", nil)

	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("server failed", map[string]any{"error": err.Error()})
		log.Fatal(err)
	}
}
