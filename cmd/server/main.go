package main

import (
	"log"
	"os"

	"github.com/Raj-Jai/kv-store/pkg/storage"
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

	log.Printf("kv-store starting on port %s with data dir %s", port, dataDir)
	log.Printf("using in-memory storage engine (persistent engine pending)")
}
