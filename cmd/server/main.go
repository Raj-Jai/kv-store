package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Raj-Jai/kv-store/pkg/api"
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
	server := api.NewServer(store, logger)

	httpServer := &http.Server{
		Addr:    ":" + port,
		Handler: server.Handler(),
	}

	logger.Info("kv-store starting", map[string]any{"port": port, "data_dir": dataDir})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
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
	case <-ctx.Done():
		logger.Info("shutdown signal received", nil)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", map[string]any{"error": err.Error()})
	}

	logger.Info("closing storage engine", nil)
	if err := store.Close(); err != nil {
		logger.Error("engine close failed", map[string]any{"error": err.Error()})
	}

	logger.Info("server stopped", nil)
}