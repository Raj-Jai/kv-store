# Detailed Implementation Plan: KV-Store-Go

This document outlines the step-by-step parallel work plan for two developers to reimplement a high-performance, persistent Key-Value Store in Go.

---

## 📁 Project File Structure

```text
kv-store-go/
├── cmd/
│   └── server/
│       └── main.go          # Entry point: Server lifecycle, signals, config parsing
├── pkg/
│   ├── api/
│   │   ├── handlers.go      # HTTP REST handlers (GET, PUT, DELETE, CLEAR)
│   │   ├── middleware.go    # Input validation, request logging, panic recovery
│   │   └── server.go        # HTTP server initialization and routing
│   ├── storage/
│   │   ├── engine.go        # Interface definition for the storage engine
│   │   ├── memstore.go      # In-memory map protected by RWMutex
│   │   ├── wal.go           # Write-Ahead Log (append-only file writer/reader)
│   │   ├── snapshot.go      # Memory state serialization to disk
│   │   └── batcher.go       # Async worker for grouping disk commits
│   └── util/
│       └── logger.go        # Structured JSON logging utility
├── go.mod                   # Go module dependencies
└── README.md                # Project documentation
```

---

## 🚀 Phase 1: Foundation & Interface Agreement (Days 1–2)

**Goal:** Establish the strict contract between the storage layer and the network layer so both developers can work completely independently.

### Developer A — Core Engine

#### Task 1A: Define Engine Interface (`pkg/storage/engine.go`)

* **Definition:** Write the Go interface containing `Get(key)`, `Put(key, val)`, `Delete(key)`, `Clear()`, and `Close()`.
* **End Result Functionality:** Acts as the source of truth for how the API communicates with the storage engine.

#### Task 1B: Implement In-Memory Store (`pkg/storage/memstore.go`)

* **Definition:** Implement the `Engine` interface using a standard Go `map[string]string` wrapped in a `sync.RWMutex`.
* **End Result Functionality:** Provides fast, thread-safe memory access for reading and writing data before disk persistence is added.

### Developer B — API & Network

#### Task 2A: Setup Project & Configuration (`cmd/server/main.go`)

* **Definition:** Initialize `go.mod`. Write the entry point that reads environment variables such as `PORT` and `DATA_DIR`.
* **End Result Functionality:** Allows the application to be configured across different environments such as development and production.

#### Task 2B: Build Mock Storage Adapter

* **Definition:** Import Developer A's interface and instantiate the basic `memstore` to use as a temporary backend for API development.
* **End Result Functionality:** Unblocks API development while the complex disk I/O engine is being built.

---

## 🏗️ Phase 2: Core Engineering & Networking (Days 3–5)

**Goal:** Build the primary functional requirements: HTTP accessibility and disk persistence.

### Developer A — Core Engine

#### Task 3A: Write-Ahead Log Implementation (`pkg/storage/wal.go`)

* **Definition:** Create an append-only file mechanism. Every `Put` and `Delete` must be serialized to a byte format, for example:

```text
[OpCode][KeyLen][Key][ValLen][Value]
```

* Append each operation to `wal.log`.
* **End Result Functionality:** Ensures that if the server crashes, no acknowledged data is lost.

#### Task 3B: WAL Recovery / Replay (`pkg/storage/wal.go`)

* **Definition:** Write a startup function that reads `wal.log` sequentially and populates the `memstore` map before the server accepts requests.
* **End Result Functionality:** Restores the exact state of the database upon server restart.

### Developer B — API & Network

#### Task 4A: REST API Handlers (`pkg/api/handlers.go`)

Implement HTTP handlers mapping to the `Engine` interface:

* `PUT /kv/{key}` — Body: value

* `GET /kv/{key}`

* `DELETE /kv/{key}`

* **End Result Functionality:** Provides the external interface for clients to interact with the database.

#### Task 4B: Input Validation & Middleware (`pkg/api/middleware.go`)

* **Definition:** Write middleware to reject oversized payloads, for example, payloads greater than 1 MB.
* Validate that keys contain only alphanumeric characters.
* Add request logging and panic recovery where appropriate.
* **End Result Functionality:** Protects the database from malicious input and excessive memory usage.

---

## 🛡️ Phase 3: Performance & Resilience (Days 6–8)

**Goal:** Optimize the database for high throughput and prevent unbounded disk usage.

### Developer A — Core Engine

#### Task 5A: Group Commit Batcher (`pkg/storage/batcher.go`)

* **Definition:** Instead of calling `fsync()` on every single request, queue incoming writes into a Go channel.
* A background worker reads from the queue and flushes multiple operations to disk:

  * Every ~10 ms, or
  * When the batch reaches 1000 items.
* **End Result Functionality:** Increases write throughput by minimizing disk I/O overhead.

#### Task 5B: Snapshot Engine (`pkg/storage/snapshot.go`)

* **Definition:** Create a background routine that periodically:

  1. Locks the map.
  2. Writes the entire state to `snapshot.dat`.
  3. Safely truncates or rotates `wal.log`.
* **End Result Functionality:** Prevents the WAL file from growing infinitely large and keeps startup recovery fast.

### Developer B — API & Network

#### Task 6A: Graceful Shutdown (`cmd/server/main.go`)

* **Definition:** Use `os.Signal` channels to catch `SIGINT` and `SIGTERM`.
* Upon receiving a signal:

  1. Stop accepting new HTTP requests.
  2. Wait for pending requests to finish.
  3. Command the Storage Engine to `Close()`.
  4. Flush pending data to disk.
* **End Result Functionality:** Prevents data corruption during deployments or unexpected server termination.

#### Task 6B: Metrics & Observability (`pkg/api/middleware.go`)

Add basic tracking or Prometheus metrics for:

* Request latency

* Total keys

* HTTP status code counts

* Request counts

* **End Result Functionality:** Gives operators visibility into the health and performance of the key-value store.

---

## 🏁 Phase 4: Integration & Polish (Days 9–10)

**Goal:** Combine both developers' work, test edge cases, and finalize the repository.

### Joint Tasks — Developer A & B

#### Task 7A: Integration Assembly

* Wire Developer B's HTTP server to use Developer A's fully finished Batched-WAL Storage Engine instead of the basic memory store.
* Verify that all API endpoints work correctly with persistent storage.

#### Task 7B: Concurrency Stress Testing

* Write Go tests or use tools such as `hey` or `wrk` to bombard the server with concurrent reads and writes.
* Test with:

```bash
go test -race ./...
```

* Test scenarios should include:

  * Concurrent writes
  * Concurrent reads
  * Concurrent reads and writes
  * Deletes during reads
  * Server restart and WAL recovery
  * Snapshot and WAL interaction
  * Graceful shutdown during active requests

* **End Result Functionality:** Helps guarantee that the system is thread-safe and resilient under concurrent workloads.

#### Task 7C: Documentation (`README.md`)

Write clear instructions covering:

* Project architecture
* Building the project
* Running the server
* Configuration/environment variables
* REST API endpoints
* `curl` examples
* Persistence and recovery
* Running tests
* Performance benchmarks

Example:

```bash
# Start the server
go run ./cmd/server

# Put a value
curl -X PUT http://localhost:8080/kv/name \
  -d "Harsh"

# Get a value
curl http://localhost:8080/kv/name

# Delete a value
curl -X DELETE http://localhost:8080/kv/name
```

---

## 🎯 Final Architecture

```text
                    ┌─────────────────────┐
                    │      HTTP Client    │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │     API Layer       │
                    │  Handlers/Middleware │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │    Engine Interface │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │     MemStore        │
                    │   map + RWMutex     │
                    └──────────┬──────────┘
                               │
                    ┌──────────┴──────────┐
                    ▼                     ▼
             ┌─────────────┐      ┌─────────────┐
             │     WAL     │      │  Snapshot   │
             │  wal.log   │      │snapshot.dat │
             └──────┬──────┘      └─────────────┘
                    │
                    ▼
             ┌─────────────┐
             │    Disk     │
             │ Persistence │
             └─────────────┘
```

### Parallel Development Principle

The key design decision is the **`Engine` interface**.

Developer B depends only on:

```go
type Engine interface {
    Get(key string) (string, error)
    Put(key, value string) error
    Delete(key string) error
    Clear() error
    Close() error
}
```

Therefore:

* **Developer A** can independently improve the storage implementation.
* **Developer B** can independently build and test the HTTP API.
* The API does not need to know whether the backend is an in-memory store, WAL-backed store, or future storage implementation.
* Integration happens primarily at the interface boundary during Phase 4.
