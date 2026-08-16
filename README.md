# kv-store-go

A high-performance, persistent, single-node key-value store in Go.

Every write is durably committed to a write-ahead log (WAL) before it is
acknowledged, so an HTTP `200` means the value is on disk. A batching engine
groups writes into a single `fsync` per batch, and periodic snapshots keep the
log bounded. The storage layer is exposed through a small `Engine` interface,
so the HTTP API never depends on the storage implementation.

## Architecture

```
                    ┌─────────────────────┐
                    │      HTTP Client    │
                    └──────────┬──────────┘
                               ▼
                    ┌─────────────────────┐
                    │     API Layer       │
                    │  Handlers/Middleware │
                    └──────────┬──────────┘
                               ▼
                    ┌─────────────────────┐
                    │    Engine Interface │
                    └──────────┬──────────┘
                               ▼
                    ┌─────────────────────┐
                    │     MemStore        │
                    │   map + RWMutex     │
                    └──────────┬──────────┘
                               ▼
                     ┌─────────┴─────────┐
                     ▼                   ▼
              ┌─────────────┐    ┌──────────────┐
              │     WAL     │    │  Batcher     │
              │  wal.log   │    │ (group commit)│
              └──────┬──────┘    └──────┬───────┘
                     │                  │
                     ▼                  ▼
              ┌─────────────┐
              │     Disk    │
              │  Persistence │
              └─────────────┘
```

### Storage engine (`pkg/storage`)

- **`Engine`** — the interface (`Get`, `Put`, `Delete`, `Clear`, `Close`) that
  decouples the API from the storage implementation.
- **`MemStore`** — thread-safe in-memory map (`sync.RWMutex`).
- **`DiskStore`** — the durable engine. Mutations are queued, appended to the
  WAL, and synced with a **single fsync per batch** before being applied to
  memory, so an acknowledged write survives a crash.
- **`WAL`** — append-only binary log `[opCode][keyLen][key][valLen][value]`.
- **Batcher** — group commit: flushes a batch when it reaches 1000 operations
  or after 10 ms, whichever comes first.
- **Snapshot** — when the WAL exceeds 1 MB, the full state is written to
  `snapshot.dat` (temp file → fsync → atomic rename) and the WAL is truncated.
  Recovery loads the snapshot and replays only the log entries after it.

### API layer (`pkg/api`)

- REST handlers, key validation (alphanumeric only), 1 MB body limit, panic
  recovery, CORS, JSON request logging, and a Prometheus metrics endpoint.

## Building

```bash
go build -o kv-store ./cmd/server
```

Requires Go 1.26+.

## Running

```bash
go run ./cmd/server
# or, with a built binary:
DATA_DIR=./data ./kv-store
```

The server listens on `:8081` by default.

### Configuration (environment variables)

| Variable | Default | Description |
| --- | --- | --- |
| `PORT` | `8081` | TCP port the HTTP server listens on |
| `DATA_DIR` | `./data` | Directory for `wal.log` and `snapshot.dat` |

## REST API

| Method | Path | Description | Success | Errors |
| --- | --- | --- | --- | --- |
| `PUT` | `/kv/{key}` | Set a key's value (raw body) | `200` JSON | `400` invalid key, `413` body too large |
| `GET` | `/kv/{key}` | Get a key's value | `200` plain text | `404` not found |
| `DELETE` | `/kv/{key}` | Delete a key | `200` | `400` invalid key |
| `GET` | `/metrics` | Prometheus metrics | `200` text | — |

Keys may contain only `[a-zA-Z0-9]`. Values may be up to 1 MB.

### Examples

```bash
# Set a value
curl -X PUT http://localhost:8081/kv/name -d "Harsh"
# {"ok":"true","key":"name"}

# Get a value
curl http://localhost:8081/kv/name
# Harsh

# Get a missing key
curl -i http://localhost:8081/kv/nope
# HTTP/1.1 404 Not Found

# Delete a key
curl -X DELETE http://localhost:8081/kv/name

# Metrics
curl http://localhost:8081/metrics
```

## Persistence and recovery

- Every write is appended to `wal.log` and fsynced **before** the response is
  sent. A `200` response therefore guarantees the write survives a crash
  (`kill -9` included).
- On startup the store loads `snapshot.dat` (if present) and replays `wal.log`
  entries written after it, restoring the exact pre-crash state.
- When `wal.log` exceeds 1 MB the store atomically snapshots the state and
  truncates the log, so the log never grows unbounded.
- Graceful shutdown (SIGINT/SIGTERM) drains in-flight writes, saves a
  snapshot, and truncates the WAL. Writes issued after shutdown fail with
  `store closed` instead of panicking.

## Testing

```bash
go test ./...             # all tests
go test -race ./...       # with the race detector (merge gate)
go test -v ./pkg/storage/ # storage tests, verbose
```

Covered scenarios include: CRUD, restart + WAL recovery, snapshot + WAL-tail
recovery, crash between snapshot rename and WAL truncation, concurrent
readers/writers/deleters, graceful shutdown during active writes, and
concurrent HTTP load through the full stack.

## Performance characteristics

Writes go through a group-commit batcher, so the cost of an `fsync` (~ms) is
amortized across a whole batch:

- **Sequential writes** — each write waits up to 10 ms for its batch to flush
  (batching trades single-op latency for throughput).
- **Concurrent writes** — a burst of writers shares one `fsync` per batch,
  yielding significantly higher aggregate throughput than a sync-per-write
  design.
- **Reads** — served from memory, no disk I/O.

## Project structure

```
cmd/server/        entry point, config, graceful shutdown, integration tests
pkg/api/           HTTP handlers, middleware, metrics
pkg/storage/       Engine interface, MemStore, DiskStore, WAL, batcher, snapshot
pkg/util/          structured JSON logging
```