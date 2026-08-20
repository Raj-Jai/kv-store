# kv-store

A distributed, linearizable key-value store in Go with raft consensus.

Clients hit any node; writes are forwarded to the current raft leader, committed
to a majority, and acknowledged only once the value is durably on disk. Reads
are served from memory on any node (follower reads may be slightly stale — see
[Consistency](#consistency)). Data is encrypted at rest (optional), the server
speaks TLS (optional), and the whole cluster comes up with `docker compose`.

## Features

- **Raft consensus** — leader election, log replication, commit, persistent
  term/vote/log, snapshot-based resync of lagging followers, log compaction.
- **Durable by default** — every acknowledged write is fsynced to the WAL before
  the HTTP `200` is sent, so a `200` survives `kill -9`. A batching engine
  groups writes into a single `fsync` per batch.
- **Linearizable writes** — a write is acked once a majority of nodes has
  applied it; reads from the leader observe every prior write.
- **Atomic operations** — `Incr`, compare-and-swap (`CAS`), and per-key TTL
  (`Expire`), all replicated through the raft log so they are safe under
  concurrency and failover.
- **Range scan** — paginated, glob-pattern scans over the key space.
- **Encryption at rest** (optional) — AES-256-GCM for the WAL, snapshots, and
  the raft log; key from an env var or a mounted key file.
- **TLS** (optional) — TLS 1.2+ with `TLS_CERT`/`TLS_KEY`.
- **Health probes** — liveness (`/healthz`) and readiness (`/readyz`, 503 until
  a leader is known) for orchestrators.
- **Hardened** — fuzz tests on the WAL, property tests, a seeded chaos suite,
  a linearizability checker, and a ≥80% coverage gate enforced in CI.

## Architecture

```
  HTTP Client ──► any node ──► raft leader (consensus, log replication)
                                   │ majority commit
                                   ▼
                          DiskStore (state machine)
                        WAL (fsynced) + snapshot + encryption
```

- **`pkg/raft`** — the consensus layer. `Node` handles elections, log
  replication, commit/apply, persistence (`raft-state.json`), and snapshot
  install/compact. Raft RPCs ride over HTTP (`POST /raft/vote`,
  `/raft/append`, `/raft/snapshot`) or an in-process memory transport.
- **`pkg/storage`** — the state machine behind `Engine`
  (`Get`/`Put`/`Delete`/`Incr`/`CAS`/`Expire`/`Scan`).
  - `MemStore` — in-memory `map[string]entry` guarded by an `RWMutex`.
  - `DiskStore` — the durable engine. Mutations are appended to the WAL and
    synced with a single `fsync` per batch before being applied to memory.
  - `WAL` — append-only binary log `[opCode][keyLen][key][valLen][value]`; in
    encrypted mode each record is a sealed blob framed by
    `[sealedLen][nonce||ct||tag]`.
  - **Batcher** — group commit: flushes a batch when it reaches 1000 operations
    or after 10 ms, whichever comes first.
  - **Snapshot** — when the WAL exceeds 1 MB, the full state is written to
    `snapshot.dat` (temp file → fsync → atomic rename) and the WAL is
    truncated. Recovery loads the snapshot and replays only the log after it.
- **`pkg/api`** — REST handlers, key validation, body limits, panic recovery,
  CORS, request logging, Prometheus metrics, and leader forwarding (a write to
  a follower is transparently proxied to the leader).
- **`pkg/client`** — a tiny HTTP client used by tests and tools.

## Building

```bash
go build -o kv-store ./cmd/server
```

Requires Go 1.26+. The module has no third-party dependencies.

## Running

Standalone (no replication):

```bash
go run ./cmd/server
# or a built binary:
DATA_DIR=./data ./kv-store
```

The server listens on `:8081` by default.

A cluster — three nodes, three terminals:

```bash
DATA_DIR=/tmp/kv1 NODE_ID=http://127.0.0.1:8081 \
PEERS=http://127.0.0.1:8082,http://127.0.0.1:8083 PORT=8081 ./kv-store
# terminal 2
DATA_DIR=/tmp/kv2 NODE_ID=http://127.0.0.1:8082 \
PEERS=http://127.0.0.1:8081,http://127.0.0.1:8083 PORT=8082 ./kv-store
# terminal 3
DATA_DIR=/tmp/kv3 NODE_ID=http://127.0.0.1:8083 \
PEERS=http://127.0.0.1:8081,http://127.0.0.1:8082 PORT=8083 ./kv-store
```

### Configuration (environment variables)

| Variable | Default | Description |
| --- | --- | --- |
| `PORT` | `8081` | TCP port the HTTP server listens on |
| `DATA_DIR` | `./data` | Directory for `wal.log`, `snapshot.dat`, `raft-state.json` |
| `NODE_ID` | — | This node's address (`http://host:port`); required in cluster mode |
| `PEERS` | — | Comma-separated addresses of the other nodes; required in cluster mode |
| `ENCRYPTION_KEY` | — | 32-byte AES-256 key as 64 hex chars (or raw 32 bytes); enables encryption at rest |
| `KEY_FILE` | — | Path to a file holding the key (hex or raw bytes); alternative to `ENCRYPTION_KEY` |
| `TLS_CERT` | — | Path to the TLS certificate; set with `TLS_KEY` to enable TLS |
| `TLS_KEY` | — | Path to the TLS private key |

`PEERS`/`NODE_ID` unset ⇒ standalone mode (no raft, ready immediately). Keys
come from the environment or a mounted file — never from source.

## REST API

| Method | Path | Description | Success | Errors |
| --- | --- | --- | --- | --- |
| `PUT` | `/kv/{key}` | Set a key's value (raw body) | `200` JSON | `400` invalid key, `413` body too large |
| `GET` | `/kv/{key}` | Get a key's value | `200` plain text | `404` not found |
| `DELETE` | `/kv/{key}` | Delete a key | `200` | `400` invalid key |
| `PUT` | `/kv/{key}/expire?ttl=<ns>` | Set a key's TTL (positive nanoseconds) | `200` | `400` bad ttl, `404` |
| `POST` | `/kv/{key}/incr` | Atomically increment the base-10 integer (absent ⇒ `1`) | `200` `{"key":k,"value":N}` | `422` not numeric / overflow |
| `PUT` | `/kv/{key}/cas` | Compare-and-swap; JSON body `{"old":"...","new":"..."}` | `200` | `404`, `409` mismatch |
| `GET` | `/kv?cursor=&count=&pattern=` | Paginated scan; glob `*` patterns, cursor is opaque | `200` `{"items":[...],"cursor":...}` | `400` bad params |
| `GET` | `/healthz` | Liveness probe | `200` `ok` | — |
| `GET` | `/readyz` | Readiness probe | `200` `ready` | `503` `no leader` in cluster mode |
| `GET` | `/metrics` | Prometheus metrics | `200` text | — |

Keys may contain only `[a-zA-Z0-9]`. Values may be up to 1 MB. Scan pages up
to `count` items (default 100, max 1000).

### Examples

```bash
# Set a value (goes to the leader wherever it is)
curl -X PUT http://localhost:8081/kv/name -d "Harsh"
# {"ok":"true","key":"name"}

# Get a value — any node
curl http://localhost:8083/kv/name
# Harsh

# Missing key
curl -i http://localhost:8081/kv/nope
# HTTP/1.1 404 Not Found

# Increment
curl -X POST http://localhost:8081/kv/counter/incr   # {"key":"counter","value":1}
curl -X POST http://localhost:8081/kv/counter/incr   # {"key":"counter","value":2}

# Compare-and-swap
curl -X PUT http://localhost:8081/kv/k -d oldval
curl -X PUT http://localhost:8081/kv/k/cas -d '{"old":"oldval","new":"newval"}'   # 200
curl -X PUT http://localhost:8081/kv/k/cas -d '{"old":"wrong","new":"x"}'         # 409

# TTL (5 seconds)
curl -X PUT http://localhost:8081/kv/name/expire?ttl=5000000000
sleep 6; curl -i http://localhost:8081/kv/name   # 404

# Scan
curl -X PUT http://localhost:8081/kv/apple -d 1
curl -X PUT http://localhost:8081/kv/apricot -d 2
curl "http://localhost:8081/kv?pattern=ap*"
# {"items":[{"key":"apple","value":"1"},{"key":"apricot","value":"2"}],"cursor":""}

# Probes
curl http://localhost:8081/healthz   # ok
curl http://localhost:8081/readyz    # ready
```

## Consistency

- **Writes** are linearizable: the leader appends to its log, waits for a
  majority commit, applies, and only then acknowledges. A `200` is durable on
  the majority — `kill -9` any nodes and the value survives.
- **Leader reads** reflect all acknowledged writes.
- **Follower reads** may be stale (a follower can lag replication). If you need
  read-your-writes, read from the leader, or accept eventual freshness — this is
  the classic raft trade-off the plan deliberately chose.
- If a leader dies, a new one is elected within a couple of seconds and
  re-proposes any uncommitted entry in a later term, so no committed write is
  lost and no uncommitted one is resurrected.

## Encryption at rest

With `ENCRYPTION_KEY` (or `KEY_FILE`) set, every on-disk artifact is sealed
with AES-256-GCM before it is written, and a node refuses to start if its data
is not encrypted:

- **WAL** (`wal.log`) — per-record sealed blobs with fresh random nonces.
- **Local snapshots** (`snapshot.dat`) — the payload is sealed.
- **Raft log** (`raft-state.json`) — sealed with the same key, because the log
  holds every proposed user command until compaction.

Keys are 32 bytes (`64 hex chars`), never hardcoded, and rotate-friendly (point
`KEY_FILE` at a new file). Raft **network** snapshots between nodes stay
plaintext JSON — encryption protects data at rest, not transport (use TLS for
the latter). Grepping the data directory for a stored value yields nothing:

```bash
ENCRYPTION_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
DATA_DIR=/tmp/enc ./kv-store
grep -r myvalue /tmp/enc   # no matches
```

Using a different key (or no key) against encrypted data fails loudly at
startup instead of silently returning garbage.

## TLS

Set both `TLS_CERT` and `TLS_KEY` to serve HTTPS (TLS 1.2 minimum):

```bash
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes -subj /CN=localhost
TLS_CERT=cert.pem TLS_KEY=key.pem ./kv-store
curl -sk https://localhost:8081/healthz
```

## Docker

The `Dockerfile` is multi-stage: a static `CGO_ENABLED=0` binary in an Alpine
runtime running as a non-root user. `docker-compose.yml` brings up a 5-node
cluster with named volumes and readiness healthchecks:

```bash
docker compose up --build -d          # 5 nodes, host ports 8081-8085
curl -X PUT http://localhost:8081/kv/hello -d world
curl http://localhost:8083/kv/hello   # reads from any node
docker compose restart kv2            # SIGTERM -> graceful shutdown
curl http://localhost:8081/kv/hello   # still there; cluster healed
docker compose down -v                # tear down, wiping volumes
```

The stack runs with encryption enabled using a demo key; override it with your
own in the shell or a `.env`:

```bash
ENCRYPTION_KEY=<your 64-hex key> docker compose up -d
```

## Persistence and recovery

- Every write is appended to `wal.log` and fsynced **before** the response is
  sent, so a `200` survives a crash (`kill -9` included).
- On startup the store loads `snapshot.dat` (if present) and replays `wal.log`
  entries written after it, restoring the exact pre-crash state.
- When `wal.log` exceeds 1 MB the store atomically snapshots the state and
  truncates the log, so the log never grows unbounded. Raft independently
  compacts its log (threshold 1000 applied entries) and resyncs lagging
  followers via installed snapshots.
- Graceful shutdown (SIGINT/SIGTERM, what Docker sends on `restart`) drains
  in-flight writes, saves a snapshot, and truncates the WAL. Writes issued
  after shutdown fail with `store closed` instead of panicking.

## Testing

```bash
go test ./...             # all tests
go test -race ./...       # with the race detector (merge gate)
bash scripts/check_coverage.sh          # per-package coverage gate (>= 80%)
CHAOS_SEED=42 go test -race ./pkg/chaos/   # seeded chaos fault matrix
```

Covered scenarios include: CRUD and all atomic ops, WAL/snapshot recovery and
crash windows, leader/follower restarts, full-cluster restart, lagging-follower
snapshot resync, concurrent readers/writers/deleters, graceful shutdown during
active writes, WAL fuzzing (`FuzzWALReplay`), and a chaos matrix that kills,
partitions, and stalls nodes while asserting linearizability on the survivors.

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
cmd/server/        entry point, config (env), TLS, health probes, graceful shutdown
pkg/api/           HTTP handlers, middleware, metrics, leader forwarding
pkg/client/        small HTTP client for tools and tests
pkg/raft/          consensus: election, replication, commit/apply, persistence, snapshots
pkg/storage/       Engine interface, MemStore, DiskStore, WAL, batcher, snapshot, encryption
pkg/chaos/         seeded fault-matrix harness (kill/partition/stall)
pkg/linearizability/  write-linearizability checker over the chaos suite
pkg/util/          structured JSON logging
docs/plan-v3.md    the two-developer build plan that drove this repo
```