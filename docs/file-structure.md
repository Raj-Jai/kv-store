# kv-store-go — File Structure (Plan v2, Two-Developer Build)

> Structure follows `docs/plan-v2.md`. Owner tags: **A** = Developer A (Core Engine),
> **B** = Developer B (API & Network), **both** = joint. Phases P1–P4 refer to plan-v2.

```
kv-store/
├── go.mod                               # both P1: module + deps (B initializes per Task 2A)
├── README.md                            # both P4 Task 7C: docs
├── cmd/
│   └── server/
│       └── main.go                      # B P1 Task 2A + P3 Task 6A: entry point, signals, config (PORT, DATA_DIR)
├── pkg/
│   ├── api/
│   │   ├── handlers.go                  # B P2 Task 4A: GET/PUT/DELETE /kv/{key}
│   │   ├── middleware.go                # B P2 Task 4B + P3 Task 6B: validation, logging, panic recovery, metrics
│   │   └── server.go                    # B P1: HTTP server init + routing
│   ├── storage/
│   │   ├── engine.go                    # A P1 Task 1A: Engine interface (source of truth)
│   │   ├── memstore.go                  # A P1 Task 1B: map + RWMutex
│   │   ├── wal.go                       # A P2 Task 3A/3B: append-only log + recovery/replay
│   │   ├── snapshot.go                  # A P3 Task 5B: state serialization, WAL truncate/rotate
│   │   └── batcher.go                   # A P3 Task 5A: group-commit worker (10ms or 1000 items)
│   └── util/
│       └── logger.go                    # B P1: structured JSON logging
├── .gitignore                           # both: data/, binaries, wal.log, snapshot.dat
└── docs/
    ├── plan-v2.md                       # the plan (source of truth)
    ├── plan.md                          # superseded (Raft/cluster plan — NOT used)
    └── file-structure.md                # this file
```

## Seam / contract

The only shared boundary is `pkg/storage/engine.go` (Developer A owns it):

```go
type Engine interface {
    Get(key string) (string, error)
    Put(key, value string) error
    Delete(key string) error
    Clear() error
    Close() error
}
```

Developer B imports only this interface; Developer A can swap `memstore` → WAL-backed → batched-WAL
without touching the API layer. Integration happens at this boundary in Phase 4.

## Ownership map

| Phase | Developer A (Core Engine) | Developer B (API & Network) |
| --- | --- | --- |
| P1 (D1–2) | `storage/engine.go` (Task 1A), `storage/memstore.go` (Task 1B) | `cmd/server/main.go` (Task 2A), mock backend via memstore (Task 2B), `api/server.go`, `util/logger.go` |
| P2 (D3–5) | `storage/wal.go` append + replay (Tasks 3A, 3B) | `api/handlers.go` (Task 4A), `api/middleware.go` validation (Task 4B) |
| P3 (D6–8) | `storage/batcher.go` (Task 5A), `storage/snapshot.go` (Task 5B) | `cmd/server/main.go` graceful shutdown (Task 6A), metrics (Task 6B) |
| P4 (D9–10) | Integration assembly (7A), concurrency stress (7B), README (7C) | same (joint) |

## Notes / differences from the old plan

- **No Raft, replication, chaos, or benchmarks** — plan-v2 is a **single-node persistent KV store**.
  `docs/plan.md` (the Raft/cluster plan) is superseded and not used.
- Directory layout moved from `internal/` to `pkg/`, and `cmd/key-value-go` → `cmd/server`.
- No `internal/contract` package — the `Engine` interface in `pkg/storage/engine.go` is the contract.

## Gates

- Merge gate: `go test -race ./...` clean (P4 Task 7B).
- P2 done when server survives restart and recovers all acknowledged writes from `wal.log`.
- P3 done when batcher measurably beats per-write fsync, and `wal.log` stops growing unbounded.
