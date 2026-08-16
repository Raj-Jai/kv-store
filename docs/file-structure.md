# kv-store-go — File Structure (Two-Person Build)

> Layout follows the reference implementation [`clstr-io/kv-store-go`](https://github.com/clstr-io/kv-store-go)
> conventions (module `github.com/clstr-io/key-value-go`, go 1.26) and is extended with the
> two-person plan's additions from `docs/plan.md`.
>
> Owner tags: **A** = Person A (Core/Consensus), **B** = Person B (Edge/Verification),
> **S** = seam/shared, **both** = co-owned. Milestones (M0–M9) refer to `docs/plan.md`.

```
kv-store/
├── go.mod                                 # module github.com/clstr-io/key-value-go, go 1.26
├── go.sum
├── Makefile                               # both: build / test / bench / chaos targets
├── clstr.yaml                             # both: challenge: kv-store, stage (per reference)
├── Dockerfile                             # both: copy from reference
├── .gitignore                             # both: copy from reference
├── README.md
├── docs/
│   ├── plan.md                            # the two-person plan (source of truth)
│   ├── file-structure.md                  # this file
│   ├── decisions.md                       # both: one-line decision log (M working agreements)
│   └── benchmarks.md                      # B: committed baseline numbers (M2, Final)
│
├── cmd/
│   └── key-value-go/
│       └── main.go                        # both: env ADDR/DATA_DIR/PEERS, api.New, Serve (B first cut)
│
├── internal/
│   ├── contract/                          # M0 — FROZEN, co-authored (nothing shared outside)
│   │   ├── contract.go                    #   Op, Command, StateMachine, Durable, Consensus, ErrNotLeader
│   │   ├── fakes.go                       #   FakeStateMachine, FakeDurable, FakeConsensus
│   │   ├── local.go                       #   LocalConsensus (single-node Consensus impl)
│   │   └── contract_test.go
│   │
│   ├── config/                            # M1 — B (reference reads env in main.go only)
│   │   ├── config.go                      #   ADDR, DATA_DIR, PEERS parsing
│   │   └── config_test.go
│   │
│   ├── store/                             # M1/M2/M9 — A (matches reference package name)
│   │   ├── memory.go                      #   M1 memoryStore (StateMachine) — extends reference
│   │   ├── errors.go                      #   M1 NotFoundError — copy from reference
│   │   ├── wal.go                         #   M1 NEW: WAL append + fsync + replay + tail repair (A)
│   │   ├── disk.go                        #   M1/M2: DiskStore wrapping WAL, batching, snapshot (A)
│   │   ├── snapshot.go                    #   M9 NEW: tmp+fsync+rename+dir-fsync+WAL truncate (B)
│   │   ├── memstore_test.go               #   M1 A: property test vs Go map oracle
│   │   ├── wal_test.go                    #   M1 A: replay; M8 B: SIGKILL matrix
│   │   ├── durable_fault_test.go          #   M1: fsync-then-panic → acked write survives (A/B)
│   │   ├── batch_test.go                  #   M2 A (B reviews post-swap)
│   │   └── snapshot_test.go               #   M9 B: crash between rename and WAL truncation
│   │
│   ├── api/                               # M1/M4 — B (matches reference package name)
│   │   ├── server.go                      #   M1 router + handlers + validation + stats
│   │   ├── redirect.go                    #   M4 NEW: 307 to leader / 503 when no leader
│   │   ├── client.go                      #   M4 NEW: redirect-following client
│   │   └── server_test.go                 #   M1 handlers; M4 redirects + client
│   │
│   ├── raft/                              # M3–M6, M8–M9 — A/B split (matches reference package)
│   │   ├── node.go                        #   M3 B: Node state, term compare, step-down — extends reference
│   │   ├── state.go                       #   M8 A: term/votedFor fsynced before RPC response
│   │   ├── election.go                    #   M3 A: timer, self-vote, fan-out, tally, heartbeat
│   │   ├── types.go                       #   M3 S: VoteRequest/AppendEntries messages (seam)
│   │   ├── transport.go                   #   M3 S: Transport interface (seam)
│   │   ├── transport_http.go              #   M3 B: peer client, timeouts, retries
│   │   ├── transport_mem.go               #   M3 B: deterministic in-process transport
│   │   ├── consensus.go                   #   M4 A: RaftConsensus behind contract.Consensus
│   │   ├── leader.go                      #   M5 A: nextIndex/matchIndex, batching, backoff
│   │   ├── follower.go                    #   M5 B: prevLog matching, conflict, truncation
│   │   ├── commit.go                      #   M6 A: median matchIndex, current-term rule
│   │   ├── apply.go                       #   M6 B: commitIndex→lastApplied apply loop
│   │   ├── snapshot.go                    #   M9 A: lastIncludedIndex/Term, compaction
│   │   ├── election_test.go               #   M3 A: ≤1 leader per term, continuous assert
│   │   ├── replication_test.go            #   M5 cross: A hostile logs, B slow/flapping leaders
│   │   ├── apply_test.go                  #   M6 B: exactly-once + unblock-after-apply
│   │   ├── persistence_test.go            #   M8 A: kill between vote decision and response
│   │   └── snapshot_test.go               #   M9: follower asks for compacted entry
│   │
│   ├── chaos/                             # M7 — shared harness (NEW)
│   │   ├── cluster.go                     #   both: 5-node harness + fault runner
│   │   ├── faults.go                      #   A: injectors (crash, partition, loss, skew, fsync)
│   │   ├── faults_test.go
│   │   ├── oracles.go                     #   B: election/log/state-machine/durability checkers
│   │   └── oracles_test.go
│   │
│   └── linearizability/                   # M7 — B (NEW)
│       ├── checker.go                     #   single-key sequential checker
│       └── checker_test.go
│
├── test/
│   └── e2e/
│       ├── kill_test.go                   # B M1: SIGKILL mid-write → restart → acked present
│       └── recovery_test.go               # B M8: SIGKILL matrix (mid-batch/fsync/snapshot/rotation)
│
└── benchmark/
    ├── bench_test.go                      # B M2: 1/10/100/1000 writers, p50/p95/p99, R/W split
    ├── results/
    │   └── baseline.csv                   # B M2: committed baseline numbers
    └── report.md                          # B Final: perf report (A reviews)
```

## Seams — never edit in the same milestone

| File | Owner |
| --- | --- |
| `internal/contract/contract.go` | Frozen — changes only by agreement, own PR |
| `internal/raft/types.go` | Shared message types |
| `internal/raft/transport.go` | Transport interface (direction-of-travel split) |

## Ownership map

| Milestone | Person A | Person B |
| --- | --- | --- |
| 0 Contract | `contract/` (co-author) | `contract/` (co-author) |
| 1 Single node | `store/` (memory, WAL, durable) | `api/`, `config/`, `test/e2e/kill_test.go` |
| 2 Performance | `store/batch.go` | `benchmark/` |
| 3 Election | `raft/election.go` | `raft/node.go`, `raft/transport_*.go`, `raft/types.go` |
| 4 Leader routing | `raft/consensus.go` | `api/redirect.go`, `api/client.go` |
| 5 Replication | `raft/leader.go` | `raft/follower.go` |
| 6 Commit | `raft/commit.go` | `raft/apply.go` |
| 7 Chaos | `chaos/faults.go` | `chaos/oracles.go`, `linearizability/` |
| 8 Recovery | `raft/state.go`, `raft/persistence_test.go` | `test/e2e/recovery_test.go`, `store/wal_test.go` |
| 9 Snapshots | `raft/snapshot.go` | `store/snapshot.go` |
| Final | correctness matrix (joint) | `benchmark/report.md` (A reviews) |

## Build order gates

- M0 done when `go test ./internal/contract/...` passes against fakes.
- Every layer has a fake before anyone depends on it.
- Merge gate (never merge red): `go test -race ./...` + chaos suite with fixed seed.
