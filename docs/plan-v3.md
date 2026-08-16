# Plan v3 — Distributed & Production-Grade KV Store (Two-Developer)

> Builds on the completed single-node store from `docs/plan-v2.md`. This plan adds the
> five CV-impact features, in the recommended order:
>
> 1. **Raft multi-node cluster** — consensus, replication, leader election
> 2. **Chaos & linearizability** — fault injection + invariant checking
> 3. **Fuzz, property tests & CI** — correctness and regression gates
> 4. **TTL, atomic ops & SCAN** — richer data-model features
> 5. **Docker & TLS** — deployment and transport security
>
> Owner tags: **A** = Developer A (Core Engine), **B** = Developer B (API & Network),
> **S** = seam/shared, **both** = joint.

---

## Contract-first rule (applies to every phase)

- Any change to `pkg/storage/engine.go`, `pkg/raft/types.go`, or `pkg/raft/transport.go`
  lands as its **own PR, agreed by both, before the feature work that needs it**.
- **Fakes before features.** If a layer needs something that does not exist yet, add it
  to the contract with a fake, then build against it.
- Merge gate (never merge red): `go vet ./...`, `go test -race ./...`, and (from
  Phase 2 on) the chaos suite with a fixed seed.

---

## Phase 1 — Raft multi-node cluster (~2–3 weeks)

**Goal:** a 5-node cluster where every acknowledged write is replicated to a majority,
a leader is always (re)elected, and the API redirects clients to the leader.

**Architecture change:** `raft.Node` wraps `storage.DiskStore` and implements
`storage.Engine`, so Developer B's HTTP handlers do **not** change. `Node.Put/Delete`
proposes to consensus; the HTTP layer only needs to handle a typed "not leader" error.

### Contract changes (joint, before any feature work)

```go
// pkg/storage/engine.go
type NotLeaderError struct{ LeaderAddr string } // LeaderAddr = "" when unknown
func (e *NotLeaderError) Error() string

// pkg/raft/types.go
type Entry struct{ Term, Index int; Cmd storage.Command }  // Command = {op,key,value}
type VoteRequest struct{ Term int; CandidateID string; LastLogIndex, LastLogTerm int }
type VoteResponse struct{ Term int; VoteGranted bool }
type AppendEntriesRequest struct {
	Term, LeaderID, PrevLogIndex, PrevLogTerm, LeaderCommit int // + []Entry
}
type AppendEntriesResponse struct{ Term int; Success bool }

// pkg/raft/transport.go — the seam, nobody edits the same file
type Transport interface {
	RequestVote(peer string, req VoteRequest) (VoteResponse, error)
	AppendEntries(peer string, req AppendEntriesRequest) (AppendEntriesResponse, error)
}
```

Fakes: `FakeEngine`, `FakeTransport` (both developers build against them from day one).

### Milestone 1.1 — Election (split by direction of travel)

| Developer A — outbound | Developer B — inbound + transport |
| --- | --- |
| Election timer (randomized 500–1000 ms), term increment, self-vote, fan-out of `RequestVote`, vote tally, majority math (`floor(N/2)+1`), candidate→leader transition, 100 ms heartbeat loop | `transport.go` + `transport_http.go` (peer client, timeouts, retries) + `transport_mem.go` (deterministic in-process transport), `node.go` (role state, term comparison, step-down to follower on higher term, vote-granting rules, election-timer reset on valid heartbeat) |

**Invariant test (not just happy path):** at most one leader per term, asserted
continuously in a 5-node in-process harness.

**Demo:** 5 nodes up → leader elected → `kill -9` the leader → new leader within a couple
of seconds → old node rejoins as follower.

### Milestone 1.2 — Leader routing

| Developer A | Developer B |
| --- | --- |
| `raft.Node` implements `storage.Engine`; `Put/Delete/Clear` call consensus and return `NotLeaderError{LeaderAddr}`; `Leader()`/`IsLeader()` helpers | HTTP handlers catch `NotLeaderError`: `307 Temporary Redirect` to the leader, `503` when no leader is known; a redirect-following client; `NODE_ID` and `PEERS` env parsing in `cmd/server/main.go` |

Because handlers already talk to an `Engine`, this milestone requires almost no handler
edits — the payoff of the contract freeze.

**Demo:** write to a follower → transparent redirect to the leader → value lands on the
majority.

### Milestone 1.3 — Log replication (cross-testing rule: A writes follower-hostile tests, B writes leader-hostile tests)

| Developer A — leader side | Developer B — follower side |
| --- | --- |
| `nextIndex`/`matchIndex` per follower, entry batching into `AppendEntries`, retry + backoff, decrement on rejection, catch-up of lagging followers | `prevLogIndex`/`prevLogTerm` matching, conflict detection, truncating divergent suffixes, appending, advancing from `leaderCommit`, term validation |

**Cross-tests:** A feeds a follower hostile logs (gaps, stale terms, divergent suffixes);
B makes the leader face a slow, flapping, or rewound follower. Neither grades their own.

### Milestone 1.4 — Commit & apply

| Developer A — commit index | Developer B — apply loop |
| --- | --- |
| Sort `matchIndex`, take the median for 5 nodes, only commit entries from the current term, advance `commitIndex` monotonically | Single goroutine draining `commitIndex→lastApplied` into `DiskStore`, exactly-once application, unblocking the waiting HTTP request only after apply |

**End state:** `PUT x=100` → Raft log → replicated to 3/5 → committed → applied →
`GET x` returns 100 from any caught-up node.

### Milestone 1.5 — Raft persistence

| Developer A | Developer B |
| --- | --- |
| `currentTerm`, `votedFor`, and the log are written and fsynced **before** any RPC response that depends on them; test that kills the process between the vote decision and the response, asserting no double vote in that term after restart | KV recovery matrix across the cluster: `SIGKILL` mid-batch / mid-fsync / mid-snapshot / mid-WAL-rotation; every case must recover to the state implied by the acks actually returned |

### Milestone 1.6 — Snapshot & compaction

| Developer A — Raft metadata | Developer B — storage side |
| --- | --- |
| `lastIncludedIndex`/`lastIncludedTerm`, log compaction, `InstallSnapshot` in the transport, correct behavior when a follower needs an already-compacted entry | Serialize state machine, write temp file, fsync, atomic rename, fsync the directory, then truncate the WAL — in that order; recovery = snapshot + WAL tail |

**Nasty case to test deliberately:** crash *between* the snapshot rename and the WAL
truncation; recovery must be idempotent.

### Phase 1 gates

- [ ] `go test -race ./...` clean, including the in-process 5-node harness
- [ ] ≤ 1 leader per term holds under continuous assertion
- [ ] `kill -9` leader → new leader in a couple of seconds
- [ ] every acked write survives a full cluster restart

---

## Phase 2 — Chaos & linearizability (~1 week)

**Goal:** prove the cluster is safe under faults, with reproducible CI runs.

### Shared harness (`pkg/chaos/cluster.go`, both)

A 5-node in-process cluster on the in-memory transport, driven by a fault schedule from a
seeded RNG. One shared runner, fixed seed in CI, random seed nightly.

| Developer A — fault injectors (`faults.go`) | Developer B — invariant oracles (`oracles.go`) |
| --- | --- |
| Leader crash, follower crash, simultaneous multi-node crash; leader isolation, minority partition, majority partition; asymmetric partition (A reaches B, B cannot reach A); packet loss / duplication / reordering / 2 s delays; clock skew between nodes; disk-full and fsync-error injection | **Election safety** — at most one leader per term; **log matching** — same index+term ⇒ identical prefix on all nodes; **state-machine safety** — no two nodes apply different commands at the same index; **durability** — every acked write readable after healing |

**Linearizability** (`pkg/linearizability/checker.go`, B): record an op history and check
it — a small single-key sequential checker (Porcupine-style if time allows). Wire into the
harness so every acked write + read gets checked.

**Gate:** the full fault matrix runs in CI with a fixed seed; a randomized seed nightly.

---

## Phase 3 — Fuzz, property tests & CI (~1 week)

**Goal:** machine-checked correctness + a repeatable pipeline.

| Developer A — storage property/fuzz | Developer B — API fuzz + CI |
| --- | --- |
| Property test: random op sequences vs a Go map oracle (reuse `memstore` as oracle); Go fuzz target for the WAL binary format — corrupt/truncated/unknown-opcode input must fail cleanly or replay to a state consistent with the decoded prefix | Go fuzz targets for HTTP handlers — malformed paths, invalid UTF-8 keys, partial/oversized bodies, `Content-Length` mismatches; `.github/workflows/ci.yml`: `go vet`, `go test -race ./...`, coverage gate (~80%), short fuzz run, chaos suite with the fixed seed, benchmark regression check |

**Working agreements:** each fuzz corpus is committed; a discovered crash opens a bug,
never a silent fix. CI is the merge gate for everyone.

---

## Phase 4 — TTL, atomic ops & SCAN (~1–1.5 weeks)

**Goal:** richer data-model features, all persisted correctly.

### Contract changes (joint)

WAL gains new opcodes (`opExpire`, `opIncr`, `opCAS`); the snapshot format is versioned so
old snapshots still load. `Engine` gains `Incr(key)`, `CAS(key, old, new)`, `Expire(key, ttl)`,
`Scan(cursor, count, pattern)`. All are agreed in one PR before implementation.

| Developer A — storage | Developer B — API |
| --- | --- |
| TTL: per-entry expiry with lazy (on read) + active (background ticker) expiration; expiry persisted via `opExpire` so TTLs survive restart; `Incr` (base-10 parse, error on non-numeric) and `CAS` serialized through the WAL as single commands; `Scan` — cursor-based, **non-blocking** (no full map lock), page iteration; snapshots persist TTLs too | Endpoints: `PUT /kv/{key}/expire?ttl=` (or `TTL` header), `POST /kv/{key}/incr`, `PUT /kv/{key}/cas` (old/new in body), `GET /kv?cursor=&count=&pattern=`; validation (TTL/count bounds, pattern length), status codes — `400` invalid, `404` missing, `409` CAS mismatch, `422` non-numeric INCR; opaque cursor encoding; metrics for keys and expired count |

**Invariant tests:** TTLs are not resurrected after restart; `Incr` is atomic under
concurrency; `CAS` fails exactly when the value changed; `Scan` sees a consistent snapshot
per page and terminates.

---

## Phase 5 — Docker & TLS (~3–5 days)

**Goal:** deployable, transport-secure server; 5-node cluster with one command.

| Developer A — storage hardening | Developer B — deployment & TLS |
| --- | --- |
| Optional AES-GCM **encryption at rest** for `wal.log`/`snapshot.dat` behind a `KEY` env var; rotation-friendly layout (key file, not hardcoded); review of TLS wiring | Multi-stage `Dockerfile`, `.dockerignore`, healthcheck (liveness/readiness split), `docker-compose.yml` with 5 node services (distinct `NODE_ID`/`PEERS`, named volumes for `DATA_DIR`); TLS support in `cmd/server/main.go` — cert/key env vars (`TLS_CERT`, `TLS_KEY`), `http.Server` with `TLSConfig`; graceful shutdown under Docker `SIGTERM` verified |

**Demo:** `docker compose up` → 5-node cluster → `curl` any node → forwarded to leader →
restart a container → cluster heals.

---

## Ownership over time

| Phase | Developer A (Core) | Developer B (API/Network) |
| --- | --- | --- |
| 1.1 Election | Outbound: timer, votes, majority, heartbeat | Inbound: handlers, `node.go`, transport |
| 1.2 Routing | Consensus wiring (`Node` as `Engine`) | Redirects, client, config |
| 1.3 Replication | Leader side (`nextIndex`/`matchIndex`) | Follower side (conflict/truncate) |
| 1.4 Commit/apply | Commit index | Apply loop |
| 1.5 Persistence | Raft state fsync | KV recovery matrix |
| 1.6 Snapshots | Raft metadata, compaction | Storage format, idempotent recovery |
| 2 Chaos | Fault injectors | Invariant oracles + linearizability |
| 3 Fuzz/CI | Storage property tests + WAL fuzz | API fuzz + CI workflow |
| 4 Data features | TTL, INCR/CAS, SCAN, snapshot v2 | Endpoints, validation, metrics |
| 5 Docker/TLS | Encryption at rest | Docker, Compose, TLS |

## New files

```
pkg/raft/            types.go(S) transport.go(S) transport_http.go(B)
                     transport_mem.go(B) node.go(B) election.go(A)
                     leader.go(A) follower.go(B) commit.go(A)
                     apply.go(B) persistence.go(A) snapshot.go(A)
pkg/chaos/           cluster.go(both) faults.go(A) oracles.go(B)
pkg/linearizability/ checker.go(B)
pkg/storage/         ttl.go(A) ops.go(A) scan.go(A)   (+ engine.go, wal.go changes)
.github/workflows/   ci.yml(B)
Dockerfile           docker-compose.yml  (B)
cmd/server/          main.go config for NODE_ID/PEERS/TLS (B)
```

## Working agreements (carried over from plan-v2)

- Contract PRs are separate and reviewed same-day. Everything else can wait.
- Fakes before features — a blocked teammate is a bug in the plan.
- Never merge to `main` red: `go vet`, `go test -race`, chaos suite (fixed seed).
- 15-minute daily sync: what landed, what's blocked, what contract change is needed.
- Interfaces before implementations, always.

---

## Effort summary

| Phase | Est. calendar time (2 devs, full-time) |
| --- | --- |
| 1. Raft cluster | 2–3 weeks |
| 2. Chaos & linearizability | ~1 week |
| 3. Fuzz, property tests & CI | ~1 week |
| 4. TTL, atomic ops & SCAN | 1–1.5 weeks |
| 5. Docker & TLS | 3–5 days |
| **Total** | **~6–8 weeks** |