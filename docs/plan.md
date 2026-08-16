# clstr-io/kv-store-go — Alternative Two-Person Implementation Plan

<aside>
🧭

An alternative to the layer-by-layer phase plan. This version organizes work into **vertical, demoable milestones** with a contract-first foundation, rotating ownership, and explicit "definition of done" gates. Person A = **Core/Consensus**, Person B = **Edge/Verification**.

</aside>

## Why a different shape

The original plan splits by layer per phase (storage vs API, WAL vs recovery, election vs RPC). That works, but it has two weaknesses:

- Each phase has a hard join point — if one half slips, the other idles.
- Nothing is demoable until several layers land together.

This plan instead ships a **working system at every milestone**, keeps both people permanently unblocked via a frozen contract package, and rotates who owns "build" vs "break" so neither person tests only their own code.

---

## Milestone 0 — Contract freeze (day 0–1, both together)

Before any feature work, write one package: `internal/contract` (or `interfaces.go`). Nothing outside it is shared.

```go
// internal/contract/contract.go
package contract

type Op uint8

const (
	OpPut Op = iota
	OpDelete
	OpClear
)

type Command struct {
	Op    Op
	Key   string
	Value string
}

// Layer 1: pure in-memory state machine. No I/O, no locks leaked.
type StateMachine interface {
	Get(key string) (string, bool)
	Apply(cmd Command) error
	Snapshot() ([]byte, error)
	Restore(b []byte) error
}

// Layer 2: durability. Wraps a StateMachine.
type Durable interface {
	Apply(cmd Command) error // returns only after the write is on disk
	Recover() error
	Checkpoint() error
	Close() error
}

// Layer 3: replication. The HTTP layer only ever sees this.
type Consensus interface {
	Propose(cmd Command) error // ErrNotLeader carries the leader hint
	Leader() (addr string, known bool)
	IsLeader() bool
}

type ErrNotLeader struct{ LeaderAddr string }
```

**Rules for the whole project:**

1. This file changes only by agreement, in its own PR, never mixed with feature code.
2. Every layer has a fake: `FakeStateMachine`, `FakeDurable`, `FakeConsensus`. Both people can build against fakes from hour one.
3. `Consensus` has a single-node implementation (`LocalConsensus`) that just calls `Durable.Apply`. The HTTP layer never changes when Raft arrives.

**Done when:** both people can draw the request path `HTTP → Consensus → Durable → StateMachine` from memory, and `go test ./internal/contract/...` passes against fakes.

---

## Milestone 1 — "It works on one node" (demoable)

Target: a single node that survives `SIGKILL` without losing acknowledged writes.

|  | Person A (Core) | Person B (Edge) |
| --- | --- | --- |
| Builds | `MemStore` implementing `StateMachine`; WAL append + fsync + replay; corrupted/truncated tail handling | HTTP router: `PUT/GET/DELETE /kv/{key}`, `DELETE /kv`; validation, status codes, JSON errors, request stats; config loading for `ADDR`, `DATA_DIR`, `PEERS` |
| Verifies | Property test: random op sequences vs a Go map oracle | Kill test harness: `SIGKILL` mid-write, restart, assert acked writes present |
| Depends on | `contract` only | `FakeDurable`, then real one |

**Key invariant to encode as a test, not prose:** an HTTP 200 implies the record is on disk. Write it as a fault-injecting `Durable` that panics immediately after fsync returns and before the response — the test must still find the value after recovery.

**Demo:** `curl` a write, `kill -9`, restart, read it back.

---

## Milestone 2 — "It's fast on one node" (demoable)

Target: group commit, with numbers to prove it.

- **Person A** — batching pipeline: bounded queue, size threshold *and* time-based flush, one fsync per batch, per-caller completion signalling, backpressure when the queue is full, drain-on-shutdown.
- **Person B** — the benchmark harness that becomes permanent infrastructure: 1 / 10 / 100 / 1000 concurrent writers, p50 / p95 / p99, read and write latency split, and a committed baseline results file.

**Gate before moving on:**

- [ ]  `go test -race ./...` clean
- [ ]  batched throughput measurably beats unbatched at ≥ 10 writers
- [ ]  no unbounded goroutine or memory growth under 60 s of load
- [ ]  graceful shutdown loses zero acked writes

**Swap point:** Person B now owns the batching code's follow-up work; Person A owns the benchmark harness. Each has to read the other's code once. This is cheap now and prevents bus-factor-1 later.

---

## Milestone 3 — "Five nodes agree who is in charge" (demoable)

Instead of splitting election engine from RPC handlers (which forces constant merging in one file), split by **direction of travel**:

- **Person A — outbound**: election timer (randomized 500–1000 ms), term increment, self-vote, fan-out of `RequestVote`, vote tallying, majority math (3 of 5), candidate → leader transition, 100 ms heartbeat loop.
- **Person B — inbound**: `POST /raft/request-vote` and `POST /raft/append-entries` handlers, term comparison, step-down to follower on higher term, vote-granting rules, election timer reset on valid heartbeat, stale leader rejection. Also owns the transport (peer client, timeouts, retries) and a deterministic in-process transport for tests.

The seam is the transport interface, so nobody edits the same functions.

```go
type Transport interface {
	RequestVote(peer string, req RequestVoteRequest) (RequestVoteResponse, error)
	AppendEntries(peer string, req AppendEntriesRequest) (AppendEntriesResponse, error)
}
```

**Test the invariant, not the happy path:** at most one leader per term. Assert it continuously in the cluster harness.

**Demo:** 5 nodes up → leader elected → `kill -9` the leader → new leader within a couple of seconds → old node rejoins as follower.

---

## Milestone 4 — "Writes go to the leader" (demoable)

- **Person A** — wire `RaftConsensus` behind the existing `Consensus` interface; `Propose` returns `ErrNotLeader{LeaderAddr}`.
- **Person B** — HTTP behavior for it: `307 Temporary Redirect` to the known leader, `503 Service Unavailable` when no leader is known, plus a client that follows redirects.

Because `Consensus` was frozen at Milestone 0, the handlers written in Milestone 1 need almost no edits. That is the whole payoff of the contract freeze.

---

## Milestone 5 — "The log replicates" (demoable)

Split by **role**, and give each person a matching adversarial test suite for the *other* role.

- **Person A — leader side**: `nextIndex` / `matchIndex` per follower, entry batching into `AppendEntries`, retry and backoff, decrementing on rejection, catching up lagging followers.
- **Person B — follower side**: `prevLogIndex` / `prevLogTerm` matching, conflict detection, truncating divergent suffixes, appending, advancing from `leaderCommit`, term validation.

**Cross-testing rule:** A writes the tests that feed a follower a hostile log (gaps, stale terms, divergent suffixes); B writes the tests that make the leader face a follower that is slow, flapping, or rewound. Neither person grades their own work.

---

## Milestone 6 — "Committed means applied" (demoable)

- **Person A** — commit index: sort `matchIndex`, take the median for 5 nodes, only commit entries from the current term, advance `commitIndex` monotonically.
- **Person B** — the apply loop: single goroutine draining `commitIndex → lastApplied` into `Durable.Apply`, exactly-once application, and unblocking the waiting HTTP request only after apply.

**End state:** `PUT x=100` → Raft log → replicated to 3/5 → committed → applied → `GET x` returns 100 from any node that has caught up.

---

## Milestone 7 — "It survives being attacked" (chaos week)

This is where the plan diverges most from the original: rather than one person testing Raft and the other testing data, both people spend the week on a **shared chaos harness**, alternating between writing a fault and writing the assertion that catches it.

**Fault library (Person A owns the injectors):**

- leader crash, follower crash, simultaneous multi-node crash
- leader isolation, minority partition, majority partition
- asymmetric partition (A can reach B, B cannot reach A)
- packet loss, duplication, reordering, 2 s delays
- clock skew between nodes
- disk full and fsync error injection

**Invariant checkers (Person B owns the oracles):**

- **Election safety** — at most one leader per term
- **Log matching** — same index + term ⇒ identical prefix on all nodes
- **State machine safety** — no two nodes apply different commands at the same index
- **Durability** — every acked write is readable after healing
- **Linearizability** — record a history of ops and check it (a small Porcupine-style checker, or a simple single-key sequential checker if time is short)

**Gate:** the full fault matrix runs in CI, with a fixed seed for reproduction and a randomized seed nightly.

---

## Milestone 8 — "It comes back from the dead"

- **Person A — Raft persistence**: `currentTerm`, `votedFor`, and the log are written and fsynced *before* any RPC response that depends on them. Add a test that kills the process between the vote decision and the response, and asserts no double vote in that term after restart.
- **Person B — KV recovery**: `SIGKILL` matrix — mid-batch, mid-fsync, mid-snapshot, mid-WAL-rotation. Each case must recover to a state consistent with the acks that were actually returned.

---

## Milestone 9 — "Logs don't grow forever"

One shared snapshot format, two owners:

- **Person A** — Raft metadata: `lastIncludedIndex`, `lastIncludedTerm`, log compaction, correct behavior when a follower needs an entry that was already compacted.
- **Person B** — storage side: serialize state machine, write to a temp file, fsync, atomic rename, fsync the directory, then truncate the WAL — in that order. Recovery = snapshot + WAL entries after it.

```
      Raft Log
┌─────────────────┐
│ compacted       │ ← replaced by snapshot
│ compacted       │
│ live entries    │
└─────────────────┘
         ↓
    Snapshot  +  WAL tail
```

**Nasty case to test deliberately:** crash *between* the snapshot rename and the WAL truncation. Recovery must be idempotent, not double-applied.

---

## Final milestone — Integration and the numbers

**Correctness matrix (both, signed off jointly):**

| Scenario | Single node | 5-node cluster |
| --- | --- | --- |
| Basic CRUD | ✓ | ✓ |
| Concurrent writers | ✓ | ✓ |
| Leader election | n/a | ✓ |
| Leader crash | ✓ | ✓ |
| Follower crash | n/a | ✓ |
| Network partition | n/a | ✓ |
| Partition healing | n/a | ✓ |
| Packet loss / delay | n/a | ✓ |
| Conflicting logs | n/a | ✓ |
| Restart + WAL replay | ✓ | ✓ |
| Snapshot recovery | ✓ | ✓ |
| Linearizability check | ✓ | ✓ |

**Performance report (Person B leads, Person A reviews):** GET / PUT / DELETE latency, throughput, WAL throughput, replication latency, recovery time, and a single-node vs 5-node comparison with a written explanation of *where* the replication cost comes from.

---

## Ownership over time

| Milestone | Person A | Person B |
| --- | --- | --- |
| 0 Contract | Co-author | Co-author |
| 1 Single node | State machine + WAL | HTTP + config + crash harness |
| 2 Performance | Batching | Benchmarks (then swap review) |
| 3 Election | Outbound (timer, votes) | Inbound (handlers, transport) |
| 4 Leader routing | Consensus wiring | Redirects + client |
| 5 Replication | Leader side | Follower side |
| 6 Commit | Commit index | Apply loop |
| 7 Chaos | Fault injectors | Invariant oracles |
| 8 Recovery | Raft persistence | KV recovery matrix |
| 9 Snapshots | Raft metadata | Storage format |
| Final | Correctness matrix | Performance report |

---

## Working agreements

- **Contract PRs are separate and reviewed same-day.** Everything else can wait.
- **Fakes before features.** If a layer has no fake, the other person is blocked, which is a bug in the plan.
- **Never merge to `main` red.** `go test -race ./...` plus the chaos suite with a fixed seed is the merge gate.
- **One shared decision log page.** Every non-obvious choice (batch flush interval, timeout ranges, snapshot trigger threshold) gets one line: decision, reason, date.
- **15-minute daily sync** with exactly three items: what landed, what's blocked, what contract change is needed.
- **Interfaces before implementations, always.** If you need something from the other layer that doesn't exist yet, add it to the contract with a fake first, then keep going.

<aside>
⚠️

The single biggest failure mode for a two-person distributed systems project is serialization: one person finishes, the other waits. Every rule above exists to prevent exactly that.

</aside>