# Embedded State Worker And Latest-State MDBX Design

## Status

Approved interactively on 2026-03-24.

## Context

Today the sidecar and the `state-worker` operate as separate processes against the same MDBX
database. That creates an avoidable bottleneck around two processes opening the same store and
forces the sidecar cache to treat MDBX as a buffered historical replica instead of as a local
canonical state cache.

The new design embeds the state worker inside the sidecar as an independently supervised OS thread.
The worker continues to trace new heads continuously, but it only writes those traced updates to
MDBX when the core engine advances commit head. MDBX becomes a latest-state-only store rather than a
replicated circular buffer.

## Goals

- Run the state worker as an OS thread inside the sidecar.
- Ensure a panic in the state worker does not crash the sidecar and triggers automatic restart.
- Make the core engine the authority for what may be committed to MDBX.
- Keep tracing off the sidecar hot path so block processing latency does not regress.
- Refactor MDBX initialization so sidecar components share one runtime-local MDBX handle.
- Remove circular-buffer state replicas and timer polling from `MdbxSource`.
- Preserve `EthRpcSource` as a fallback whenever MDBX is down or behind.

## Non-Goals

- Adding backpressure or memory caps for staged state diffs.
- Preserving historical point-in-time state inside MDBX.
- Reintroducing synced-too-far handling or circular-buffer reconciliation.
- Solving a full canonical rewind design for non-monotonic commit heads.

## Key Decisions

### 1. Embedded Worker, Supervised By Sidecar

The sidecar will own a `StateWorkerSupervisor` alongside the existing engine, event sequencing, and
transaction observer threads. The supervisor will spawn the worker on a dedicated named OS thread
and keep an in-memory control/status handle shared with the rest of the sidecar.

The worker must be failure-isolated from the rest of the sidecar:

- if the worker returns an error, the supervisor logs it and restarts it
- if the worker panics, the supervisor catches the thread failure, marks the worker unhealthy, and
  restarts it
- worker failure must not trigger sidecar shutdown

The sidecar remains the long-lived process boundary. The worker becomes an internal runtime
component.

### 2. Continuous Head Tracing, Commit-Gated MDBX Writes

The worker still subscribes to `newHeads` and still traces blocks as they arrive from the execution
client. That work is intentionally decoupled from commit head.

For each traced block, the worker builds a `BlockStateUpdate` and stores it in an in-memory staging
area keyed by block number. The staging area is sequential and append-only from the current durable
MDBX height forward.

`CommitHead` does not tell the worker to fetch a block. It tells the worker which already-fetched
and already-staged blocks are now allowed to be flushed to MDBX.

This produces the required behavior:

- tracing continues even if commit head is slow
- commit head is the only authority that permits MDBX mutation
- MDBX never advances beyond `commit_head.block_number`

### 3. Unbounded In-Memory Staging

The staged state-diff buffer is intentionally unbounded. If commit head stalls while chain head
continues to advance, the worker continues tracing and retains staged diffs in memory until commit
head catches up.

This is a deliberate tradeoff accepted for this refactor:

- no secondary backpressure policy is introduced
- no disk-backed pre-commit queue is introduced
- worst-case memory exhaustion is acceptable

Important consequence: a worker thread panic is recoverable, but an OOM can still terminate the
entire sidecar process. That risk is accepted by design.

### 4. Latest-State-Only MDBX

MDBX will be refactored from a circular-buffered historical store into a latest-state canonical
store.

The new MDBX model keeps only:

- the latest canonical account/storage/bytecode state
- metadata such as `mdbx_synced_through`
- any latest-block metadata still needed for observability or block-hash lookups

MDBX will no longer maintain:

- namespace replicas
- circular-buffer depth
- historical state snapshots per block
- synced-too-far reconciliation logic

When the worker flushes staged blocks to MDBX, it applies them sequentially from
`mdbx_synced_through + 1` through the highest committed block currently allowed, updating durable
latest state after each block.

### 5. Engine Owns The Commit Boundary

`CoreEngine::process_commit_head` remains the sole authority for advancing canonical committed
state.

After commit head has been accepted by the engine, the engine publishes the committed block number
to the supervisor through a small non-blocking interface such as `publish_commit_head(block_number)`.

The engine does not wait for the worker to flush MDBX. The worker consumes that commit target
asynchronously and advances durable state in the background.

This keeps the expensive tracing and write path off the engine hot path and avoids regressions in
block processing latency.

### 6. Shared Sidecar MDBX Runtime

MDBX initialization inside the sidecar should move behind a single shared accessor, for example a
`OnceLock`, `LazyLock`, or equivalent runtime-owned holder.

The goal is not global mutable state for its own sake. The goal is a single sidecar-local MDBX
runtime that can be shared by:

- the embedded state worker writer path
- `MdbxSource`
- any future sidecar-local MDBX readers

This replaces the current model where sidecar state sources construct separate MDBX reader clients
against configuration values.

### 7. `MdbxSource` Stops Polling

`MdbxSource` no longer polls MDBX on a timer to infer available block range.

Instead, the supervisor publishes worker status in shared memory, including at least:

- `latest_head_seen`
- `highest_staged_block`
- `mdbx_synced_through`
- worker health / restarting status

`MdbxSource` uses that status to decide whether it is synced enough to serve reads. If the worker is
behind or unavailable, `MdbxSource` reports itself unsynced and the sidecar cache falls through to
`EthRpcSource`.

Because MDBX is latest-state-only, `MdbxSource` no longer needs range intersection logic or a
target-block poller. It only needs to know whether durable latest state is caught up enough for the
sidecar’s current minimum synced height.

### 8. `EthRpcSource` Remains Fallback

`EthRpcSource` remains configured as the fallback source whenever:

- the worker is restarting
- MDBX has not yet been flushed to the required committed block
- MDBX initialization failed

This is critical because worker restarts discard staged in-memory diffs. After restart, the worker
must retrace from durable MDBX state forward before local state becomes current again.

The cache therefore behaves as follows:

- prefer MDBX when `mdbx_synced_through` is sufficiently current
- fall back to `EthRpcSource` when MDBX is behind or the worker is unhealthy

## Detailed Runtime Flow

### Startup

1. The sidecar initializes the shared MDBX runtime.
2. The supervisor reads `mdbx_synced_through` from MDBX metadata.
3. The supervisor spawns the worker thread.
4. The worker connects to the execution client and subscribes to `newHeads`.
5. The worker backfills from `mdbx_synced_through + 1` to current head into the in-memory staging
   area.
6. As commit head messages later arrive from the engine, the worker flushes staged blocks into MDBX
   up to the committed target.

### Steady State

1. Chain head advances.
2. The worker traces the new block and stages its `BlockStateUpdate` in memory.
3. The engine later accepts `CommitHead(N)`.
4. The engine publishes `N` to the supervisor.
5. The worker flushes all staged blocks `<= N` into MDBX, in order.
6. Each staged block is dropped from memory immediately after a successful MDBX commit.

### Restart Recovery

If the worker thread exits or panics:

1. The supervisor marks the worker unhealthy.
2. All in-memory staged diffs are discarded.
3. The supervisor preserves the latest published commit target.
4. The worker is restarted after a short backoff.
5. On restart, the worker reads `mdbx_synced_through` from MDBX, reconnects, and retraces forward
   to rebuild staged state.
6. Once staged diffs cover the latest commit target again, flush resumes.

This recovery model is correct because only MDBX is treated as durable. Anything newer than
`mdbx_synced_through` is recoverable by retracing.

## MDBX Refactor Direction

The MDBX crate should be simplified around latest-state semantics.

Expected structural changes:

- remove `CircularBufferConfig` from sidecar-facing MDBX usage
- remove namespaced replica logic used only for circular buffering
- remove available-range APIs such as `get_available_block_range()`
- replace block-scoped state reads with latest-state reads, or internally map sidecar reads to the
  latest durable block
- keep durable metadata for the latest committed block height and any latest-block metadata still
  needed by readers

The new writer contract becomes:

- apply block diff to latest durable state
- durably record `mdbx_synced_through = block_number`
- make that block the new latest canonical durable state

## Sidecar Integration Direction

The cleanest integration is to reuse state-worker logic as a library rather than duplicating it in
the sidecar binary.

Recommended direction:

- move reusable worker code from `crates/state-worker` behind a library API
- keep `crates/state-worker/src/main.rs` only as a thin wrapper if standalone execution remains
  useful for tests or tooling
- let `crates/sidecar` depend on the reusable worker runtime and supervisor types

That preserves existing tracing and block-diff construction logic while changing the runtime model
from “separate process” to “embedded supervised thread.”

## Configuration Changes

The sidecar configuration should converge toward one embedded state-worker configuration instead of
multiple independent MDBX replicas.

Directionally:

- remove or deprecate sidecar MDBX depth/buffer configuration
- remove the assumption that multiple MDBX state replicas may exist
- keep execution-node connection and genesis configuration required by the embedded worker
- keep optional `EthRpcSource` configuration as the fallback source

Backward-compatibility shims are acceptable during rollout, but the target model is a single
sidecar-owned MDBX plus optional RPC fallback.

## Testing Strategy

The refactor should be validated with tests that cover both behavior and latency risk.

### Required Behavioral Tests

- worker traces new heads into memory without mutating MDBX before commit head
- commit head flushes staged blocks sequentially into MDBX
- MDBX never advances beyond the latest committed block
- `MdbxSource` does not poll on a timer
- worker thread panic does not crash the sidecar and triggers restart
- restart discards staged diffs and rebuilds from `mdbx_synced_through + 1`
- `EthRpcSource` serves reads while the worker is behind or restarting
- only one sidecar MDBX runtime is initialized and shared
- sidecar works without multiple MDBX replicas

### Required Performance Validation

- existing sidecar block-processing latency benchmark shows no regression
- commit head processing remains non-blocking with respect to tracing and MDBX flush

## Assumptions And Risks

- Commit heads are assumed to be monotonic for this refactor. A true durable rewind design for
  latest-state-only MDBX is out of scope.
- Worker restart is safe because staged state is recoverable by retracing.
- OOM remains possible because staged diffs are intentionally unbounded.
- Local MDBX freshness may temporarily lag after restart, but correctness is preserved by
  `EthRpcSource` fallback.

## Acceptance Criteria Mapping

- State worker runs as an OS thread inside the sidecar:
  satisfied by the supervisor-managed embedded worker thread.
- A panic in the state worker does not crash the sidecar; it restarts automatically:
  satisfied by supervisor restart behavior.
- MDBX is only updated up to `commit_head.block_number`:
  satisfied by commit-gated flush semantics.
- `MdbxSource` does not poll MDBX on a timer:
  satisfied by supervisor-published shared status.
- Multiple state replicas no longer required:
  satisfied by latest-state-only MDBX.
- `EthRpcSource` fallback works when state worker is down/behind:
  satisfied by cache sync-status gating.
- No regression in block processing latency:
  satisfied by keeping tracing and MDBX flush off the engine hot path and verified by benchmarks.
