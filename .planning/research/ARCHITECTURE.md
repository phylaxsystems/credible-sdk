# Architecture Patterns

**Domain:** Embedded state worker thread with commit-head flow control in a multi-threaded async Rust sidecar
**Researched:** 2026-03-25
**Confidence:** HIGH — derived entirely from direct source reading of the existing codebase

---

## Recommended Architecture

The target design embeds the `StateWorker` as an OS thread inside the sidecar process, mirroring the exact lifecycle pattern already used by `EventSequencing`, `CoreEngine`, and `TransactionObserver`. A single `mpsc` channel carries `CommitHead` signals from `CoreEngine` to the embedded worker. The worker buffers `BlockStateUpdate` entries in a `VecDeque<BlockStateUpdate>` (bounded at 128 entries) and only calls `Writer::commit_block()` for entries whose `block_number <= commit_head.block_number`. `MdbxSource` stops polling MDBX every 50 ms and instead reads a shared `Arc<AtomicU64>` that the state worker updates after each flush.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                            SIDECAR PROCESS                              │
│                                                                         │
│  main tokio runtime                                                     │
│  ┌──────────────┐  flume   ┌──────────────────┐  flume                  │
│  │ GrpcTransport│ ──────>  │ EventSequencing  │ ──────>                 │
│  │  (async)     │          │  (OS thread)     │         │               │
│  └──────────────┘          └──────────────────┘         ▼               │
│                                                 ┌──────────────────┐   │
│                                                 │   CoreEngine     │   │
│                                                 │   (OS thread)    │   │
│                                                 │                  │   │
│                                                 │ process_commit_  │   │
│                                                 │ head() fires     │   │
│                                                 └────────┬─────────┘   │
│                                                          │             │
│                                              CommitHead  │ mpsc::send  │
│                                              (block_num) │             │
│                                                          ▼             │
│                                           ┌──────────────────────────┐ │
│                                           │  StateWorkerThread       │ │
│                                           │  (OS thread, own tokio   │ │
│                                           │   single-thread runtime) │ │
│                                           │                          │ │
│                                           │  ┌────────────────────┐  │ │
│                                           │  │ BlockStateUpdate   │  │ │
│                                           │  │ buffer (VecDeque,  │  │ │
│                                           │  │ max 128 entries)   │  │ │
│                                           │  └────────────────────┘  │ │
│                                           │          │               │ │
│                                           │          ▼ flush ≤ N     │ │
│                                           │  Writer::commit_block()  │ │
│                                           │          │               │ │
│                                           │          ▼               │ │
│                                           │  Arc<AtomicU64>          │ │
│                                           │  (committed_head)  ──────┼─┼──> MdbxSource
│                                           └──────────────────────────┘ │
│                                                                         │
│  EthRpcSource (fallback, unchanged)                                     │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Component Boundaries

| Component | Responsibility | Owns | Communicates With |
|-----------|---------------|------|-------------------|
| `CoreEngine` (existing) | Processes `CommitHead` events from EventSequencing; updates `current_head` | Nothing new | Sends `CommitHead` copy to `StateWorkerThread` via `mpsc::Sender` |
| `StateWorkerThread` (new) | Wraps `StateWorker` in an OS thread with its own single-threaded tokio runtime; handles restart with backoff; catches panics | `StateWorker`, flume sender for exit notification, `Arc<AtomicU64>` | Receives `CommitHeadSignal` from `CoreEngine` via `mpsc::Receiver`; writes to MDBX via `Writer`; updates `committed_head` atomic |
| `StateWorkerHandle` (new) | Returned by `StateWorkerThread::spawn()`; holds `JoinHandle`, exit `oneshot::Receiver`, `Arc<AtomicU64>` for committed head | `JoinHandle`, exit channel, `Arc<AtomicU64>` | `run_sidecar_once()` wires it; `MdbxSource` reads the `Arc<AtomicU64>` |
| `CommitHead` channel (new) | Carries `CommitHeadSignal { block_number: u64 }` from `CoreEngine` to `StateWorkerThread` | `mpsc::Sender` (held by `CoreEngine`), `mpsc::Receiver` (held by `StateWorkerThread`) | Ordered delivery guarantees; `try_send` not needed — blocking `send` acceptable since receiver always ready |
| In-memory buffer (new, inside `StateWorkerThread`) | Holds `BlockStateUpdate` entries traced but not yet committed to MDBX; bounded at 128 | `VecDeque<BlockStateUpdate>` | Filled by `StateWorker::process_block()`; drained by commit-head flush handler |
| `MdbxSource` (modify existing) | Provides MDBX-backed state to `CoreEngine` | Existing `StateReader` + modified sync awareness | Reads `Arc<AtomicU64>` committed_head instead of polling; `is_synced()` compares committed_head vs required block |
| `EthRpcSource` (unchanged) | Fallback state source when MDBX is behind or worker is down | Unchanged | Unchanged |

---

## Data Flow

### CommitHead Flow (the new path)

```
Sequencer/shadow-driver
        |
        | gRPC CommitHead event
        v
GrpcTransport  -->  EventSequencing  -->  CoreEngine
                                               |
                               process_commit_head() runs
                                               |
                               ┌───────────────┴──────────────────┐
                               │ existing path                    │ new path
                               │ (cache finalization, etc.)       │
                               │                                  │
                               v                                  v
                       current_head updated          CommitHeadSignal { block_number }
                                                      sent via mpsc to StateWorkerThread
                                                               |
                                                    StateWorkerThread receives signal
                                                               |
                                                    Drain buffer: for each entry
                                                    where entry.block_number <= N:
                                                      Writer::commit_block(&entry)
                                                               |
                                                    committed_head.store(N, Release)
                                                               |
                                                    MdbxSource reads committed_head
                                                    via Arc<AtomicU64> on next is_synced()
```

### Block Tracing Flow (state worker's own loop, inside its tokio runtime)

```
execution client (WebSocket)
        |
        | newHeads subscription
        v
StateWorker::stream_blocks() / catch_up()
        |
        v
StateWorker::process_block(block_number)
        |
        | prestateTracer RPC
        v
BlockStateUpdate assembled
        |
        v
Buffer push (if buffer.len() < 128)
        OR
wait (if buffer full — backpressure: tracing pauses until flush makes room)
```

### MdbxSource Sync Check (simplified)

```
Before: spawn_block_range_poller() fires every 50ms, reads MDBX,
        stores oldest/head into Arc<AtomicU64> pair;
        is_synced() calls calculate_target_block() range intersection

After:  CoreEngine calls MdbxSource::update_committed_head(N)
        (or MdbxSource reads Arc<AtomicU64> updated by StateWorkerThread)
        is_synced() returns committed_head >= required_block_number
        No polling. No range intersection. No "went too far" guard.
```

---

## Patterns to Follow

### Pattern 1: OS Thread Spawn with Oneshot Exit Notification

All three existing blocking components (`EventSequencing`, `CoreEngine`, `TransactionObserver`) use the identical pattern. The state worker thread must follow it exactly for consistency and so `run_sidecar_once()` can `select!` on its exit notification.

```rust
// From crates/sidecar/src/event_sequencing/mod.rs (lines 232-245)
pub fn spawn(self, shutdown: Arc<AtomicBool>) -> SequencingThreadResult {
    let (tx, rx) = oneshot::channel();
    let handle = std::thread::Builder::new()
        .name("sidecar-sequencing".into())
        .spawn(move || {
            let mut sequencing = self;
            let result = sequencing.run_blocking(shutdown.as_ref());
            let _ = tx.send(result.clone());
            result
        })?;
    Ok((handle, rx))
}
```

The state worker thread adopts the same shape:

```rust
pub fn spawn(self, shutdown: Arc<AtomicBool>) -> StateWorkerThreadResult {
    let (tx, rx) = oneshot::channel();
    let handle = std::thread::Builder::new()
        .name("sidecar-state-worker".into())
        .spawn(move || {
            let result = self.run_blocking(shutdown.as_ref());
            let _ = tx.send(result.clone());
            result
        })?;
    Ok((handle, rx))
}
```

### Pattern 2: Blocking Loop with recv_timeout Shutdown Polling

Both `EventSequencing` and `CoreEngine` use `flume::Receiver::recv_timeout(100ms)` so the shutdown `AtomicBool` is checked periodically without blocking indefinitely.

The state worker's inner loop (the part that drains CommitHead signals from mpsc) uses the equivalent: `mpsc::Receiver::recv_timeout(Duration::from_millis(100))`.

### Pattern 3: Dedicated Single-Threaded Tokio Runtime for Blocking Components

The `StateWorker` itself is async (WebSocket subscription, `await` calls). It must NOT borrow the sidecar's main tokio runtime. The OS thread creates its own runtime at startup:

```rust
fn run_blocking(&self, shutdown: &AtomicBool) -> Result<(), StateWorkerError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(self.run_inner(shutdown))
}
```

This matches the requirement stated in PROJECT.md R1 and mirrors the isolation principle already implied by how EventSequencing and CoreEngine avoid doing any async work internally.

### Pattern 4: Panic Catch + Restart Backoff

`state-worker/src/main.rs` already uses `AssertUnwindSafe(...).catch_unwind()` with a restart counter. The embedded thread wraps `run_blocking()` in `std::panic::catch_unwind()`:

```rust
// Inside OS thread closure
let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
    self.run_blocking(shutdown.as_ref())
}));
match result {
    Ok(r) => r,
    Err(_) => Err(StateWorkerError::Panicked),
}
```

`StateWorkerError::Panicked` maps to `ErrorRecoverability::Recoverable`, so `run_sidecar_once()` treats it as a non-fatal exit and restarts on next iteration.

### Pattern 5: Bounded Buffer with Backpressure

The 128-block buffer is a `VecDeque<BlockStateUpdate>` owned by the state worker thread. Backpressure is simple: after tracing a block, if `buffer.len() >= 128`, the worker does not advance `next_block` until the CommitHead flush drains at least one entry. No separate semaphore or async notify is needed because the worker's inner loop checks both the CommitHead receiver and the buffer length before calling `process_block()`.

---

## Anti-Patterns to Avoid

### Anti-Pattern 1: Running StateWorker on the Sidecar's Main Tokio Runtime

**What:** Spawning the state worker as a `tokio::spawn` task rather than an OS thread.
**Why bad:** The state worker does blocking RPC calls (prestateTracer) that would starve the main async runtime. The existing EventSequencing and CoreEngine are OS threads precisely to avoid this. If the state worker falls behind it would block gRPC transport and indexer tasks.
**Instead:** Always use `std::thread::Builder::new().spawn()` with a dedicated `tokio::runtime::Builder::new_current_thread()` inside.

### Anti-Pattern 2: Sharing the MDBX Writer Across Threads

**What:** Passing `StateWriter` (which holds a raw MDBX write handle) to both the sidecar main thread and the state worker thread.
**Why bad:** MDBX write handles are not `Send + Sync` in a general sense — concurrent writes from multiple threads corrupt the database. The state worker is the sole owner of `StateWriter`; `MdbxSource` uses a separate `StateReader` handle.
**Instead:** The `StateWriter` is owned exclusively inside `StateWorkerThread`. `MdbxSource` holds a `StateReader` clone (as it does today) plus the new `Arc<AtomicU64>` committed_head pointer.

### Anti-Pattern 3: Using an Atomic for the CommitHead Signal

**What:** Replacing the `mpsc` channel with an `Arc<AtomicU64>` for commit-head notification (so the engine just does `committed_head.store(N, Release)` and the worker polls it).
**Why bad:** Atomics do not provide ordered delivery of every commit-head value. If the engine emits blocks 100, 101, 102 rapidly and the state worker polls between 100 and 102, it will see only 102, silently skipping the flush for 100 and 101. The mpsc channel delivers every value in order, which is necessary for the buffer drain logic to flush precisely up to each commit-head. PROJECT.md explicitly validates this choice.
**Instead:** `std::sync::mpsc::channel()` (or `tokio::sync::mpsc`) — unidirectional, ordered, one message per CommitHead event.

### Anti-Pattern 4: Removing EthRpcSource While State Worker May Be Behind

**What:** Deleting or disabling `EthRpcSource` as part of this milestone.
**Why bad:** The state worker can be restarting, catching up, or simply behind. During that window, `MdbxSource::is_synced()` returns false and `Sources` falls through to `EthRpcSource`. PROJECT.md explicitly puts EthRpcSource removal out of scope.
**Instead:** Leave `EthRpcSource` intact and unchanged.

### Anti-Pattern 5: Writing to MDBX Unconditionally After Each Traced Block

**What:** Calling `Writer::commit_block()` immediately after `StateWorker::process_block()` returns, without waiting for a CommitHead signal.
**Why bad:** This is the current two-process architecture that causes "went too far" bugs. MDBX height would race ahead of `current_head`, making the engine read state from blocks it has not yet committed.
**Instead:** Buffer the `BlockStateUpdate` in memory after tracing. Only flush to MDBX after receiving a CommitHead signal for that block number.

---

## Component Boundaries Summary

```
crates/sidecar/src/
├── state_worker_thread/      ← NEW module
│   ├── mod.rs                ← StateWorkerThread struct, spawn(), run_blocking()
│   ├── error.rs              ← StateWorkerError, ErrorRecoverability impl
│   └── buffer.rs             ← bounded VecDeque wrapper (optional separation)
├── cache/sources/mdbx/
│   └── mod.rs                ← MODIFY: remove range_poller, add committed_head AtomicU64 read
├── engine/
│   └── mod.rs                ← MODIFY: process_commit_head() sends to CommitHead channel
└── main.rs                   ← MODIFY: run_sidecar_once() wires CommitHead channel,
                                  spawns StateWorkerThread, adds exit to select!

crates/state-worker/          ← KEEP lib code (StateWorker struct, StateWorkerConfig),
                                  DELETE bin/main.rs entry point
```

---

## Build Order (What Depends on What)

The milestone has three requirement groups (R1, R2, R3) from PROJECT.md. The build order follows strict dependency:

### Step 1: Thread Scaffold (R1) — no functional change yet

Build `StateWorkerThread` struct with `spawn()` and `run_blocking()` that immediately returns `Ok(())`. Wire it into `run_sidecar_once()` so the thread starts and the exit notification is added to the `select!`. Verify the sidecar compiles and passes tests with the new empty thread.

**Why first:** Everything else depends on the thread existing. This step has zero risk — the empty thread does nothing but participate in shutdown.

**Dependency chain:** `StateWorkerThread::spawn()` → `ThreadHandles` extended → `run_async_components` extended.

### Step 2: CommitHead Channel (R2, part 1)

Add `mpsc::Sender<CommitHeadSignal>` to `CoreEngine` (stored in `CoreEngineConfig` or as a field). In `process_commit_head()`, after updating `current_head`, send `CommitHeadSignal { block_number }`. In `StateWorkerThread::run_blocking()`, receive from the channel (with timeout for shutdown polling) and log receipt.

**Why second:** The channel is the backbone of flow control. Nothing in R2 works without it. CoreEngine must not call the state worker synchronously — the channel decouples them.

**Dependency chain:** CommitHead channel send → CommitHead channel receive → buffer drain logic → MDBX flush.

### Step 3: In-Memory Buffer and Flush Logic (R2, parts 2–4)

Inside `StateWorkerThread`, replace the immediate `Writer::commit_block()` call with a buffer push. On receiving a CommitHead signal for block N, drain all buffer entries where `block_number <= N` to MDBX in order. After flushing, update `Arc<AtomicU64>` with N. Enforce the 128-block cap by pausing tracing when the buffer is full.

**Why third:** Requires the CommitHead channel (Step 2) to already work. The buffer and flush together implement the core invariant: "MDBX height never exceeds commit_head.block_number."

**Dependency chain:** buffer push → CommitHead receive → ordered flush → AtomicU64 update.

### Step 4: MdbxSource Simplification (R3)

Replace `spawn_block_range_poller` with reading the `Arc<AtomicU64>` updated by Step 3. Remove `calculate_target_block()`. Remove "went too far" guard. Remove circular buffer depth requirement (depth = 1 is now sufficient).

**Why fourth:** Cannot be done until the `Arc<AtomicU64>` is populated by Step 3. The AtomicU64 only has valid data once the buffer-and-flush path works end-to-end.

**Dependency chain:** AtomicU64 (Step 3) → MdbxSource simplified `is_synced()` → `calculate_target_block` removed → depth > 1 config removed.

### Step 5: Cleanup

Remove `crates/state-worker/src/main.rs` and the binary crate entry from `Cargo.toml`. Remove Helm chart configuration for the standalone state-worker Deployment. Remove `Dockerfile.state-worker` if no longer needed.

**Why last:** Only safe to remove after Steps 1–4 are verified in production. The standalone binary is the escape hatch if the embedded path has issues.

---

## Integration Points with Existing Sidecar Components

| Sidecar Component | Integration Point | Change Required |
|-------------------|-------------------|-----------------|
| `CoreEngine` | `process_commit_head()` | Add `Option<mpsc::Sender<CommitHeadSignal>>` field; send after updating `current_head`. Optional so tests without state worker still compile. |
| `run_sidecar_once()` in `main.rs` | Thread spawning, wiring | Create CommitHead mpsc channel; pass Sender to engine config, Receiver to StateWorkerThread; add StateWorkerThread to ThreadHandles; add exit rx to `run_async_components select!`. |
| `MdbxSource` | `is_synced()` / block range check | Replace polling tokio task with reading `Arc<AtomicU64>` committed_head provided at construction time. |
| `Sources` (cache layer) | No change | Unchanged — still calls `is_synced()` on each source; MdbxSource behavior change is transparent. |
| `EthRpcSource` | No change | Unchanged — remains fallback. |
| `mdbx` crate | No change | `Writer::commit_block()` API unchanged; `StateWriter` ownership moves fully inside the new thread. |
| `state-worker` crate | Library survives, binary removed | `StateWorker` struct and all supporting modules become library code imported by `sidecar`; `main.rs` deleted. |

---

## Scalability Considerations

| Concern | Current (two-process) | After (embedded) |
|---------|----------------------|-----------------|
| IPC overhead | File-system MDBX poll every 50ms | Zero — shared memory AtomicU64 load |
| Block lag | Up to 50ms polling + trace latency | Trace latency only; CommitHead flush is synchronous with engine commit |
| Memory | OS page cache for MDBX disk reads | 128 `BlockStateUpdate` structs in process memory (~few MB at peak) |
| Failure isolation | Crash of state-worker binary leaves sidecar running (EthRpcSource fallback) | Panic in state worker thread is caught; sidecar restarts cleanly; EthRpcSource fallback covers the window |
| MDBX write contention | Two processes competing for MDBX lock | Eliminated — single writer, single process |

---

## Sources

All findings are HIGH confidence — derived directly from source code in this repository:

- `crates/sidecar/src/engine/mod.rs` — CoreEngine struct, `spawn()`, `run_blocking()`, `process_commit_head()`
- `crates/sidecar/src/event_sequencing/mod.rs` — EventSequencing `spawn()` pattern (canonical reference for state worker thread pattern)
- `crates/sidecar/src/engine/queue.rs` — `CommitHead` struct, `TxQueueContents` enum
- `crates/sidecar/src/main.rs` — `run_sidecar_once()`, `ThreadHandles`, channel wiring
- `crates/sidecar/src/cache/sources/mdbx/mod.rs` — MdbxSource, `spawn_block_range_poller()`, `calculate_target_block()`, 50ms polling
- `crates/state-worker/src/worker.rs` — `StateWorker::run()`, `process_block()`, `catch_up()`
- `crates/state-worker/src/main.rs` — panic catch + restart loop pattern, `run_once()`
- `crates/mdbx/src/lib.rs` — `BlockStateUpdate`, `CommitStats`
- `.planning/PROJECT.md` — requirements R1/R2/R3, key decisions, constraints
