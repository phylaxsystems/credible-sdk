# Phase 2: CommitHead Flow Control — Research

**Researched:** 2026-03-25
**Domain:** In-process CommitHead mpsc channel, bounded VecDeque buffer, MDBX-gated flush, metrics instrumentation
**Confidence:** HIGH — all findings derived from direct source reading of the existing codebase and prior project-level research

---

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| FLOW-01 | State worker buffers `BlockStateUpdate` in a bounded `VecDeque` after tracing each block | Buffer design section; `BlockStateUpdate` is already `Clone + Serialize + Deserialize`; `VecDeque` is stdlib |
| FLOW-02 | Core engine sends `CommitHeadSignal { block_number }` via mpsc channel after each `process_commit_head` | `process_commit_head` injection point identified at line 1604 of `engine/mod.rs`; `CoreEngine` struct fields documented |
| FLOW-03 | State worker flushes buffered updates where `block_number <= commit_head` to MDBX on signal | Flush loop pattern documented; `Writer::commit_block()` API verified in `mdbx/src/lib.rs:462` |
| FLOW-04 | MDBX height never exceeds `current_commit_head.block_number` | Invariant enforced by only calling `commit_block` inside the flush handler, never on buffer push |
| FLOW-05 | Buffer bounded at 128 blocks — state worker pauses tracing when buffer is full (backpressure) | Buffer-full pause pattern documented; `VecDeque::len() >= 128` check before `process_block` |
| FLOW-06 | On restart, state worker resumes from last committed block in MDBX via `compute_start_block` | `compute_start_block` already exists in `state-worker/src/worker.rs:161`; called with `start_override: None` |
| OBSV-01 | Buffer utilization gauge (`state_worker_buffer_depth`) updated after each trace/flush | `metrics::gauge!` pattern verified in `sidecar/src/metrics.rs` and `state-worker/src/metrics.rs` |
| OBSV-02 | Restart counter (`state_worker_restarts_total`) incremented on each thread restart | `restart_count` already tracked in `run_thread_loop`; needs `counter!` emission |
| OBSV-03 | Buffer-full pause counter (`state_worker_buffer_full_pauses_total`) incremented when tracing pauses | New counter; emitted in the buffer-full branch of the trace loop |
</phase_requirements>

---

## Summary

Phase 2 extends the `StateWorkerThread` scaffold from Phase 1 with the functional core of the milestone: a `flume::unbounded()` `CommitHead` channel from `CoreEngine` to `StateWorkerThread`, a `VecDeque<BlockStateUpdate>` buffer bounded at 128 entries, and a flush handler that drains buffer entries whose `block_number <= commit_head` to MDBX on each signal. After each flush, the thread also performs a best-effort flush of all ready entries on shutdown.

The work spans three files: `state_worker_thread/mod.rs` (replace `run_blocking_inner` with full `StateWorker::run()` logic plus channel receive), `engine/mod.rs` (add `commit_head_tx: Option<flume::Sender<CommitHeadSignal>>` field and send at end of `process_commit_head`), and `main.rs` (construct the `flume::unbounded()` channel and pass sender/receiver during wiring). Three metrics are added as standalone `gauge!` and `counter!` calls following the existing `state-worker/src/metrics.rs` pattern.

The critical design constraint verified from PITFALLS.md is that the CommitHead channel **must be unbounded** — a bounded channel creates a circular deadlock between the full 128-entry buffer (blocking tracing) and a full CommitHead channel (blocking the engine send). The existing codebase uses `flume::unbounded()` for all inter-OS-thread channels in `run_sidecar_once()`.

**Primary recommendation:** Use `flume::unbounded()` for the CommitHead signal channel; bind backpressure exclusively to the 128-entry `VecDeque` buffer. Replace `run_blocking_inner`'s no-op loop with a `StateWorker::run()` call wrapped by the CommitHead receiver loop. Add `CommitHeadSignal` as a new type in `state_worker_thread/mod.rs` — it does not need to share the engine's `CommitHead` type.

---

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `flume` | 0.12.0 | `unbounded()` CommitHead signal channel | Established cross-thread channel primitive for all OS thread communication in this codebase; `recv_timeout` matches existing shutdown-poll loops |
| `std::collections::VecDeque` | stdlib | 128-entry bounded in-memory buffer | No external crate needed; O(1) push-back and front-drain; already used in similar contexts |
| `metrics` | 0.24.3 | `gauge!`, `counter!` for OBSV-01/02/03 | Workspace standard; `state-worker/src/metrics.rs` is the canonical pattern to follow |
| `mdbx::Writer::commit_block` | workspace | Write `BlockStateUpdate` to MDBX | Existing API, no changes required; returns `CommitStats` |
| `mdbx::Reader::latest_block_number` | workspace | Resume from last persisted block (FLOW-06) | Existing API called by `StateWorker::compute_start_block` |
| `state-worker::StateWorker` | workspace | Block tracing + state assembly | Already contains all logic for `process_block`, `catch_up`, `stream_blocks`, `compute_start_block` |

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `tokio::sync::broadcast` | tokio 1.49.0 | Shutdown signal inside StateWorker's own tokio runtime | Existing `StateWorker::run` signature takes `broadcast::Receiver<()>` |
| `std::sync::atomic::AtomicU64` | stdlib | `Arc<AtomicU64>` committed_head (written Phase 2, consumed Phase 3) | Construct and write with `Release` ordering in Phase 2; Phase 3 wires `MdbxSource` to read it |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `flume::unbounded()` | `flume::bounded(128)` | Bounded CommitHead channel creates deadlock: buffer full + channel full = both threads blocked. NEVER use bounded for CommitHead. |
| `flume::unbounded()` | `tokio::sync::mpsc::unbounded_channel()` | tokio mpsc is async-native; engine OS thread calls `send()` synchronously; `flume` works from sync and async contexts without a runtime handle requirement |
| `VecDeque<BlockStateUpdate>` | `Vec<BlockStateUpdate>` | Vec drain from front is O(n); VecDeque drain from front is O(1); buffer will frequently drain from the lowest index |

**Installation:** No new Cargo.toml changes required. `flume`, `metrics`, `mdbx`, and `state-worker` crate are all already workspace dependencies of the `sidecar` crate.

---

## Architecture Patterns

### Recommended Project Structure

Phase 2 adds one new file and modifies three existing files:

```
crates/sidecar/src/
├── state_worker_thread/
│   ├── mod.rs          ← MODIFY: replace run_blocking_inner no-op with StateWorker::run()
│   │                      Add: CommitHeadSignal type, flume Receiver field, VecDeque buffer
│   │                      Add: flush_ready_blocks(), metrics helpers
│   └── error.rs        ← MODIFY: add new error variants (StateWorkerConfig, MdbxWrite, Trace)
├── engine/
│   └── mod.rs          ← MODIFY: add commit_head_tx field, send in process_commit_head()
└── main.rs             ← MODIFY: create flume::unbounded() channel, pass to engine + worker

crates/state-worker/src/
└── worker.rs           ← READ ONLY: StateWorker::run() called, not modified
```

### Pattern 1: CommitHead Signal Type

Define `CommitHeadSignal` in `state_worker_thread/mod.rs`. It carries only the block number extracted from the engine's `CommitHead.block_number` (a `U256` — needs `.saturating_to::<u64>()` conversion to match `BlockStateUpdate.block_number: u64`).

```rust
// In crates/sidecar/src/state_worker_thread/mod.rs
/// Signal from CoreEngine to StateWorkerThread after each committed block.
#[derive(Debug, Clone, Copy)]
pub struct CommitHeadSignal {
    pub block_number: u64,
}
```

**Why not reuse `engine::queue::CommitHead`:** That struct is `pub(crate)` to the engine module and carries iteration-specific fields (`selected_iteration_id`, `last_tx_hash`) that the state worker does not need. The signal channel should carry minimal data — just the committed block number.

### Pattern 2: CoreEngine Sender Field

Add `commit_head_tx: Option<flume::Sender<CommitHeadSignal>>` to `CoreEngine<DB>` struct. Wire it via `CoreEngineConfig` so tests that do not need the state worker compile without changes.

```rust
// In CoreEngine struct (crates/sidecar/src/engine/mod.rs ~line 518):
/// Optional sender for CommitHead signals to the state worker thread.
/// `None` when the state worker is not wired (tests, legacy configs).
commit_head_tx: Option<flume::Sender<CommitHeadSignal>>,
```

Send at the END of `process_commit_head`, after `self.first_commit_head_processed = true` and before `Ok(())`:

```rust
// Source: engine/mod.rs process_commit_head, after line 1708
if let Some(ref tx) = self.commit_head_tx {
    let signal = CommitHeadSignal {
        block_number: commit_head.block_number.saturating_to::<u64>(),
    };
    // flume::unbounded() send is infallible unless receiver is dropped.
    // If the state worker has exited, the send error is non-fatal —
    // EthRpcSource covers the window.
    let _ = tx.send(signal);
}
```

**Why `let _ =`:** The state worker may have exited and be restarting. A dropped receiver returns `SendError` from `flume::Sender::send()`. Ignoring this is correct — the engine must continue processing regardless of state worker health.

### Pattern 3: StateWorkerThread — CommitHead Receive Loop

Replace `run_blocking_inner` in `state_worker_thread/mod.rs` with a function that:

1. Constructs the tokio runtime (already done in Phase 1)
2. Constructs `StateWorker` (requires provider URL, MDBX path, genesis from config)
3. Runs the StateWorker inner loop with a modified logic:
   - On each `process_block()` return: push `BlockStateUpdate` to buffer instead of writing to MDBX directly
   - On each `CommitHead` signal received (via `flume::Receiver::recv_timeout`): flush all buffer entries where `block_number <= signal.block_number`
   - When `buffer.len() >= 128`: pause tracing (do not call `process_block`) until a flush drains space

The StateWorker currently calls `self.writer_reader.commit_block(&update)` directly inside `process_block`. For the embedded case this coupling must be broken. The two approaches are:

**Approach A (recommended):** Modify `StateWorker::process_block` to return `BlockStateUpdate` instead of committing directly. The embedded caller buffers it; the standalone binary commits immediately. This requires a signature change to `process_block`.

**Approach B:** Keep `StateWorker` unmodified; fork the tracing logic. Extract `fetch_block_state` and `apply_system_calls` from `StateWorker` and call them directly in `StateWorkerThread`.

**Recommendation: Approach A.** `process_block` already assembles `BlockStateUpdate` and then calls `commit_block`. Separating the two steps is a minimal, clean refactor. The standalone `state-worker/src/main.rs` binary is being removed in Phase 3 anyway; the `StateWorker` library code can be modified freely.

```rust
// Sketch of modified process_block signature in state-worker/src/worker.rs
pub async fn process_block(&mut self, block_number: u64) -> Result<BlockStateUpdate> {
    // ... trace, apply system calls ...
    // Return update instead of calling commit_block
    Ok(update)
}
```

The standalone binary's `run_once` would then call `worker.process_block(n).await?` and immediately `writer_reader.commit_block(&update)` to preserve existing behavior.

### Pattern 4: Buffer Flush Handler

```rust
// Inside StateWorkerThread's async inner loop
fn flush_ready_blocks(
    buffer: &mut VecDeque<BlockStateUpdate>,
    commit_head: u64,
    writer: &impl Writer,
    committed_head: &Arc<AtomicU64>,
) -> Result<(), StateWorkerError> {
    while let Some(front) = buffer.front() {
        if front.block_number > commit_head {
            break;
        }
        let update = buffer.pop_front()
            .ok_or(StateWorkerError::BufferUnderflow)?;
        writer.commit_block(&update)
            .map_err(|e| StateWorkerError::MdbxWrite(e.to_string()))?;
        committed_head.store(update.block_number, Ordering::Release);
    }
    gauge!("state_worker_buffer_depth").set(buffer.len() as f64);
    Ok(())
}
```

**Ordering note:** `committed_head.store(..., Ordering::Release)` is required. Phase 3 will wire `MdbxSource` to read this atomic with `Acquire` ordering. Set `Release` from the start to avoid a correctness gap.

### Pattern 5: Buffer-Full Backpressure

Before calling `process_block` in the catch-up and streaming loops:

```rust
if buffer.len() >= BUFFER_CAPACITY {
    counter!("state_worker_buffer_full_pauses_total").increment(1);
    gauge!("state_worker_buffer_depth").set(buffer.len() as f64);
    // Drain any pending CommitHead signals before retrying
    // recv_timeout allows shutdown polling
    match commit_head_rx.recv_timeout(RECV_TIMEOUT) {
        Ok(signal) => flush_ready_blocks(&mut buffer, signal.block_number, ...),
        Err(flume::RecvTimeoutError::Timeout) => { /* retry */ }
        Err(flume::RecvTimeoutError::Disconnected) => return Ok(()), // engine exited
    }
    continue; // re-check buffer length
}
```

This is synchronous — no async Notify needed. The state worker's inner tokio runtime is `new_current_thread`; polling `commit_head_rx.recv_timeout` is a blocking call that does not block the tokio executor (it is called from a sync context via `rt.block_on`).

**Important:** `flume::Receiver::recv_timeout` is synchronous — it does NOT require an async context. It can be called in a blocking fashion from within `rt.block_on(async { ... })` only if wrapped in `tokio::task::block_in_place` or called outside the async block. The recommended structure is to drive the outer CommitHead receive loop synchronously (outside `rt.block_on`) and run only the async tracing inside `rt.block_on`. See Anti-Pattern 2 below.

### Pattern 6: Shutdown Best-Effort Flush

On shutdown (shutdown flag set), after `StateWorker::run()` returns, attempt to flush all buffered blocks that are already authorized by the last known `commit_head`:

```rust
// After StateWorker::run() returns cleanly or on error:
if let Err(e) = flush_ready_blocks(&mut buffer, last_commit_head, ...) {
    warn!(error = %e, "Best-effort flush on shutdown failed; resuming from MDBX on restart");
}
```

This is not a correctness requirement (FLOW-06 restart recovery handles gaps), but reduces the catch-up window after restart.

### Anti-Patterns to Avoid

- **Bounded CommitHead channel:** Any capacity limit on the CommitHead channel creates a potential deadlock. See PITFALLS.md Pitfall 3.
- **Driving CommitHead recv inside `rt.block_on`:** `flume::Receiver::recv_timeout` inside a `current_thread` runtime will work if called from sync code, but if the state worker's async future is running `process_block` (which awaits), the recv is never polled. The CommitHead receive loop must interleave with the async tracing loop. The recommended architecture is a two-level loop: outer sync loop polls `commit_head_rx.recv_timeout`; inner async loop drives one block trace via `rt.block_on(worker.process_block(...))`. Alternatively, use `tokio::sync::mpsc` for the CommitHead channel and drive it inside the async loop with `tokio::select!`.
- **Calling `commit_block` in `process_block`:** The existing `StateWorker::process_block` calls `commit_block` directly. This MUST be refactored so the buffer accumulates `BlockStateUpdate` values gated by `commit_head`. FLOW-04 invariant depends on this separation.
- **`Relaxed` ordering on `committed_head` store:** Store with `Ordering::Release` from the first implementation. Phase 3 depends on this being correct; retrofitting after the fact creates a latent race.
- **Sending `engine::queue::CommitHead` directly over the channel:** That type has `pub(crate)` fields and carries fields the state worker doesn't need. Use the lightweight `CommitHeadSignal { block_number: u64 }` type.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Block tracing and WS subscription | Custom `debug_traceBlock` client | `StateWorker::process_block` (existing) | Already handles `prestateTracer` diffMode, system calls, genesis hydration, missing-block retries |
| Restart resume point | Custom "last written block" tracker | `Writer::latest_block_number()` (existing `mdbx` Reader trait) | Already atomic with MDBX write transactions; returns `Option<u64>` |
| Cross-thread channel | Custom ring buffer | `flume::unbounded()` | Proven, tested, already in workspace; recv_timeout built in |
| Ordered MDBX write | Manual write ordering logic | Drain `VecDeque` from front in block_number order | Buffer is pushed in order; front is always the lowest block_number; no sort needed |
| Metrics registration | Custom registry | `metrics::gauge!` / `metrics::counter!` macros | Already integrated with prometheus exporter in sidecar; zero setup needed |

---

## Common Pitfalls

### Pitfall 1: CommitHead Channel Deadlock (CRITICAL — design before implementation)

**What goes wrong:** If the CommitHead channel is bounded and the engine blocks on `send()` while the state worker's buffer is full and it is blocked waiting for a CommitHead signal, both threads deadlock.

**Why it happens:** The 128-entry VecDeque blocks tracing when full. The state worker cannot drain the buffer until it receives a CommitHead signal. The engine cannot send the CommitHead signal because the channel is full. Circular wait.

**How to avoid:** Use `flume::unbounded()`. The CommitHead channel carries only `u64` block numbers — memory pressure is not a concern. Backpressure lives entirely in the `VecDeque` buffer, not in the channel.

**Warning signs:** Both threads show no log output and zero CPU usage. `htop` shows them in D state (blocked on channel ops). Last log shows "buffer full" from state worker at the same block height as the last engine CommitHead.

### Pitfall 2: Async/Sync Interleaving in the State Worker Inner Loop

**What goes wrong:** The state worker's `StateWorker::run()` is async. The CommitHead receiver is sync (`flume::Receiver::recv_timeout`). Running both inside a single `rt.block_on(async { ... })` means the CommitHead receiver is never polled while `process_block().await` is in flight (since `current_thread` runtime has no other worker to run the receiver concurrently).

**Why it happens:** `new_current_thread` is a cooperative scheduler — only one future runs at a time. If the outer loop awaits `process_block`, the CommitHead receive branch is not polled until `process_block` returns.

**How to avoid:** Two valid approaches:
1. **Outer sync loop, inner async per-block:** `loop { if let Ok(sig) = commit_head_rx.try_recv() { flush(...) }; if buffer.len() < 128 { rt.block_on(worker.process_one_block(...)) } }`. This runs each block trace synchronously (blocking the thread) and checks CommitHead between blocks.
2. **Tokio select inside async loop:** Use `tokio::sync::mpsc::unbounded_channel` for CommitHead (so both the trace future and the recv future can be driven by `tokio::select!` inside a single `rt.block_on`). The engine's sync context uses `try_send` or `blocking_send`.

**Recommendation:** Option 1 (outer sync loop) is simpler and consistent with the existing shutdown-poll pattern (the existing `run_blocking_inner` uses a sync loop with `tokio::time::sleep` — the blocking analog is `thread::sleep`). It is also consistent with `EventSequencing` and `CoreEngine` which use `flume::recv_timeout` in a blocking OS thread loop without any tokio context.

### Pitfall 3: `process_commit_head` Returning Early Without Sending Signal

**What goes wrong:** `process_commit_head` has two early-return paths before the final `Ok(())`: the `!is_valid_state` path (line ~1640) and the `NothingToCommit` error path (line ~1646). If the CommitHead send is placed only at the bottom, these early returns will skip the signal, causing the state worker's buffer to grow indefinitely.

**Why it happens:** The early-return paths represent cache-invalidation events where the engine does not commit anything. The state worker still needs the signal to know what block number the engine is on — even if the engine cache was invalid.

**How to avoid:** Send `CommitHeadSignal` on ALL return paths from `process_commit_head`, including early returns. The signal says "the engine is now at block N" regardless of whether the engine's cache was valid. The state worker should flush its buffer up to N either way.

```rust
// Before EVERY return in process_commit_head:
self.send_commit_head_signal(commit_head.block_number);
```

Extract the send into a helper method to avoid copy-paste across all return sites.

### Pitfall 4: Block Number Type Mismatch (U256 vs u64)

**What goes wrong:** `CommitHead.block_number` is `U256` (from the engine's queue type). `BlockStateUpdate.block_number` is `u64` (from the mdbx crate). The comparison `entry.block_number <= commit_head` operates on different types.

**How to avoid:** Convert at the channel boundary: `CommitHeadSignal { block_number: commit_head.block_number.saturating_to::<u64>() }`. The signal type carries `u64`. All buffer comparisons use `u64`. `U256.saturating_to::<u64>()` is the correct conversion for block numbers (blocks will not exceed u64::MAX in practice).

### Pitfall 5: StateWorker Config — Missing Fields for Embedded Context

**What goes wrong:** `StateWorker::new` requires `provider: Arc<RootProvider>`, `trace_provider`, `writer_reader`, `genesis_state`, and `system_calls`. These are currently assembled in `state-worker/src/main.rs::run_once()` from CLI args. In the embedded context, these must come from sidecar config.

**Why it matters:** The sidecar config currently has `state.sources` / `state.legacy` paths pointing to an MDBX path for the **reader**. For the embedded writer, we need: WS URL for the execution client, MDBX write path, genesis file path, and state depth. These config fields must be added to `SidecarConfig` (or reuse existing fields if present).

**How to avoid:** Before implementing, audit `Config` in `crates/sidecar/src/args.rs` to check whether the WS URL and genesis path fields already exist or need to be added. This is a prerequisite for `StateWorkerThread::new()` accepting a config struct rather than accepting the provider directly.

### Pitfall 6: Restart Counter Metric Not Wired

**What goes wrong:** OBSV-02 requires `state_worker_restarts_total` to be incremented on each restart. The `restart_count` variable in `run_thread_loop` already tracks this but does not emit a metric.

**How to avoid:** Add `counter!("state_worker_restarts_total").increment(1)` at the same site as `restart_count = restart_count.saturating_add(1)` in `run_thread_loop`. Note: the restart_count variable is a local convenience; the canonical count is in the metrics counter.

---

## Code Examples

### CommitHeadSignal Type and Sender Field

```rust
// Source: new code in crates/sidecar/src/state_worker_thread/mod.rs

/// Signal sent by CoreEngine to StateWorkerThread after each committed block.
///
/// Carries only the committed block number — the state worker uses this to
/// flush all buffered `BlockStateUpdate` entries where `block_number <= signal.block_number`.
#[derive(Debug, Clone, Copy)]
pub struct CommitHeadSignal {
    pub block_number: u64,
}
```

```rust
// Source: addition to CoreEngine struct in crates/sidecar/src/engine/mod.rs

/// Optional sender for CommitHead signals to the state worker thread.
/// `None` when state worker is not wired (tests, legacy mode).
commit_head_tx: Option<flume::Sender<CommitHeadSignal>>,
```

### CommitHead Signal Send in process_commit_head

```rust
// Source: addition at end of process_commit_head, crates/sidecar/src/engine/mod.rs

// Helper to avoid repetition at multiple return sites
fn send_commit_head_signal(&self, block_number: U256) {
    if let Some(ref tx) = self.commit_head_tx {
        let signal = CommitHeadSignal {
            block_number: block_number.saturating_to::<u64>(),
        };
        // Infallible for unbounded channel while receiver exists.
        // If state worker exited, send error is intentionally ignored.
        let _ = tx.send(signal);
    }
}
```

### Channel Construction in run_sidecar_once

```rust
// Source: addition in crates/sidecar/src/main.rs, inside run_sidecar_once()

// CommitHead channel: CoreEngine -> StateWorkerThread
// MUST be unbounded to prevent circular deadlock with the 128-entry buffer.
// See PITFALLS.md Pitfall 3.
let (commit_head_tx, commit_head_rx) = flume::unbounded::<CommitHeadSignal>();
```

### Flush Loop (Synchronous, Buffer-Side)

```rust
// Source: new function in crates/sidecar/src/state_worker_thread/mod.rs

const BUFFER_CAPACITY: usize = 128;

fn flush_ready_blocks<W>(
    buffer: &mut VecDeque<BlockStateUpdate>,
    commit_head: u64,
    writer: &W,
    committed_head: &Arc<AtomicU64>,
) -> Result<(), StateWorkerError>
where
    W: Writer,
    W::Error: std::fmt::Display,
{
    while let Some(front) = buffer.front() {
        if front.block_number > commit_head {
            break;
        }
        // pop_front is O(1) on VecDeque; safe because we just checked front exists.
        let Some(update) = buffer.pop_front() else {
            break; // unreachable but avoids indexing_slicing lint
        };
        writer.commit_block(&update)
            .map_err(|e| StateWorkerError::MdbxWrite(e.to_string()))?;
        // Release ordering: ensures MdbxSource (Phase 3) observes durable MDBX writes
        // before seeing the updated height.
        committed_head.store(update.block_number, Ordering::Release);
        info!(
            target: "state_worker_thread",
            block_number = update.block_number,
            "Block flushed to MDBX"
        );
    }
    gauge!("state_worker_buffer_depth").set(buffer.len() as f64);
    Ok(())
}
```

### Outer Sync Loop Architecture (Recommended)

```rust
// Source: replacement for run_blocking_inner in state_worker_thread/mod.rs

fn run_blocking_inner(
    shutdown: &AtomicBool,
    commit_head_rx: &flume::Receiver<CommitHeadSignal>,
    config: &StateWorkerConfig,
) -> Result<(), StateWorkerError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| StateWorkerError::RuntimeBuild(e.to_string()))?;

    // StateWorker and writer constructed once per run_blocking_inner call.
    // On restart, a new instance is built from scratch — compute_start_block
    // reads MDBX to find the resume point.
    let (worker, writer, committed_head) = rt.block_on(build_state_worker(config))?;

    let mut buffer: VecDeque<BlockStateUpdate> = VecDeque::with_capacity(BUFFER_CAPACITY);
    let mut next_block = worker.compute_start_block(None)
        .map_err(|e| StateWorkerError::ComputeStartBlock(e.to_string()))?;

    loop {
        // Drain any pending CommitHead signals (non-blocking check first)
        while let Ok(signal) = commit_head_rx.try_recv() {
            flush_ready_blocks(&mut buffer, signal.block_number, &writer, &committed_head)?;
        }

        // Check shutdown
        if shutdown.load(Ordering::Acquire) {
            info!(target: "state_worker_thread", "Shutdown: flushing remaining ready blocks");
            // Best-effort flush on shutdown (non-blocking — flush what's ready)
            let last_signal = commit_head_rx.try_iter().last();
            if let Some(sig) = last_signal {
                let _ = flush_ready_blocks(&mut buffer, sig.block_number, &writer, &committed_head);
            }
            return Ok(());
        }

        // Backpressure: pause tracing when buffer is full
        if buffer.len() >= BUFFER_CAPACITY {
            counter!("state_worker_buffer_full_pauses_total").increment(1);
            gauge!("state_worker_buffer_depth").set(buffer.len() as f64);
            // Block waiting for CommitHead signal (with shutdown timeout)
            match commit_head_rx.recv_timeout(RECV_TIMEOUT) {
                Ok(signal) => {
                    flush_ready_blocks(&mut buffer, signal.block_number, &writer, &committed_head)?;
                }
                Err(flume::RecvTimeoutError::Timeout) => continue,
                Err(flume::RecvTimeoutError::Disconnected) => {
                    // Engine exited: drain on engine-side drop; state worker exits cleanly
                    return Ok(());
                }
            }
            continue;
        }

        // Trace next block (blocking: runs one async block trace on current thread)
        match rt.block_on(worker.process_block(next_block)) {
            Ok(update) => {
                buffer.push_back(update);
                next_block += 1;
                gauge!("state_worker_buffer_depth").set(buffer.len() as f64);
            }
            Err(e) => {
                return Err(StateWorkerError::Trace(e.to_string()));
            }
        }
    }
}
```

### StateWorkerError New Variants

```rust
// Source: crates/sidecar/src/state_worker_thread/error.rs additions

#[derive(Debug, Error, Clone)]
pub enum StateWorkerError {
    // Existing from Phase 1:
    #[error("Failed to build tokio runtime: {0}")]
    RuntimeBuild(String),
    #[error("State worker panicked: {0}")]
    Panicked(String),
    #[error("Thread spawn I/O error: {0}")]
    ThreadSpawn(String),

    // New in Phase 2:
    #[error("Failed to compute start block: {0}")]
    ComputeStartBlock(String),
    #[error("MDBX write failed: {0}")]
    MdbxWrite(String),
    #[error("Block trace failed: {0}")]
    Trace(String),
    #[error("State worker config invalid: {0}")]
    Config(String),
}

// All Phase 2 variants are Recoverable — EthRpcSource covers downtime while restarting.
impl From<&StateWorkerError> for ErrorRecoverability {
    fn from(e: &StateWorkerError) -> Self {
        match e {
            StateWorkerError::Panicked(_)
            | StateWorkerError::RuntimeBuild(_)
            | StateWorkerError::ThreadSpawn(_)
            | StateWorkerError::ComputeStartBlock(_)
            | StateWorkerError::MdbxWrite(_)
            | StateWorkerError::Trace(_)
            | StateWorkerError::Config(_) => ErrorRecoverability::Recoverable,
        }
    }
}
```

### Metrics Pattern (following state-worker/src/metrics.rs)

```rust
// Source: pattern from crates/state-worker/src/metrics.rs
// New additions in crates/sidecar/src/state_worker_thread/mod.rs or metrics submodule

use metrics::{counter, gauge};

/// Update buffer depth gauge. Call after every push and flush.
fn record_buffer_depth(depth: usize) {
    // Cast precision loss allowed for metrics (matches state-worker/src/metrics.rs pattern)
    gauge!("state_worker_buffer_depth").set(depth as f64);
}

/// Increment restart counter. Call at same site as restart_count.saturating_add(1).
fn record_restart() {
    counter!("state_worker_restarts_total").increment(1);
}

/// Increment buffer-full pause counter.
fn record_buffer_full_pause() {
    counter!("state_worker_buffer_full_pauses_total").increment(1);
}
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `StateWorker::process_block` commits to MDBX immediately | Returns `BlockStateUpdate`; caller decides when to flush | Phase 2 | Enables CommitHead gating; standalone binary adapts trivially |
| Standalone state-worker binary (two processes) | Embedded OS thread in sidecar | Phase 1 scaffold, Phase 2 functional | Eliminates 50ms polling, file-system lock contention, cross-process MDBX races |
| No flow control between engine and state worker | 128-entry buffer + CommitHead signal gates MDBX writes | Phase 2 | Core invariant: MDBX height never exceeds commit_head.block_number |

**Deprecated/outdated:**
- `run_blocking_inner`'s no-op sleep loop (Phase 1 placeholder): replaced entirely by the CommitHead receive + buffer + flush architecture in Phase 2.
- Direct `commit_block` call in `StateWorker::process_block`: replaced by `return Ok(update)` in Phase 2; standalone binary calls `commit_block` on the returned value.

---

## Open Questions

1. **StateWorker config fields in sidecar `Config`**
   - What we know: `StateWorker::new()` needs a WS URL, MDBX write path, genesis file path, and state depth. The sidecar config has `state.legacy.state_worker_mdbx_path` (read-only path) and `state.sources` entries.
   - What's unclear: Does sidecar config already have a dedicated embedded-state-worker config section, or does Phase 2 need to add new config fields? Check `crates/sidecar/src/args.rs` at the start of planning.
   - Recommendation: Planner should read `crates/sidecar/src/args.rs` and `Config` struct before specifying the `StateWorkerConfig` type. If fields are missing, add them as a Wave 0 task.

2. **`process_block` signature change — impact on integration tests**
   - What we know: `crates/state-worker/src/integration_tests/` calls `process_block` indirectly via `StateWorker::run()`. Changing `process_block` to return `BlockStateUpdate` requires the caller to commit.
   - What's unclear: Do any integration tests call `process_block` directly? If so, those tests need updating.
   - Recommendation: `grep -r process_block crates/state-worker/src/integration_tests/` at plan time to check. If tests only call `worker.run(...)`, no test changes needed.

3. **Genesis handling in embedded context**
   - What we know: `StateWorker::new` accepts `genesis_state: Option<GenesisState>`. The genesis is read from a file path in the standalone binary.
   - What's unclear: Should the embedded state worker hydrate genesis on first run? If yes, the sidecar needs a genesis file path in config. If no, MDBX must be pre-seeded externally.
   - Recommendation: Treat genesis as required for the embedded path. The sidecar already needs the genesis to exist (since it was previously written by the standalone binary). Planner confirms with the team whether genesis hydration is Phase 2 scope or pre-condition.

---

## Environment Availability

Step 2.6: SKIPPED — Phase 2 is code changes only. No new external tools, services, runtimes, or CLI utilities are required. `flume`, `tokio`, `mdbx`, and `state-worker` are all in-workspace dependencies already available.

---

## Validation Architecture

> `workflow.nyquist_validation` not explicitly set to false in `.planning/config.json` — section included.

### Test Framework

| Property | Value |
|----------|-------|
| Framework | cargo-nextest (CI) / cargo test (local) |
| Config file | None in sidecar crate; nextest uses workspace config |
| Quick run command | `cargo test -p sidecar --lib 2>&1 | tail -20` |
| Full suite command | `cargo nextest run -p sidecar -p state-worker` |

### Phase Requirements to Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| FLOW-01 | Buffer accepts BlockStateUpdate up to 128 entries, then blocks | unit | `cargo test -p sidecar state_worker_thread::tests::test_buffer_capacity` | ❌ Wave 0 |
| FLOW-02 | CoreEngine.process_commit_head sends CommitHeadSignal on ALL return paths | unit | `cargo test -p sidecar engine::tests::test_commit_head_signal_sent` | ❌ Wave 0 |
| FLOW-03 | flush_ready_blocks drains entries where block_number <= commit_head | unit | `cargo test -p sidecar state_worker_thread::tests::test_flush_ready_blocks` | ❌ Wave 0 |
| FLOW-04 | MDBX writer not called until CommitHead signal received | unit | `cargo test -p sidecar state_worker_thread::tests::test_no_commit_without_signal` | ❌ Wave 0 |
| FLOW-05 | Buffer-full causes backpressure; counter incremented | unit | `cargo test -p sidecar state_worker_thread::tests::test_buffer_full_pause` | ❌ Wave 0 |
| FLOW-06 | On restart, compute_start_block returns latest_block_number + 1 | unit | `cargo test -p state-worker worker::tests::test_compute_start_block_from_mdbx` | ✅ (existing test infrastructure) |
| OBSV-01 | state_worker_buffer_depth gauge updated after push and flush | unit | `cargo test -p sidecar state_worker_thread::tests::test_buffer_depth_metric` | ❌ Wave 0 |
| OBSV-02 | state_worker_restarts_total counter incremented on restart | unit | `cargo test -p sidecar state_worker_thread::tests::test_restart_counter` | ❌ Wave 0 |
| OBSV-03 | state_worker_buffer_full_pauses_total incremented when buffer full | unit | (covered by FLOW-05 test) | ❌ Wave 0 |

### Sampling Rate

- **Per task commit:** `cargo test -p sidecar --lib state_worker_thread`
- **Per wave merge:** `cargo nextest run -p sidecar -p state-worker`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps

- [ ] `crates/sidecar/src/state_worker_thread/tests.rs` — covers FLOW-01 through FLOW-05, OBSV-01 through OBSV-03
- [ ] Mock `Writer` trait impl for unit tests (can use existing `state-worker/src/integration_tests/setup.rs` patterns)
- [ ] Mock `CommitHeadSignal` sender for engine unit tests (FLOW-02 early-return paths)

---

## Sources

### Primary (HIGH confidence)

- `crates/sidecar/src/state_worker_thread/mod.rs` — Phase 1 scaffold: `run_blocking_inner`, restart loop, `RECV_TIMEOUT`, `STACK_SIZE` — the target for Phase 2 replacement
- `crates/sidecar/src/state_worker_thread/error.rs` — existing `StateWorkerError` variants; Phase 2 adds `MdbxWrite`, `Trace`, `Config`, `ComputeStartBlock`
- `crates/sidecar/src/engine/mod.rs` — `CoreEngine` struct (line 518), `process_commit_head` (line 1604), all early-return paths; `CommitHead.block_number: U256` (queue.rs line 143)
- `crates/sidecar/src/main.rs` — `run_sidecar_once` wiring (line 634), `ThreadHandles` (line 268), channel creation pattern (`flume::unbounded()` at line 653), state worker spawn (line 722)
- `crates/state-worker/src/worker.rs` — `StateWorker::run()`, `process_block()`, `compute_start_block()`, `catch_up()`, `stream_blocks()` — library code to embed
- `crates/state-worker/src/metrics.rs` — canonical `metrics::gauge!` / `metrics::counter!` pattern for `state_worker_*` metrics namespace
- `crates/mdbx/src/lib.rs` — `BlockStateUpdate` struct (line 199), `Writer::commit_block()` trait (line 462), `Reader::latest_block_number()` trait
- `.planning/research/PITFALLS.md` — Pitfall 3 (CommitHead deadlock), Pitfall 5 (shutdown ordering), Pitfall 2 (MDBX single-writer)
- `.planning/research/ARCHITECTURE.md` — CommitHead data flow diagram, build order, integration points table
- `.planning/research/STACK.md` — CommitHead channel type decision (`flume::unbounded()`), `VecDeque` pattern, metrics pattern

### Secondary (MEDIUM confidence)

- `.planning/research/SUMMARY.md` — Phase 2 delivers section confirming channel type, buffer bound, and metrics scope
- `crates/sidecar/src/engine/queue.rs` — `CommitHead` struct field visibility (`pub(crate)`) confirming a separate `CommitHeadSignal` type is needed

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — all primitives (`flume`, `VecDeque`, `metrics`, `mdbx::Writer`) verified in existing codebase
- Architecture: HIGH — injection points, struct fields, and data flow verified from direct source reading
- Pitfalls: HIGH — CommitHead deadlock (Pitfall 3) and async/sync interleaving are the dominant risks; both verified from codebase structure and prior research
- Open questions: MEDIUM — three items require checking `args.rs` and integration test structure at plan time; none block implementation

**Research date:** 2026-03-25
**Valid until:** 2026-04-25 (stable Rust workspace; no external API changes expected)
