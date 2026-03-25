# Feature Landscape

**Domain:** In-process worker thread with bounded buffer and commit-head flow control
**Project:** Credible SDK — State Worker Thread Integration
**Researched:** 2026-03-25
**Confidence:** HIGH (based on direct codebase analysis + established patterns in the existing sidecar)

---

## Context: What Is Being Built

The state worker currently runs as a standalone binary that writes chain state diffs to MDBX
while the sidecar polls that DB every 50ms. This milestone embeds the state worker as an OS
thread inside the sidecar process, replacing the polling relationship with a direct
CommitHead signal channel. The core invariant: MDBX writes never advance past the engine's
current commit head.

The existing sidecar already has three production OS threads (EventSequencing, CoreEngine,
TransactionObserver) that establish clear patterns for every feature below. Analysis of those
threads is the primary evidence source for all claims.

---

## Table Stakes

Features required for correctness. Without any of these, the integration is broken.

| Feature | Why Required | Complexity | Notes |
|---------|--------------|------------|-------|
| OS thread spawned via `std::thread::Builder` with named thread | Dedicated OS thread required for isolation; matches EventSequencing/CoreEngine pattern | Low | Name for debuggability in thread dumps — `sidecar-state-worker` |
| Own single-threaded tokio runtime | State worker uses `async` (WebSocket subscription, RPC calls); must not share the sidecar main runtime to prevent I/O starvation in either direction | Low | Pattern: `tokio::runtime::Builder::new_current_thread().build()` then `rt.block_on()` inside the OS thread |
| Panic isolation via `std::panic::catch_unwind` or thread join | Workspace Clippy denies `unwrap`/`expect`/`panic` but panics can still arrive from FFI or third-party code; the sidecar must survive a state worker panic | Medium | Existing `ThreadHandles::join_all()` already logs `"thread panicked"` on `Err(_)` from `handle.join()` — must not propagate that as a sidecar crash |
| Restart with exponential backoff | Transient errors (WebSocket drop, RPC timeout) should not permanently disable the state worker; continuous restart without backoff creates thundering-herd against the execution client | Low | Match existing pattern: `sidecar` outer `loop` already calls `run_sidecar_once()` and increments `sidecar_restarts_total`; state worker thread should use same structure with a dedicated backoff counter |
| EthRpcSource remains active as fallback | MdbxSource requires the state worker to be running and ahead of the engine; if the state worker is down or behind, the engine must still be able to validate transactions | Low | Already implemented; no change required — must not be removed |
| In-memory `BlockStateUpdate` buffer (bounded at 128 blocks) | State worker traces blocks faster than the engine commits them; without a buffer, the tracing loop blocks waiting for commit-head signals, coupling tracing latency directly to engine latency | Medium | Buffer is a `VecDeque<BlockStateUpdate>` inside the state worker thread; 128 is the configured bound from PROJECT.md |
| Tracing pauses when buffer is full | An unbounded buffer lets the state worker run arbitrarily far ahead of the engine, consuming unbounded memory; pausing at capacity keeps memory bounded and back-pressures naturally | Low | Implementation: worker checks `buffer.len() >= MAX_BUFFER_DEPTH` before tracing the next block; if full, waits (sleep or channel recv with timeout) for a CommitHead signal |
| CommitHead channel: engine signals `flush up to block N` | The engine already emits CommitHead events; forwarding the block number to the state worker thread via an mpsc channel is the gating mechanism that tells the worker which buffered updates are safe to write to MDBX | Low | `std::sync::mpsc::channel` or `tokio::sync::mpsc` with `try_recv` inside the worker loop; ordered delivery guaranteed |
| Flush buffered updates where `block_number <= commit_head` to MDBX on signal | Once signalled, the worker must drain all buffered updates up to (inclusive) the signalled block number from the in-memory buffer and commit them to MDBX atomically | Medium | Drain loop: `while let Some(update) = buffer.front() && update.block_number <= commit_head { writer.commit_block(&update)?; buffer.pop_front(); }` |
| MDBX height never exceeds current `commit_head.block_number` | This is the core safety invariant of the entire milestone — if MDBX advances past commit_head, MdbxSource could serve future state to the engine, causing incorrect assertion execution | Low | Enforced by the flush-on-signal pattern above; no explicit guard needed beyond correct implementation |
| Resume from last committed MDBX block on restart | After a restart, the state worker must call `writer_reader.latest_block_number()` to determine where to resume — the existing `compute_start_block` method already does this | Low | Already implemented in `StateWorker::compute_start_block()`; must be preserved in the embedded version |
| Atomic `Arc<AtomicU64>` for MDBX height visible to MdbxSource | Replaces the 50ms polling loop; after each successful `commit_block`, the state worker updates the shared atomic so MdbxSource can read the current MDBX height without polling | Low | `Arc<AtomicU64>` stored in both `StateWorker` and `MdbxSource`; update after every `commit_block`, read in `MdbxSource::is_synced()` / sync-check logic |
| Graceful shutdown via `Arc<AtomicBool>` shutdown flag | Sidecar already uses a `shutdown_flag: Arc<AtomicBool>` and signals all threads; state worker must check this flag in its loop and exit cleanly | Low | Pattern is identical to EventSequencing: `if shutdown.load(Ordering::Relaxed) { return Ok(()); }` in the blocking loop |
| `StateWorkerError` with `From<&StateWorkerError> for ErrorRecoverability` | Workspace requires typed error enums with thiserror and recoverability classification; engine thread errors already follow this; state worker thread must match | Low | Recoverable: WebSocket drop, RPC timeout, transient MDBX error. Unrecoverable: MDBX corruption, logic invariant violation |
| Workspace Clippy compliance: no `unwrap`/`expect`/`panic` | Workspace-level deny lints apply to all crates; violations will fail CI | Low | All error paths must use `?`, `map_err`, or explicit `match` |

---

## Differentiators

Features that provide operational excellence. The integration works correctly without them,
but they significantly improve debuggability and incident response.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| Buffer utilization gauge (`state_worker_buffer_depth`) | Operators can see in real-time how far ahead the state worker is tracing relative to the engine; a gauge perpetually near 128 indicates commit-head lag and potential backpressure issues | Low | `gauge!("state_worker_buffer_depth").set(buffer.len() as f64)` after each trace; free win given existing `metrics` crate usage |
| Buffer-full pause counter (`state_worker_buffer_full_pauses_total`) | Distinguishes "buffer is deep because engine is fast" from "buffer is stuck because CommitHead signals stopped flowing"; the counter reveals if the state worker is being throttled | Low | `counter!("state_worker_buffer_full_pauses_total").increment(1)` each time the worker pauses due to full buffer |
| Restart counter for the state worker thread (`state_worker_restarts_total`) | Mirrors the existing `sidecar_restarts_total` counter; reveals flapping state workers in dashboards | Low | `counter!("state_worker_restarts_total").increment(1)` in the restart loop, same pattern as `main.rs` |
| Tracing duration histogram (`state_worker_trace_duration_seconds`) | Identifies if block tracing latency is the bottleneck vs. MDBX write latency; informs future optimization work | Low | Already have `state_worker_total_duration_seconds` for commit; adding a trace histogram completes the pipeline breakdown |
| CommitHead lag gauge (`state_worker_commit_head_lag_blocks`) | How many blocks are in the buffer waiting for a CommitHead signal; equals `buffer.len()` but named for clarity in dashboards | Low | Can be the same metric as `state_worker_buffer_depth` with a better name, or a derived metric |
| Health status reporting as gauge (`state_worker_thread_healthy`) | Visible in Grafana dashboards for on-call operators; `1` when the thread is running, `0` when in backoff/restart | Low | Set to `1` at thread start, `0` when the thread is in restart backoff; stored as `Arc<AtomicBool>` readable from sidecar health endpoint |
| Structured tracing spans for thread lifecycle | `tracing::info_span!("state_worker")` wrapping the run loop gives structured context to all log lines emitted by the state worker; consistent with existing `target = "event_sequencing"` log patterns | Low | Already used throughout `StateWorker` — ensure the embedded version carries forward the structured target convention |

---

## Anti-Features

Things to explicitly NOT build. Each represents a concrete failure mode or a design violation.

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| Unbounded in-memory buffer | Memory grows without limit if the engine stalls (e.g., RPC outage); at ~1 MB per block with full state diffs, 10,000 buffered blocks = ~10 GB of heap | Bound at 128 blocks; pause tracing when full |
| Direct MDBX `commit_block` without checking CommitHead | Allows state worker to write future blocks into MDBX before the engine has committed them, re-introducing the "went too far" bug that this entire milestone exists to fix | Always gate `commit_block` on the CommitHead channel signal |
| Sharing the sidecar's main tokio runtime | State worker's WebSocket subscription and RPC calls compete with the engine and transport for runtime threads; a slow RPC call in the state worker can starve gRPC I/O on the engine | Own single-threaded tokio runtime inside the OS thread |
| Tokio task instead of OS thread | Async tasks can be preempted at yield points but cannot be truly isolated from other tasks in the same runtime; a blocked state worker blocks other tasks; this project's threading model is explicit: one OS thread per long-lived blocking pipeline | `std::thread::Builder::new().spawn()` as used by EventSequencing and CoreEngine |
| Blocking the engine thread to wait for state worker | Any synchronous wait inside the engine OS thread blocks all transaction processing; the CommitHead signal must be fire-and-forget from the engine's side | Engine sends to the mpsc channel with `try_send` or bounded send; state worker reads asynchronously |
| Feature-flagged parallel standalone mode | "Run both standalone and embedded simultaneously" during rollout doubles operational complexity, creates two sources of truth for MDBX writes, and risks concurrent write corruption | Clean cut migration: remove standalone binary entirely at the milestone boundary |
| Polling MdbxSource every 50ms | The entire point of the `Arc<AtomicU64>` shared height is to eliminate polling; re-introducing a poll loop negates the architectural improvement and re-introduces latency jitter | Update height via atomic after `commit_block`; read with `load(Ordering::Acquire)` in MdbxSource |
| Multiple MDBX circular buffer namespaces (depth > 1) | With CommitHead gating, the engine and state worker are always synchronized to within one commit head; the circular buffer was needed to handle range-overlap when the two processes could diverge; with in-process flow control, depth=1 is correct and sufficient | Reduce `CircularBufferConfig` depth to 1; remove `calculate_target_block` range-intersection logic |
| `calculate_target_block` / range-intersection logic in MdbxSource | This logic exists to handle "state worker wrote past what the engine needs"; CommitHead gating makes this case impossible | Remove the method and its call sites in R3 (after CommitHead integration is complete) |
| Registering state worker panic as an Unrecoverable error | The sidecar should survive a state worker panic and restart the thread; marking it Unrecoverable would trigger `critical!` and halt the sidecar | Classify state worker thread panics as Recoverable; alert on high `state_worker_restarts_total` instead |

---

## Feature Dependencies

```
CommitHead channel (mpsc, engine -> state worker)
  -> Bounded buffer flush on signal
      -> MDBX height never exceeds commit_head
          -> Arc<AtomicU64> shared height
              -> MdbxSource polling eliminated
                  -> Multiple circular buffer namespaces unnecessary
                      -> calculate_target_block removal

OS thread spawn (std::thread)
  -> Own tokio runtime (block_on inside thread)
      -> Panic isolation (catch_unwind or thread.join() error handling)
          -> Restart with backoff
              -> EthRpcSource fallback remains active during restart

Bounded buffer (128 blocks, VecDeque)
  -> Buffer-full pause
  -> Buffer utilization metrics (differentiator, no deps)

StateWorkerError + ErrorRecoverability
  -> Graceful shutdown (AtomicBool flag)
  -> Restart with backoff (Recoverable errors trigger restart)
```

---

## MVP Recommendation

The "R1" milestone requirement set is the MVP: OS thread, isolated runtime, panic isolation,
restart with backoff, EthRpcSource fallback. This establishes the thread lifecycle before adding
flow control.

**R1 priority (lifecycle correctness — no buffer or flow control yet):**
1. OS thread with named thread, isolated single-threaded tokio runtime
2. Panic isolation via thread join error handling
3. Restart with exponential backoff on Recoverable errors
4. `StateWorkerError` with `ErrorRecoverability`
5. Graceful shutdown via shared `AtomicBool`
6. EthRpcSource fallback confirmed working

**R2 priority (flow control correctness):**
7. Bounded `VecDeque<BlockStateUpdate>` buffer (128 blocks)
8. CommitHead mpsc channel from engine to state worker
9. Flush buffered updates on CommitHead signal
10. Buffer-full pause when capacity reached
11. Resume from last MDBX block on restart

**R3 priority (cleanup + simplification):**
12. `Arc<AtomicU64>` for MdbxSource height (eliminates polling)
13. MDBX circular buffer depth reduced to 1
14. Remove `calculate_target_block` and range-intersection logic
15. Remove standalone state-worker binary

**Defer (not this milestone):**
- Cache invalidation recovery via state worker — explicitly out of scope
- State diff cross-validation between engine and state worker — explicitly out of scope

**Differentiators to add alongside R2 (low effort, high value):**
- `state_worker_buffer_depth` gauge (adds during buffer implementation, trivial cost)
- `state_worker_buffer_full_pauses_total` counter (adds during pause implementation)
- `state_worker_restarts_total` counter (adds during restart loop, same pattern as sidecar)

---

## Sources

All findings derived directly from codebase analysis:

- `crates/sidecar/src/event_sequencing/mod.rs` — `EventSequencing::spawn()` and `run_blocking()` — OS thread + shutdown flag + `recv_timeout` pattern (HIGH confidence)
- `crates/sidecar/src/main.rs` — `ThreadHandles::join_all()`, `run_sidecar_once()`, `shutdown_flag: Arc<AtomicBool>` — thread lifecycle, restart loop, panic detection pattern (HIGH confidence)
- `crates/state-worker/src/worker.rs` — `StateWorker::run()`, `compute_start_block()`, restart and backoff via `SUBSCRIPTION_RETRY_DELAY_SECS` — what the embedded worker must preserve (HIGH confidence)
- `crates/state-worker/src/metrics.rs` — full metrics inventory for the current standalone worker (HIGH confidence)
- `crates/sidecar/src/metrics.rs` — `StateMetrics`, `SourceMetrics`, `BlockMetrics` — sidecar metrics patterns (HIGH confidence)
- `.planning/PROJECT.md` — explicit requirements R1/R2/R3, 128-block buffer bound, clean-cut migration decision (HIGH confidence)
- `.planning/codebase/ARCHITECTURE.md` — threading model, error handling strategy, cross-cutting concerns (HIGH confidence)
