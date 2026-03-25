# Project Research Summary

**Project:** Credible SDK — State Worker Thread Integration
**Domain:** Embedded in-process OS thread with MDBX commit-head flow control
**Researched:** 2026-03-25
**Confidence:** HIGH

## Executive Summary

The milestone replaces the current two-process architecture — where the state worker runs as a standalone binary and the sidecar polls MDBX every 50ms — with a single sidecar process that owns the state worker as a dedicated OS thread. The core architectural payoff is replacing file-system polling and cross-process MDBX lock contention with in-process flow control: the engine sends a `CommitHead` signal directly to the state worker thread after each commit, the state worker buffers up to 128 `BlockStateUpdate` entries and only flushes to MDBX when gated by that signal, and a shared `Arc<AtomicU64>` replaces the 50ms poll loop with a zero-latency height read. All required primitives already exist in the workspace — no new crate dependencies are needed.

The recommended implementation follows the established OS thread pattern used by three existing sidecar components (`EventSequencing`, `CoreEngine`, `TransactionObserver`): `std::thread::Builder::new()` with a named thread, an isolated `tokio::runtime::Builder::new_current_thread().enable_all()` runtime inside the thread, `flume` channels for cross-thread signaling, and a `tokio::sync::oneshot` channel for exit notification back to `run_sidecar_once()`. The entire pattern can be copied nearly verbatim from `EventSequencing`. The state worker binary is removed at the end of the milestone — no parallel standalone mode is needed or desired.

The highest risks are all at Phase 1 (thread spawn): nested tokio runtime panics if the OS thread is not truly isolated, MDBX `MDBX_BUSY` errors if the write env is opened twice from the same process, and shutdown ordering hangs if the state worker join handle is not wired into `ThreadHandles`. These are all design-time decisions that cannot be safely retrofitted. Phase 2 introduces the only other critical risk: a deadlock if the `CommitHead` channel is bounded in a way that lets the engine block while the state worker is also blocked waiting for that same signal. The mitigation is to use `flume::unbounded()` for the signal channel and rely exclusively on the 128-entry in-memory buffer for backpressure.

---

## Key Findings

### Recommended Stack

The stack is the existing workspace with no additions. Every required primitive is already a resolved dependency. The implementation applies well-understood Rust concurrency primitives already present in the codebase — there is no greenfield technology selection to make.

**Core technologies:**

- `std::thread::Builder::new()` (stdlib): OS thread spawn — three existing precedents in this codebase; the official Tokio docs confirm `spawn_blocking` is wrong for long-lived tasks
- `tokio::runtime::Builder::new_current_thread().enable_all()` (tokio 1.49.0): isolated runtime inside the thread — `TransactionObserver` uses this exact pattern
- `flume::bounded(128)` / `flume::unbounded()` (flume 0.12.0): cross-thread channels — the established primitive for all OS thread communication in this codebase; `recv_timeout` API matches existing loop patterns
- `Arc<AtomicU64>` (stdlib): shared MDBX height — established pattern in `grpc/server.rs`; lock-free for single-writer single-reader; `Release` store / `Acquire` load pairing is mandatory
- `tokio::sync::oneshot` (tokio): exit notification from thread back to `run_sidecar_once` — used verbatim by `EventSequencing` and `CoreEngine`
- `std::panic::AssertUnwindSafe + catch_unwind` (stdlib): panic isolation — copied from the existing `state-worker/src/main.rs`; MDBX transaction atomicity ensures MDBX is safe on panic

No new dependencies are required in `Cargo.toml`. The only Cargo change may be confirming `flume` and `tokio` are already listed in the `sidecar` crate's `[dependencies]`.

### Expected Features

The milestone is structured around three requirement groups (R1, R2, R3) that correspond directly to dependency order. The full feature set is documented in `.planning/research/FEATURES.md`.

**Must have (table stakes — R1, thread lifecycle):**
- Named OS thread with isolated single-threaded tokio runtime
- Panic isolation via `catch_unwind` and `JoinHandle::join()` error handling
- Restart with fixed 1-second backoff and saturating restart counter (matching existing state-worker binary)
- `StateWorkerError` with `ErrorRecoverability` classification (Recoverable / Unrecoverable)
- Graceful shutdown via shared `Arc<AtomicBool>` checked in the blocking loop
- `EthRpcSource` fallback confirmed active and undisturbed

**Must have (table stakes — R2, flow control):**
- Bounded `VecDeque<BlockStateUpdate>` buffer capped at 128 entries
- `CommitHead` mpsc channel from `CoreEngine` to `StateWorkerThread`
- Flush buffered entries where `block_number <= commit_head` to MDBX on signal
- Buffer-full pause: tracing halts when buffer is at capacity
- Resume from `latest_block_number()` in MDBX on restart

**Must have (table stakes — R3, cleanup):**
- `Arc<AtomicU64>` shared height replaces 50ms polling in `MdbxSource`
- MDBX circular buffer depth reduced to 1
- `calculate_target_block` and range-intersection logic removed
- Standalone state-worker binary removed

**Should have (differentiators — add alongside R2, low effort):**
- `state_worker_buffer_depth` gauge
- `state_worker_buffer_full_pauses_total` counter
- `state_worker_restarts_total` counter
- `state_worker_trace_duration_seconds` histogram
- `state_worker_thread_healthy` health gauge

**Defer (out of scope for this milestone):**
- Cache invalidation recovery via state worker
- State diff cross-validation between engine and state worker
- `EthRpcSource` removal

**Anti-features (explicitly forbidden):**
- Unbounded in-memory buffer
- Direct `commit_block` without CommitHead gating
- Sharing the sidecar's main tokio runtime with the state worker
- Bounded CommitHead signal channel (deadlock risk)
- Feature-flagged parallel standalone mode

### Architecture Approach

The target architecture inserts `StateWorkerThread` alongside the three existing OS threads in `run_sidecar_once()`. `CoreEngine` gains an `Option<flume::Sender<CommitHeadSignal>>` field and sends on it inside `process_commit_head()` after updating `current_head`. The state worker thread owns the `StateWriter` exclusively, buffers `BlockStateUpdate` entries, and flushes on each signal. `MdbxSource` stops polling and instead reads an `Arc<AtomicU64>` constructed in `run_sidecar_once()` and shared with the state worker at construction time. The full data flow and ASCII diagram are in `.planning/research/ARCHITECTURE.md`.

**Major components:**

1. `StateWorkerThread` (new, `crates/sidecar/src/state_worker_thread/`) — wraps `StateWorker` in an OS thread with isolated tokio runtime; owns the restart loop, panic catch, and bounded buffer; exclusive owner of `StateWriter`
2. `CommitHead` mpsc channel (new wiring in `main.rs`) — `flume::unbounded()` sender held by `CoreEngine`, receiver consumed by `StateWorkerThread`; carries `CommitHeadSignal { block_number: u64 }`
3. `Arc<AtomicU64>` committed-head pointer (new) — written by `StateWorkerThread` with `Release` ordering after each flush; read by `MdbxSource` with `Acquire` ordering; constructed in `run_sidecar_once()` before either consumer is spawned
4. `MdbxSource` (modified) — `spawn_block_range_poller` and `calculate_target_block` removed; `is_synced()` simplified to `committed_head.load(Acquire) >= required_block`
5. `StateWorkerHandle` (new) — return type of `StateWorkerThread::spawn()`; holds `JoinHandle`, oneshot exit receiver, and `Arc<AtomicU64>`

**Build order:** Thread scaffold (R1) → CommitHead channel wiring (R2a) → buffer + flush logic (R2b–d) → MdbxSource simplification (R3) → binary cleanup.

### Critical Pitfalls

Full analysis with phase mapping in `.planning/research/PITFALLS.md`. Top pitfalls by severity and implementation order:

1. **Nested tokio runtime panic** — `Runtime::new()` inside an async context panics with "Cannot start a runtime from within a runtime." Prevention: always use `std::thread::Builder::new().spawn()` (genuinely new OS thread, no inherited tokio context), never `spawn_blocking`. Address in Phase 1.

2. **MDBX duplicate RW env open** — Opening the same MDBX path twice with `DatabaseEnvKind::RW` from the same process returns `MDBX_BUSY` at startup. Prevention: `StateWriter` constructed once in the state worker thread closure; `MdbxSource` continues using `StateDb::open_read_only()` or shares a read-only view. Address in Phase 1.

3. **CommitHead channel deadlock** — If the CommitHead channel is bounded and the engine's send blocks while the state worker is also blocked waiting for that signal, a circular wait deadlocks both threads. Prevention: use `flume::unbounded()` for the signal channel; rely solely on the 128-entry buffer for backpressure. Address in Phase 2 design, before implementation.

4. **`AtomicU64` with `Relaxed` ordering** — On weakly-ordered architectures (ARM), `Relaxed` on the committed-height store allows the engine to observe an updated height before the MDBX data is durable, serving empty state for an "available" block. Prevention: `Release` on store after `commit_block`, `Acquire` on load in `MdbxSource`. Address in Phase 3 at design time.

5. **Shutdown ordering hang** — The state worker may be mid-RPC when SIGTERM arrives; `join_all()` waits indefinitely. Prevention: wire the state worker join handle into `ThreadHandles`; join engine before state worker; state worker's existing `tokio::select!` on `shutdown_rx` interrupts in-flight calls. Address in Phase 1 (join handle wiring) and Phase 2 (best-effort flush on shutdown).

---

## Implications for Roadmap

Based on the dependency chains established by research, the natural phase structure aligns with the R1/R2/R3 requirement groups from `PROJECT.md`. Each phase is a discrete, testable increment with no phase-straddling dependencies.

### Phase 1: Thread Scaffold and Lifecycle

**Rationale:** All subsequent work depends on the thread existing and the lifecycle being correct. An empty thread that participates in startup, shutdown, and panic detection de-risks every future phase. Phase 1 has zero functional change to state processing — the risk surface is small. The three critical pitfalls involving setup decisions (nested runtime, duplicate MDBX RW env, shutdown ordering, stack size) must all be resolved here — they cannot be safely deferred.

**Delivers:** `StateWorkerThread` struct with `spawn()` and `run_blocking()` returning `Ok(())` immediately; wired into `run_sidecar_once()` so its exit notification is part of the `select!`; `StateWorkerHandle` returned and joined in `ThreadHandles::join_all()`; `StateWorkerError` with `ErrorRecoverability`; graceful shutdown via `AtomicBool`; sidecar compiles, existing tests pass, `EthRpcSource` fallback confirmed working.

**Addresses (from FEATURES.md):** Named OS thread, isolated single-threaded tokio runtime, panic isolation, restart backoff, graceful shutdown, `StateWorkerError`, EthRpcSource fallback preserved.

**Avoids (from PITFALLS.md):** Nested tokio runtime panic (Pitfall 1), duplicate MDBX RW env open (Pitfall 7), stack overflow via explicit stack size at spawn (Pitfall 4), shutdown hang (Pitfall 5 — join handle wiring).

### Phase 2: CommitHead Flow Control and Bounded Buffer

**Rationale:** The CommitHead channel is the backbone of the entire milestone's correctness guarantee. Buffer and flush logic depend on the channel being operational. The deadlock pitfall must be designed out before writing a single line of channel code. This phase delivers the core invariant: MDBX height never exceeds `commit_head.block_number`. Observability metrics (buffer depth, pause counter, restart counter) should be added alongside the features they instrument — the incremental cost is trivial and the operational value is immediate.

**Delivers:** `flume::unbounded()` `CommitHead` channel from `CoreEngine::process_commit_head()` to `StateWorkerThread`; `VecDeque<BlockStateUpdate>` buffer bounded at 128; flush loop draining entries where `block_number <= commit_head`; buffer-full pause with tracing halt; resume from `latest_block_number()` on restart; `state_worker_buffer_depth`, `state_worker_buffer_full_pauses_total`, `state_worker_restarts_total` metrics.

**Uses (from STACK.md):** `flume::unbounded()` (consistent with existing cross-thread channels), `VecDeque` (stdlib).

**Implements (from ARCHITECTURE.md):** CommitHead data flow path; buffer and flush pipeline; CoreEngine integration point.

**Avoids (from PITFALLS.md):** CommitHead channel deadlock (Pitfall 3 — unbounded channel), unconditional `commit_block` anti-pattern (Anti-Pattern 5 in ARCHITECTURE.md), shutdown best-effort flush (Pitfall 5 — flush logic).

### Phase 3: MdbxSource Simplification and Cleanup

**Rationale:** The `Arc<AtomicU64>` that Phase 2 populates is the precondition for this phase. Simplifying `MdbxSource` before the atomic has valid data would break the fallback path. This phase removes the 50ms polling overhead, the "went too far" guard, and the range-intersection logic that the CommitHead gating makes impossible. Removing the standalone binary is the final act — safe only after end-to-end verification in production.

**Delivers:** `MdbxSource::spawn_block_range_poller` removed; `is_synced()` simplified to `committed_head.load(Acquire) >= required_block`; `calculate_target_block` and call sites removed; circular buffer depth reduced to 1; `crates/state-worker/src/main.rs` deleted; Helm chart and Dockerfile for standalone binary removed.

**Uses (from STACK.md):** `Arc<AtomicU64>` with `Release` store (state worker) / `Acquire` load (`MdbxSource`).

**Avoids (from PITFALLS.md):** `Relaxed` ordering bug (Pitfall 6 — explicit `Release/Acquire` pair from the start), "Looks Done But Isn't" checklist item for AtomicU64 ordering.

### Phase Ordering Rationale

- Phase 1 must come before Phase 2 because the thread must exist before the CommitHead channel can be wired into it; the panic-isolation and shutdown-ordering decisions in Phase 1 are also prerequisites for Phase 2's best-effort flush logic.
- Phase 2 must come before Phase 3 because the `Arc<AtomicU64>` only carries valid data after the buffer-and-flush path is operational end-to-end; simplifying `MdbxSource` before that data is available would break the sync check.
- Phase 3 cleanup (binary removal) must be last because the standalone binary remains the operational escape hatch until the embedded path is verified in production.
- This ordering matches the R1 → R2 → R3 dependency chain documented in `PROJECT.md` and the build order in `ARCHITECTURE.md`.

### Research Flags

Phases with standard, well-documented patterns (skip `research-phase`):

- **Phase 1:** Direct copy of `EventSequencing` and `CoreEngine` spawn pattern. Pattern verified at three call sites in the codebase. No external API research needed.
- **Phase 2:** Buffer and flush logic is straightforward `VecDeque` manipulation. Channel selection (`flume::unbounded()`) is settled. Metrics follow existing `state-worker/src/metrics.rs` patterns.
- **Phase 3:** `Arc<AtomicU64>` pattern verified in `grpc/server.rs`. MDBX env opening modes documented in `crates/mdbx/src/db.rs`. `MdbxSource` changes are deletions, not new code.

No phase requires a `research-phase` pass during planning. All unknowns are resolved by codebase evidence with HIGH confidence.

---

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | Every choice verified against existing codebase precedents at named file/line locations; official Tokio docs corroborate `spawn_blocking` vs `std::thread` distinction |
| Features | HIGH | Derived directly from `PROJECT.md` R1/R2/R3 requirements and codebase analysis; no external domain research required |
| Architecture | HIGH | Patterns copied verbatim from three existing OS threads; all component boundaries verified in source; data flow matches stated `PROJECT.md` invariants |
| Pitfalls | HIGH | Seven critical pitfalls identified with codebase file/line citations; MDBX single-writer constraint verified in `db.rs`; atomic ordering verified against Rust docs |

**Overall confidence:** HIGH

### Gaps to Address

- **`catch_unwind` soundness for MDBX mid-write panics:** Research notes MEDIUM confidence on whether `AssertUnwindSafe` around an MDBX write is fully safe. MDBX's own transaction rollback-on-drop handles the data integrity case, but the soundness contract of `AssertUnwindSafe` is compiler-silenced, not compiler-verified. Add a specific integration test that panics mid-`commit_block` and confirms MDBX height is consistent after restart.

- **Circular buffer depth reduction to 1:** Reducing `CircularBufferConfig` depth from its current value to 1 is flagged as safe by the architecture research (CommitHead gating makes "went too far" impossible), but the specific config location and any downstream effects on MDBX namespace creation are not verified. Confirm the config change and test at the start of Phase 3.

- **Tokio `select!` shutdown interruption of in-flight RPC:** The state worker's existing `tokio::select!` on `shutdown_rx` is documented to interrupt WebSocket subscriptions and RPC calls. Verify this behavior holds for `prestateTracer` calls specifically, which may use a separate timeout path. Address with an integration test in Phase 1 or Phase 2.

---

## Sources

### Primary (HIGH confidence)

- `crates/sidecar/src/event_sequencing/mod.rs` — canonical OS thread spawn pattern, `recv_timeout` shutdown polling
- `crates/sidecar/src/engine/mod.rs` — `CoreEngine::spawn()`, `process_commit_head()`, oneshot exit notification
- `crates/sidecar/src/transaction_observer/mod.rs` — `new_current_thread` runtime inside OS thread (line 215)
- `crates/sidecar/src/main.rs` — `ThreadHandles`, `run_sidecar_once()`, `shutdown_flag: Arc<AtomicBool>`, restart loop
- `crates/sidecar/src/cache/sources/mdbx/mod.rs` — `spawn_block_range_poller()`, `calculate_target_block()`, 50ms polling (to be removed)
- `crates/sidecar/src/transport/grpc/server.rs` — `Arc<AtomicU64>` pattern (line 880)
- `crates/state-worker/src/worker.rs` — `StateWorker::run()`, `compute_start_block()`, `process_block()`
- `crates/state-worker/src/main.rs` — `AssertUnwindSafe + catch_unwind`, fixed 1s restart backoff (lines 82, 102-109)
- `crates/state-worker/src/metrics.rs` — metrics inventory
- `crates/mdbx/src/db.rs` — `StateDb`, single-writer `tx_mut()`, `MDBX_EXCLUSIVE` flag (line 137)
- `crates/mdbx/src/writer.rs` — `commit_block()` atomicity, preprocessing order
- `.planning/PROJECT.md` — R1/R2/R3 requirements, 128-block buffer bound, clean-cut migration decision
- [tokio::task::fn.spawn_blocking](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html) — long-lived task warning
- [tokio::runtime::Builder](https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html) — `new_current_thread` semantics
- [libmdbx: single writer at a time](https://github.com/erthink/libmdbx) — write serialization
- [Rust Atomics — Ordering](https://doc.rust-lang.org/std/sync/atomic/enum.Ordering.html) — Release/Acquire semantics
- [std::panic::catch_unwind — limitations](https://doc.rust-lang.org/std/panic/fn.catch_unwind.html) — stack overflow / `panic=abort` gaps

### Secondary (MEDIUM confidence)

- [Tokio: "Cannot start a runtime from within a runtime"](https://github.com/tokio-rs/tokio/discussions/3857) — nested runtime behavior
- [Tokio: Bridging sync and async code](https://tokio.rs/tokio/topics/bridging) — `block_on` from OS thread

---
*Research completed: 2026-03-25*
*Ready for roadmap: yes*
