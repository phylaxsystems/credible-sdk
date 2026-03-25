# Pitfalls Research

**Domain:** Rust sidecar state worker thread integration — in-process OS thread with MDBX writes and commit-head flow control
**Researched:** 2026-03-25
**Confidence:** HIGH (codebase verified + official documentation corroborated)

---

## Critical Pitfalls

### Pitfall 1: Nested Tokio Runtime — `Runtime::new()` called from inside the main tokio runtime

**What goes wrong:**
The state worker thread is spawned from `run_sidecar_once()`, which runs inside `#[tokio::main]`. If `Runtime::new().block_on(...)` is called inside the OS thread without first checking whether a tokio context is already active, tokio panics with `"Cannot start a runtime from within a runtime"`. Even `Handle::block_on` panics in single-threaded (`current_thread`) runtimes because there are no other worker threads to yield to.

**Why it happens:**
The existing `EventSequencing` and `CoreEngine` threads are purely synchronous loops — they use `flume::recv_timeout` and never call into async code. The state worker is fundamentally different: its entire `StateWorker::run()` is `async` (WebSocket subscription, RPC calls). Developers reach for `Runtime::new()` as the obvious way to drive async code from an OS thread, without realizing the main runtime's context propagates into the spawned thread via thread-local storage.

The correct pattern is: call `Runtime::new()` inside `std::thread::spawn` only when the spawned thread has no inherited tokio context. Because `std::thread::spawn` creates a genuinely new OS thread (no tokio executor), there is no inherited handle — `Runtime::new().block_on(...)` works correctly inside that thread. The mistake is calling `tokio::task::spawn_blocking` (which runs inside the existing runtime) or accidentally calling `block_on` on the main runtime's `Handle` from within the thread.

**How to avoid:**
Use `std::thread::Builder::new().spawn(|| { let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()...; rt.block_on(worker.run(...)) })`. This follows the pattern already established for `EventSequencing` and `CoreEngine`: OS thread with its own isolated runtime. Do NOT use `tokio::task::spawn_blocking` for the state worker, as that runs within the main runtime's thread pool.

**Warning signs:**
- Runtime panic at startup: `"Cannot start a runtime from within a runtime"`
- Segfault or silent freeze when `Handle::block_on` is called inside a `current_thread` runtime (no worker threads to yield to)
- Test passes in isolation but panics when run inside the full sidecar harness

**Phase to address:**
Phase 1 (OS thread spawn + isolated runtime). The isolation must be correct from the very first spawn; it cannot be retrofitted after the fact.

---

### Pitfall 2: MDBX Single-Writer Constraint — Two concurrent write transactions block indefinitely

**What goes wrong:**
MDBX enforces that only one write transaction is active at any moment. A second caller of `tx_mut()` blocks until the first commits or rolls back. If the state worker (holding a write transaction during `commit_block`) and any other component both attempt write transactions against the same `DatabaseEnv` simultaneously, the second caller hangs. In the post-integration world, the sidecar and state worker both share the same MDBX environment via `Arc<DatabaseEnv>` (per `StateDb`'s `Clone` implementation). Any code path that opens a second write transaction creates a latent hang.

Currently the sidecar opens the MDBX env in read-only mode (`StateReader`) and state-worker opens it read-write (`StateWriter`), so they use separate `DatabaseEnv` instances and there is no conflict. After integration into a single process, if this separation is not maintained — e.g., if a future refactor accidentally shares the same `DatabaseEnv` between the reader and writer paths — the write-lock hang will appear only under load and will look like a frozen process rather than an error.

**Why it happens:**
`StateDb` derives `Clone` (line 82, `crates/mdbx/src/db.rs`) and `inner` is `Arc<DatabaseEnv>`. Cloning a `StateDb` shares the underlying `DatabaseEnv`. Teams doing "consolidate all MDBX access" refactors (e.g., the TODO in `crates/sidecar/src/transaction_observer/db.rs:30` about a global sidecar MDBX) may inadvertently route a second write path through the same env. MDBX won't error; it will silently block.

**How to avoid:**
Keep the `StateWriter` (held exclusively by the state worker thread) and the `StateReader` (held by `MdbxSource`) as separate `DatabaseEnv` instances opened from the same on-disk path. MDBX supports multiple environments opened against the same file simultaneously; readers do not block writers. Never expose the writer's `StateDb` to sidecar code. If a "global sidecar MDBX" consolidation happens, the writer must remain in the state worker's exclusive ownership.

**Warning signs:**
- `tx_mut()` call blocks for more than a few hundred milliseconds with no log output
- Block processing stalls at a point where `commit_block` was called
- Adding a second `StateWriter` anywhere in the codebase is always wrong

**Phase to address:**
Phase 1 (thread spawn and MDBX env construction). The env construction strategy must be decided up front and enforced by type visibility — do not expose `StateWriter` outside the state worker module.

---

### Pitfall 3: CommitHead Channel Deadlock — Engine blocked on full buffer while state worker blocked on empty channel

**What goes wrong:**
The design introduces a bounded 128-block in-memory buffer in the state worker. The state worker pauses tracing when the buffer is full (waiting for the engine to send a `CommitHead` signal). The engine sends `CommitHead` after processing each block. If the channel carrying the `CommitHead` signal is itself bounded AND both the engine's send and the state worker's recv are on the same synchronous thread (or the send blocks), a circular wait forms:

- State worker: buffer is full, blocked waiting for `CommitHead` to flush
- Engine: blocked trying to send `CommitHead` into a full channel, never gets to process the next block

The standard pattern in the existing codebase uses unbounded `flume::unbounded()` channels for intra-component communication (as seen in `run_sidecar_once` for transport→sequencing and sequencing→engine). Using a bounded channel for the `CommitHead` signal is unnecessary and dangerous.

**Why it happens:**
`tokio::sync::mpsc::channel(N)` requires the sender to `await` when the channel is full. If the engine's `CommitHead` sender is called synchronously on the engine OS thread (which it should be — the engine is a blocking loop), the `try_send` may silently drop signals or the `send().await` will require the engine to be inside a tokio future, which it is not. Teams sometimes reach for bounded channels thinking "bounded is safer," but for flow signals between two components that are never supposed to back up, bounded creates more failure modes than it solves.

**How to avoid:**
Use `std::sync::mpsc::sync_channel` or `flume::unbounded()` for the `CommitHead` signal channel — consistent with the rest of the sidecar. The CommitHead channel carries lightweight `u64` block numbers; there is no memory pressure justification for bounding it. The backpressure mechanism is the 128-block in-memory buffer in the state worker itself, not the channel.

If a `tokio::sync::mpsc` channel is used (to allow the engine, which runs inside a tokio context, to send without blocking), use `try_send` in a non-blocking path and document the semantics. Never block the engine thread waiting for the CommitHead channel to drain.

**Warning signs:**
- Both state worker and engine thread show no log output but are not exiting
- `htop`/`top` shows two threads stuck with no CPU usage (blocked on channel ops)
- Log shows "buffer full" from state worker and nothing from engine at the same block height

**Phase to address:**
Phase 2 (CommitHead flow control). This pitfall must be designed out before implementation begins, not debugged after.

---

### Pitfall 4: `catch_unwind` Gaps — Stack overflow and `panic=abort` silently terminate the sidecar

**What goes wrong:**
The existing `EventSequencing` and `CoreEngine` threads detect panics by observing that the `JoinHandle::join()` result is `Err(_)` (line 289, `main.rs`: `Err(_) => tracing::error!("Engine thread panicked")`). This works for panics that unwind. It does not work for:

1. **Stack overflow** — the OS sends SIGSEGV/SIGABRT; the process aborts immediately, no unwinding, no recovery
2. **`panic = "abort"`** in any dependency — aborts the process immediately
3. **`abort_on_drop` / double-panic** — if a type in the state worker panics on drop while already panicking, Rust aborts
4. **FFI panics** — MDBX is a C library; a C-level assertion failure or signal from inside MDBX cannot be caught by Rust's unwind machinery

The state worker's async runtime (WebSocket connections, RPC retries) uses more stack depth than the existing synchronous engine threads. A deeply recursive RPC response deserialization or a WebSocket handler could stack-overflow.

**Why it happens:**
Teams that implement "panic recovery" via `catch_unwind` or `JoinHandle::join()` believe they have comprehensive crash isolation. The official Rust documentation notes: "This function might not catch all panics; a panic is not always implemented via unwinding, but can be implemented by aborting the process."

The codebase's workspace-level `deny(panic)` lint prevents explicit `panic!()` calls in library code, but does not protect against OS-level stack overflows or panics from transitive C dependencies.

**How to avoid:**
- Set an explicit stack size when spawning the state worker thread: `std::thread::Builder::new().stack_size(8 * 1024 * 1024).spawn(...)`. The default 2MB stack is often insufficient for deeply nested async futures on a `current_thread` runtime.
- Do not rely solely on `JoinHandle::join()` for panic detection; also monitor the `oneshot::Receiver` exit channel (already in use for engine/sequencing) so abnormal exit is detected even if the thread was aborted.
- For MDBX writes: MDBX transactions roll back atomically on drop, so a state worker panic mid-write leaves MDBX in a consistent state. Confirm this property holds with a test.
- Accept that a SIGSEGV/SIGABRT from stack overflow will abort the whole process. The only mitigation is a process supervisor (already present in the deployment environment).

**Warning signs:**
- Sidecar process exits with signal 11 (SIGSEGV) or signal 6 (SIGABRT) with no Rust panic message
- State worker exit notification never arrives on the oneshot channel (process was killed before the channel send)
- `RUST_MIN_STACK` environment variable tests show different behavior than production (different stack depths)

**Phase to address:**
Phase 1 (thread spawn). Set stack size at spawn time. Do not defer to "we'll see if it stack-overflows."

---

### Pitfall 5: Shutdown Ordering — State worker not flushed before sidecar exits

**What goes wrong:**
When the sidecar receives SIGTERM or CTRL+C, `shutdown_flag.store(true)` is broadcast to all threads. The existing threads (`EventSequencing`, `CoreEngine`, `TransactionObserver`) respond to this flag on their next `recv_timeout` poll cycle (100ms max latency, per `RECV_TIMEOUT`). The state worker's shutdown path is more complex:

1. The state worker may be mid-trace of a block (holding an in-flight RPC call)
2. The state worker may have buffered blocks not yet written to MDBX (buffer partially flushed)
3. The engine may have sent a `CommitHead` signal that the state worker has not yet processed

If `thread_handles.join_all()` times out (no timeout is currently set — it blocks forever), the process hangs. If it does NOT wait for the state worker (handles dropped), the state worker's in-flight MDBX write is interrupted mid-transaction. MDBX rolls back the transaction on drop, so MDBX itself is safe, but any buffered blocks that were never flushed are silently discarded, causing a gap in the MDBX height on next start.

**Why it happens:**
The current `ThreadHandles::join_all()` (lines 284-310, `main.rs`) joins all threads sequentially with no timeout. This pattern works for the existing synchronous threads because they respond to the shutdown flag quickly. The state worker, if stuck in a long RPC call (e.g., waiting for a `prestateTracer` response from a slow execution client), will not respond to the flag until the RPC completes. Joining a hung state worker will hang `join_all()`, which hangs the sidecar shutdown, which may trigger the process supervisor to SIGKILL the process — leaving MDBX in whatever state the write transaction was in when SIGKILL arrived.

**How to avoid:**
- The state worker's `run()` loop already uses `tokio::select!` with `shutdown_rx.recv()` (per `worker.rs` lines 110-155). This correctly interrupts subscriptions and RPC calls mid-flight.
- After the shutdown signal, the state worker should attempt a "best-effort flush": write any fully-traced blocks from its buffer to MDBX, then exit. This is not the same as blocking until the buffer is empty; it means writing what is already ready.
- Add the state worker's join handle to `ThreadHandles` and ensure `join_all()` joins it after the engine and sequencing threads (since the engine must exit before the state worker can safely stop receiving CommitHead signals).
- On restart, the state worker already resumes from `latest_block_number()` in MDBX, so a gap caused by an abrupt shutdown is recovered automatically. The flush is a nicety, not a correctness requirement.

**Warning signs:**
- `join_all()` hangs for more than 5s after shutdown signal
- MDBX height after restart is behind the pre-shutdown commit head (indicates partial flush was not completed)
- Log shows shutdown signal but no "state worker exiting" message

**Phase to address:**
Phase 1 (thread spawn) for the join handle wiring; Phase 2 (CommitHead flow control) for the best-effort flush logic.

---

### Pitfall 6: `AtomicU64` MDBX Height — Wrong memory ordering makes engine read stale height

**What goes wrong:**
Requirement R3 replaces the 50ms poll loop with `Arc<AtomicU64>` shared between the state worker (writer) and `MdbxSource` (reader). If the state worker uses `Relaxed` ordering to store the new MDBX height, and `MdbxSource` uses `Relaxed` ordering to load it, the compiler and CPU are free to reorder operations. On weakly-ordered architectures (ARM), the engine may observe an updated height before the actual MDBX data is durably written, and read stale/empty data for a block that appears "available" according to the atomic.

**Why it happens:**
`Relaxed` is attractive because it's the cheapest atomic ordering. The existing codebase uses `Ordering::Acquire` for the shutdown flag load (line 1310, `engine/mod.rs`: `shutdown.load(Ordering::Acquire)`) and `Ordering::Relaxed` for the store (line 710, `main.rs`: `shutdown_flag.store(true, Ordering::Relaxed)`) — a deliberate choice for a flag where stale-read just adds one poll cycle of latency. The MDBX height is different: the height value is used to make correctness decisions (is block N readable?), not merely timing decisions.

**How to avoid:**
The state worker must store the new MDBX height with `Release` ordering after `commit_block()` completes. `MdbxSource` must load with `Acquire` ordering. This establishes a happens-before relationship: the MDBX write is visible to any thread that observes the updated height. A single `fetch_max` with `Release` (to ensure monotonic, non-decreasing values) on the writer side is the correct primitive. On x86, `Release` and `Acquire` compile to plain loads/stores with no extra barrier, so there is no performance cost.

**Warning signs:**
- `MdbxSource` returns `None` or stale data for block N despite the height atomic showing N is available
- Intermittent assertion failures in `MdbxSource` reads that only reproduce on ARM or under heavy load
- MSAN/TSAN reports data race on the height atomic

**Phase to address:**
Phase 3 (MdbxSource in-process height). The ordering must be chosen at design time; retrofitting after seeing data races is expensive.

---

### Pitfall 7: MDBX Env Opened as RW by Both Sidecar and State Worker (duplicate open)

**What goes wrong:**
Currently, the sidecar opens MDBX via `StateReader` (`DatabaseEnvKind::RO`) and the state-worker opens via `StateWriter` (`DatabaseEnvKind::RW`). Both are separate OS processes, sharing the file on disk. After integration, if the code naively opens the same path twice with `RW` mode (e.g., because the `StateDb::open()` is called in two places in the same process without sharing the `Arc`), MDBX will return `MDBX_BUSY` and refuse to open.

This is distinct from Pitfall 2 (write concurrency): this is the environment open itself failing. It's most likely to appear during testing when a test creates both a `StateWriter` and a `StateReader` against the same path using separate `StateDb::open()` calls instead of cloning the `StateDb`.

**Why it happens:**
`StateDb::open()` always opens `DatabaseEnvKind::RW`. `StateDb::open_read_only()` opens `DatabaseEnvKind::RO`. The sidecar currently calls `StateReader::new()`, which internally calls `StateDb::open_read_only()`. If integration moves to a shared in-process `StateDb`, any code path that calls `StateDb::open()` a second time for the same path creates a second `DatabaseEnv` — and MDBX's `MDBX_EXCLUSIVE: false` setting (line 137, `db.rs`) allows multiple environments against the same path, but only one can have write access.

**How to avoid:**
In-process, the state worker's `StateDb` (RW) is the single source of truth. `MdbxSource` should continue using `StateReader` via `StateDb::open_read_only()` or share the same `Arc<StateDb>` with the write env exposed through a read-only interface. Do not open the same path twice with `DatabaseEnvKind::RW` from the same process.

**Warning signs:**
- `StateDb::open()` returns `StateError::DatabaseOpen` with message containing "MDBX_BUSY"
- Error appears at startup, not during operation
- Disappears when the two open calls are replaced with a single `Arc` clone

**Phase to address:**
Phase 1 (environment construction). Must be resolved before any other work can proceed.

---

## Technical Debt Patterns

Shortcuts that seem reasonable but create long-term problems.

| Shortcut | Immediate Benefit | Long-term Cost | When Acceptable |
|----------|-------------------|----------------|-----------------|
| Using `tokio::sync::mpsc` for CommitHead signal instead of `std::sync::mpsc` | Familiar async API, integrates with engine's async context | Engine is a blocking thread; `.send().await` requires spawning a task or using `try_send`, creating subtle drop semantics | Never — use `flume::unbounded()` or `std::sync::mpsc::unbounded_channel()` to match existing patterns |
| Skipping the stack size override on thread spawn | Simpler code, uses OS default | State worker's async runtime + WebSocket + RPC deserialization can overflow 2MB default; silent process abort | Never — set `8 * 1024 * 1024` at spawn time |
| Sharing a single `StateDb` (RW) between writer and reader paths | Eliminates the need for two MDBX env opens | Any code path that gets the `StateDb` handle can call `tx_mut()`, violating the single-writer ownership model | Never — keep `StateWriter` in the state worker module only, expose a `StateReader` view to consumers |
| Exposing `commit_head` as `AtomicU64` with `Relaxed` ordering | Zero-cost on x86 | Silent correctness bugs on ARM; stale height reads cause `MdbxSource` to serve empty state for "available" blocks | Never — use `Release/Acquire` pair; cost is identical on x86 |
| Dropping the state worker join handle without joining | Avoids blocking shutdown | MDBX write transaction is abandoned mid-flight on process exit; although MDBX rolls back, the state worker logs nothing and the gap is invisible until the next start | Acceptable only if the deployment supervisor restarts the sidecar within seconds |

---

## Integration Gotchas

Common mistakes when connecting the state worker thread to the rest of the sidecar.

| Integration | Common Mistake | Correct Approach |
|-------------|----------------|------------------|
| CommitHead channel direction | Engine sends to state worker on a channel the state worker owns; state worker `recv` blocks on a `tokio::sync::mpsc` receiver inside a `block_on` — this requires the state worker's runtime to be running, but the state worker may not have started its runtime yet | Wire the channel before spawning the thread; pass the `Receiver` into the thread closure; ensure the `Receiver` is polled inside the state worker's `rt.block_on(run())` call |
| MDBX height AtomicU64 ownership | `Arc<AtomicU64>` is created inside the state worker and cloned into `MdbxSource` — but `MdbxSource` is constructed before the state worker thread starts, so the `Arc` must be constructed before the thread, not inside it | Construct `Arc<AtomicU64>` in `run_sidecar_once()`, pass a clone to `MdbxSource`, pass the original into the state worker thread closure |
| Restart on state worker failure | State worker restarts but the `Arc<AtomicU64>` height is not reset — `MdbxSource` continues to read from the same atomic, which may still show the last height before the crash | Height reset is not needed: on restart, the state worker writes `latest_block_number()` from MDBX and then increments normally; `MdbxSource` reading a briefly stale height just causes it to fall back to `EthRpcSource`, which is correct |
| CommitHead signal on sidecar restart | `run_sidecar_once()` creates a new CommitHead channel on every restart; the old state worker thread (from before the restart) still holds the old receiver | Shutdown the state worker thread (via shutdown flag) before `run_sidecar_once()` exits, then re-create the channel and re-spawn the state worker on the next `run_sidecar_once()` call |

---

## Performance Traps

Patterns that work at small scale but fail as load grows.

| Trap | Symptoms | Prevention | When It Breaks |
|------|----------|------------|----------------|
| State worker buffer flush on every CommitHead | Flushing all buffered blocks synchronously inside the state worker's main loop blocks tracing new blocks while writing | Flush is already async-sequential inside the worker's tokio runtime; ensure `commit_block` (synchronous, CPU-bound) does not block the WebSocket subscription — use a nested `spawn_blocking` or ensure the runtime is multi-threaded if tracing overlap is needed | At high block rates (>4 blocks/sec) if `commit_block` takes >250ms per block |
| Sending CommitHead for every block, not batched | 12 blocks/second = 12 channel sends/second; negligible now but wasted syscalls if block rate increases | Use `fetch_max` on the `AtomicU64` as the signal mechanism (already planned for R3); state worker polls this rather than receiving on a channel — eliminates the channel entirely for the common case | Not a real concern at Ethereum mainnet rates; relevant for high-frequency chains |
| MDBX write holding a long transaction open | `commit_block` calls `rayon` parallel preprocessing before opening the write transaction (per `writer.rs` doc); if this order is inverted, the write transaction holds the write lock during CPU work, blocking readers who need the namespace | Verify that the preprocessing (sort, diff) completes before `tx_mut()` is called; this is already the design in `writer.rs` but must not regress during refactoring | On every write if the order is accidentally inverted |

---

## "Looks Done But Isn't" Checklist

Things that appear complete but are missing critical pieces.

- [ ] **State worker thread spawned:** Verify the runtime isolation — add a test that spawns the state worker from within a `#[tokio::main]` test harness and confirms no "Cannot start a runtime from within a runtime" panic. The existing integration test in `crates/state-worker/src/integration_tests/` runs the worker standalone, not embedded.
- [ ] **Panic recovery wired:** The `JoinHandle` must be added to `ThreadHandles` in `main.rs` and joined in `join_all()`. If the state worker exits, the sidecar must detect it via the oneshot channel and restart, not silently continue with `EthRpcSource` indefinitely.
- [ ] **CommitHead channel type decided:** Confirm `std::sync::mpsc` vs `flume` vs `tokio::sync::mpsc` before Phase 2 implementation. An incorrect choice silently drops signals under backpressure without an error.
- [ ] **AtomicU64 ordering verified:** The `Release/Acquire` pair must be present before Phase 3 ships. A code review that only checks "it uses an AtomicU64" and not the ordering variant will miss this.
- [ ] **MDBX env open mode verified:** After integration, confirm via a test that no code path opens the same MDBX path with `DatabaseEnvKind::RW` twice. A startup smoke test that checks `StateError::DatabaseOpen` is not returned is sufficient.
- [ ] **Stack size set on spawn:** Check `thread::Builder::new().stack_size(...)` is present in the state worker spawn site. This is invisible at review time if the spawner uses `std::thread::spawn()` shorthand.
- [ ] **Graceful shutdown ordering:** Confirm that the engine thread is joined before the state worker thread in `join_all()`. The state worker must not exit while the engine is still running (it might send a final CommitHead after the state worker exits, causing a send-on-closed error).

---

## Recovery Strategies

When pitfalls occur despite prevention, how to recover.

| Pitfall | Recovery Cost | Recovery Steps |
|---------|---------------|----------------|
| Nested runtime panic at startup | LOW | Identify the code path calling `block_on` inside an async context; move `Runtime::new()` into a fresh `std::thread::spawn` closure |
| MDBX write transaction hang | MEDIUM | Kill the sidecar process; identify which two code paths both called `tx_mut()`; make one of them use `StateReader` (RO) instead |
| CommitHead channel deadlock | MEDIUM | Restart the sidecar (channel is re-created on restart); change the channel to unbounded before next deploy |
| Stack overflow crash | LOW | Sidecar process supervisor restarts within seconds; on next startup the state worker resumes from MDBX height; root-fix by setting explicit stack size |
| Stale AtomicU64 height causing empty reads | MEDIUM | Change `Relaxed` to `Release/Acquire` pair; no data migration needed; stale reads fall back to `EthRpcSource` automatically during the window |
| Shutdown hang (join_all blocked) | LOW | Process supervisor SIGKILLs after timeout; MDBX rolls back in-flight write on process death; state worker resumes from last committed block on restart |

---

## Pitfall-to-Phase Mapping

| Pitfall | Prevention Phase | Verification |
|---------|------------------|--------------|
| Nested tokio runtime (`Runtime::new` inside main runtime) | Phase 1 — OS thread spawn | Test that spawns state worker from inside `#[tokio::main]` without panic |
| MDBX single-writer constraint (second `tx_mut` blocks) | Phase 1 — env construction | Confirm `StateWriter` is not cloned outside the state worker module; no second RW open on the same path |
| CommitHead channel deadlock (bounded channel + full buffer) | Phase 2 — CommitHead flow control | Load test: fill buffer to 128, confirm engine CommitHead send does not block |
| `catch_unwind` gaps (stack overflow aborts process) | Phase 1 — OS thread spawn | Set explicit stack size; test with deep recursion in state worker to confirm no SIGSEGV |
| Shutdown ordering (state worker not flushed) | Phase 1 (join handle wiring) + Phase 2 (flush logic) | Integration test: send shutdown signal mid-trace; verify MDBX height after restart matches pre-shutdown height |
| AtomicU64 `Relaxed` ordering (stale height reads) | Phase 3 — in-process height | TSAN run on ARM or QEMU; code review to verify `Release/Acquire` pair |
| Duplicate RW env open (`MDBX_BUSY`) | Phase 1 — env construction | Smoke test: sidecar starts without `StateError::DatabaseOpen`; verify in CI |

---

## Sources

- Codebase: `crates/sidecar/src/main.rs` (ThreadHandles, run_sidecar_once, shutdown pattern)
- Codebase: `crates/sidecar/src/engine/mod.rs` (CoreEngine::spawn, OS thread pattern)
- Codebase: `crates/mdbx/src/db.rs` (StateDb, single-writer tx_mut documentation)
- Codebase: `crates/mdbx/src/writer.rs` (commit_block atomicity documentation)
- Codebase: `crates/state-worker/src/worker.rs` (StateWorker::run, shutdown_rx handling)
- Codebase: `crates/sidecar/src/cache/sources/mdbx/mod.rs` (MdbxSource, polling pattern being replaced)
- [Tokio: Bridging sync and async code](https://tokio.rs/tokio/topics/bridging)
- [Tokio Runtime docs — `Cannot start a runtime from within a runtime`](https://github.com/tokio-rs/tokio/discussions/3857)
- [Rust Nomicon: Unwinding](https://doc.rust-lang.org/nomicon/unwinding.html)
- [std::panic::catch_unwind — limitations](https://doc.rust-lang.org/std/panic/fn.catch_unwind.html)
- [libmdbx: writes serialized, single writer at a time](https://github.com/erthink/libmdbx)
- [Rust Atomics and Locks — Memory Ordering (Release/Acquire)](https://mara.nl/atomics/memory-ordering.html)
- [Rust Atomics — Ordering docs](https://doc.rust-lang.org/std/sync/atomic/enum.Ordering.html)
- [Tokio: `block_in_place` / `Handle::block_on` for bridging](https://docs.rs/tokio/latest/tokio/task/fn.block_in_place.html)
- [reth-libmdbx environment lifetime (Arc-based)](https://reth.rs/docs/reth_db/mdbx/struct.Environment.html)

---
*Pitfalls research for: Rust sidecar state worker thread integration with MDBX commit-head flow control*
*Researched: 2026-03-25*
