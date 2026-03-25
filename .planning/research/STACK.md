# Technology Stack: State Worker Thread Integration

**Project:** Credible SDK — State Worker Thread Integration
**Researched:** 2026-03-25
**Milestone:** Embedding state worker as in-process OS thread with CommitHead flow control
**Overall confidence:** HIGH — all recommendations verified against existing codebase patterns and official Tokio documentation

---

## Recommended Stack

This milestone adds **no new crate dependencies**. Every primitive required is already present in the workspace. The stack is defined by which existing tools to use for each sub-problem.

### Thread Spawning

| Mechanism | Choice | Version | Rationale |
|-----------|--------|---------|-----------|
| Thread creation | `std::thread::Builder::new()` | stdlib | Match existing CoreEngine, EventSequencing, TransactionObserver pattern exactly |
| Thread naming | `.name("sidecar-state-worker".into())` | stdlib | Named threads appear in profiler and panic messages |

**Why `std::thread::Builder::new()`, not `tokio::task::spawn_blocking`:**

`spawn_blocking` occupies a slot in Tokio's blocking thread pool. The official Tokio docs explicitly state: "For workloads that run indefinitely or for extended periods (for example, background workers), prefer a dedicated thread created with `thread::spawn`." (HIGH confidence — verified against `docs.rs/tokio/latest/tokio/task/fn.spawn_blocking`). The state worker is a perpetual loop; `spawn_blocking` would deprive the pool of capacity for short-lived blocking operations. Three existing precedents in this codebase confirm `std::thread::Builder::new()` as the project standard for long-lived threads.

**Why not `tokio::task::spawn` (async task on main runtime):**

The sidecar explicitly isolates CoreEngine, EventSequencing, and TransactionObserver onto their own OS threads to prevent one slow component from starving the main tokio runtime's I/O capacity. The state worker does CPU-heavy tracing and MDBX writes; it must not block the main runtime's worker threads.

### Tokio Runtime Inside the Thread

| Mechanism | Choice | Version | Rationale |
|-----------|--------|---------|-----------|
| Runtime variant | `tokio::runtime::Builder::new_current_thread().enable_all().build()` | tokio 1.49.0 (resolved) | Single-threaded, isolated from sidecar main runtime |
| Runtime driver | `.block_on(run_state_worker_loop(...))` | tokio 1.49.0 | Drives async future to completion on the dedicated OS thread |

**Why `new_current_thread`, not multi-thread:**

The state worker performs sequential work: trace block → buffer → receive CommitHead signal → flush. There is no benefit from a multi-thread scheduler and using one would create another scheduler competing with the sidecar's own multi-thread scheduler for CPU cores. `new_current_thread` is exactly what TransactionObserver uses (verified: `crates/sidecar/src/transaction_observer/mod.rs:215`).

**Why `.enable_all():`**

The state worker needs timers (for WebSocket keepalives and tracing timeouts) and I/O (WebSocket). Both are enabled by `enable_all()`. Omitting either causes runtime panics when those primitives are first used.

**Construction error handling:**

```rust
let runtime = tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()
    .map_err(StateWorkerError::RuntimeBuild)?;
```

Do not use `unwrap`/`expect` — workspace Clippy denies both.

### Panic Catching

| Mechanism | Choice | Version | Rationale |
|-----------|--------|---------|-----------|
| Panic interception | `std::panic::AssertUnwindSafe(future).catch_unwind().await` | stdlib | Existing state-worker pattern; catches panic payload without aborting process |
| Payload inspection | `downcast_ref::<&str>()` then `downcast_ref::<String>()` | stdlib | Both string formats appear in practice |

**The existing state-worker already demonstrates this pattern** (`crates/state-worker/src/main.rs:82`):

```rust
let result = AssertUnwindSafe(run_once(&args)).catch_unwind().await;
match result {
    Ok(Ok(())) => { /* normal exit */ }
    Ok(Err(err)) => { /* error exit */ }
    Err(panic_payload) => {
        if let Some(message) = panic_payload.downcast_ref::<&str>() {
            warn!(panic = %message, "state worker panicked; restarting");
        } else if let Some(message) = panic_payload.downcast_ref::<String>() {
            warn!(panic = %message, "state worker panicked; restarting");
        } else {
            warn!("state worker panicked; restarting");
        }
    }
}
```

When the state worker is embedded as an OS thread (instead of a standalone async main), the `catch_unwind` moves to the thread closure wrapping the `block_on` call. The OS thread's `JoinHandle::join()` returning `Err(_)` is the secondary detection mechanism used by `ThreadHandles::join_all()` in `crates/sidecar/src/main.rs:289`.

**Soundness caveat (MEDIUM confidence):** `AssertUnwindSafe` tells the compiler the wrapped value is safe across unwind boundaries regardless of whether it actually is — it silences the check. This is the pragmatic trade-off the existing codebase already accepts. For the state worker context (writes to MDBX and signals via channels), the risk is that a panic mid-write leaves MDBX in an intermediate state; MDBX's own transaction semantics already handle this (uncommitted write transactions are automatically rolled back on process/thread drop).

### Restart Backoff

| Mechanism | Choice | Rationale |
|-----------|--------|-----------|
| Backoff strategy | Fixed 1-second delay, `tokio::time::sleep(Duration::from_secs(1))` | Match existing state-worker binary exactly |
| Counter | Saturating `u64`, logged on each restart | Match existing state-worker binary exactly |
| Backoff crate | None — do not add | No exponential backoff crate exists in the workspace; fixed 1s matches the current binary; the sidecar's overall restart strategy is also immediate re-try |

**Why not add `backon` or `backoff`:**

The PROJECT.md constraint is "match existing OS thread patterns." The existing state-worker uses a fixed 1-second restart delay with a saturating counter (`crates/state-worker/src/main.rs:102-109`). The sidecar's own outer restart loop has no delay at all. Adding a backoff crate would introduce a new dependency for no behavioral benefit at this milestone — and would diverge from the pattern that operators already understand. If exponential backoff is desired in the future, it can be added in a dedicated improvement.

**What the existing pattern looks like for the embedded thread context:**

```rust
let mut restart_count: u64 = 0;
loop {
    // spawn OS thread, block until it exits
    let result = spawn_state_worker_thread(...);
    // inspect result, log panic/error
    restart_count = restart_count.saturating_add(1);
    tokio::time::sleep(Duration::from_secs(1)).await;
}
```

### CommitHead Signal Channel

| Dimension | Choice | Version | Rationale |
|-----------|--------|---------|-----------|
| Channel type | `flume::bounded(128)` | 0.12.0 | Bounded capacity applies backpressure; engine blocks if state worker is stalled |
| Sender ownership | `CoreEngine` (async context, `flume::Sender::send_async().await`) | — | Engine lives on an OS thread with its own tokio runtime; async send is non-blocking |
| Receiver ownership | `StateWorker` OS thread (sync `recv_timeout`) | — | State worker drives a blocking receive loop; `flume::Receiver::recv_timeout` is the established pattern |

**Why `flume::bounded`, not `tokio::sync::mpsc`:**

`tokio::sync::mpsc` is async-native — its blocking-thread-facing API (`blocking_send`, `blocking_recv`) is a special-case wrapper. In this codebase, `flume` is the established cross-thread channel primitive; all existing OS thread-to-OS thread communication uses it. `flume` channels work identically from sync and async contexts with no runtime requirement at the receiver. Using `tokio::sync::mpsc` here would introduce a mixed-primitive pattern without benefit.

`tokio::sync::mpsc` does appear in the codebase (e.g., `crates/sidecar/src/transport/grpc/server.rs:883`) but only between tokio tasks within the same runtime, never across OS thread boundaries. That distinction should be preserved.

**Why bounded at 128:**

This is the buffer depth stated in PROJECT.md R2: "Buffer is bounded at 128 blocks." The CommitHead channel bound should match this buffer capacity so that channel backpressure kicks in before the in-memory block buffer overflows. A full channel means the engine's send will block (or return an error), which is the intended flow-control signal.

**Why not `std::sync::mpsc`:**

`std::sync::mpsc` is single-consumer only. `flume` is a drop-in replacement with MPMC semantics, a richer API (`recv_timeout` matching what the existing threads use), and it's already a workspace dependency. `std::sync::mpsc::Receiver::recv_timeout` exists but returns `std::sync::mpsc::RecvTimeoutError`, creating two different error type families in the same codebase.

### Shared Height Tracking

| Dimension | Choice | Version | Rationale |
|-----------|--------|---------|-----------|
| Type | `Arc<AtomicU64>` | stdlib | Zero-cost read path; no lock contention for a single writer / single reader pattern |
| Ordering (write) | `Ordering::Release` | stdlib | Ensures all MDBX writes prior to the store are visible before the height update |
| Ordering (read) | `Ordering::Acquire` | stdlib | Pairs with Release; `MdbxSource` observes consistent state |
| Notification | None — polled on `MdbxSource::basic_ref` | stdlib | PROJECT.md R3 removes the 50ms polling timer; `MdbxSource` reads the atomic on demand rather than on a background timer |

**Why `Arc<AtomicU64>` is sufficient without `tokio::sync::Notify`:**

The 50ms polling timer is what PROJECT.md removes. `MdbxSource` does not need push notification — it reads height on demand when the executor calls `basic_ref`. An atomic read on the hot path adds nanoseconds, not microseconds. `tokio::sync::Notify` would be correct if `MdbxSource` needed to _await_ a height advance, but that is not the access pattern here.

Existing `Arc<AtomicU64>` usage in the codebase (e.g., `crates/sidecar/src/transport/grpc/server.rs:880`) confirms the pattern is established and the team is comfortable with it.

### Exit Signaling (Thread to Sidecar Main)

| Mechanism | Choice | Rationale |
|-----------|--------|-----------|
| Exit notification | `tokio::sync::oneshot` channel | Match CoreEngine and EventSequencing pattern exactly |
| Handle type | `std::thread::JoinHandle<Result<(), StateWorkerError>>` | Returned from `spawn`, collected by `ThreadHandles::join_all` |

This pattern is used verbatim in all three existing OS threads (verified: `crates/sidecar/src/event_sequencing/mod.rs:233`, `crates/sidecar/src/engine/mod.rs:1287`).

---

## Alternatives Considered

| Category | Recommended | Alternative | Why Not |
|----------|-------------|-------------|---------|
| Thread creation | `std::thread::Builder::new()` | `tokio::task::spawn_blocking` | `spawn_blocking` is for short-lived blocking; long-lived tasks exhaust the blocking thread pool per official Tokio docs |
| Runtime isolation | `new_current_thread` per state worker thread | Shared sidecar main runtime | Cross-runtime task starvation; existing threads use isolation for exactly this reason |
| CommitHead channel | `flume::bounded(128)` | `tokio::sync::mpsc::channel(128)` | `tokio::sync::mpsc` is async-native with a special blocking wrapper; `flume` is already the established cross-thread primitive in this codebase |
| CommitHead channel | `flume::bounded(128)` | `std::sync::mpsc` | SPSC-only, no `recv_timeout` matching existing patterns, separate error type family |
| Height sharing | `Arc<AtomicU64>` | `Arc<tokio::sync::RwLock<u64>>` | Lock contention on every executor state access; atomics are lock-free and sufficient for single-writer single-reader |
| Height sharing | `Arc<AtomicU64>` | `Arc<AtomicU64>` + `tokio::sync::Notify` | Notify adds push semantics not required by `MdbxSource` on-demand read pattern |
| Backoff | Fixed 1s (manual) | `backon` crate | No existing workspace dependency; adds a crate for behavior already implemented in 3 lines |
| Panic catching | `AssertUnwindSafe + catch_unwind` | OS thread panic = sidecar crash | Sidecar must keep running when state worker fails; `EthRpcSource` is the fallback |

---

## What NOT to Use

**Do not use `tokio::task::spawn_blocking` for the state worker.** The official Tokio documentation is explicit: long-lived tasks in the blocking thread pool queue behind new `spawn_blocking` calls once the pool is saturated. This is a reliability hazard for a service that also uses `spawn_blocking` internally for MDBX reads via `tokio::task::block_in_place` (if applicable).

**Do not use `tokio::sync::watch` for CommitHead.** Watch channels deliver only the _latest_ value — if the engine sends two CommitHead events before the state worker processes them, only the second is visible. Ordered delivery matters: each CommitHead should trigger a discrete flush up to that block number. Use a bounded MPSC channel so all CommitHead events are delivered in order.

**Do not use `tokio::runtime::Builder::new_multi_thread` for the state worker's runtime.** The state worker is I/O-bound and sequential; a multi-thread scheduler spawns additional OS threads and competes with the sidecar's main multi-thread runtime for CPU cores. `new_current_thread` gives full async capability (timers, I/O) on a single OS thread, which is the right trade-off.

**Do not reach for a retry crate (`backon`, `backoff`, `tokio-retry`).** The restart loop is 4 lines of code. Adding a dependency for this creates churn in `Cargo.toml`, `deny.toml` license review, and future upgrade surface — for no behavioral gain over the fixed 1-second delay the existing state-worker already uses.

---

## Cargo Changes

No new workspace dependencies are required. All primitives are from:

- `tokio` 1.49.0 (resolved, `^1.48.0` in `Cargo.toml`) — `tokio::runtime::Builder`, `tokio::time::sleep`, `tokio::sync::oneshot`
- `flume` 0.12.0 (resolved) — `flume::bounded`, `flume::Sender`, `flume::Receiver`
- `std` stdlib — `std::thread::Builder`, `std::sync::Arc`, `std::sync::atomic::AtomicU64`, `std::panic::AssertUnwindSafe`, `std::panic::catch_unwind`

The only Cargo change is adding `flume` and `tokio` as dependencies to the `sidecar` crate's `Cargo.toml` `[dependencies]` section if not already present (they likely already are given the existing sidecar uses both).

---

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| `std::thread::Builder::new()` for thread spawning | HIGH | Three existing precedents in codebase; official Tokio docs confirm `spawn_blocking` is wrong for long-lived tasks |
| `new_current_thread` runtime in the thread | HIGH | TransactionObserver uses this exact pattern; verified at `transaction_observer/mod.rs:215` |
| `flume::bounded` for CommitHead channel | HIGH | Established cross-thread channel primitive; `recv_timeout` API matches all existing OS thread loops |
| `Arc<AtomicU64>` for height sharing | HIGH | Established pattern in codebase (grpc/server.rs); correct for single-writer single-reader |
| `AssertUnwindSafe + catch_unwind` | HIGH | Existing state-worker binary uses this verbatim; `JoinHandle::join()` Err branch is secondary detection |
| Fixed 1s restart backoff | HIGH | Copied directly from existing state-worker binary |
| No new crate dependencies required | HIGH | Every primitive is available from existing workspace dependencies |

---

## Sources

- [tokio::task::fn.spawn_blocking — official docs](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html) — long-lived task warning
- [tokio::runtime::Builder — official docs](https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html) — `new_current_thread` semantics
- [tokio::sync::mpsc — official docs](https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html) — bounded channel backpressure behavior
- [flume CHANGELOG](https://github.com/zesterer/flume/blob/master/CHANGELOG.md) — 0.12.0 released 2025-12-08
- `crates/sidecar/src/event_sequencing/mod.rs` — `std::thread::Builder::new()` + `flume::recv_timeout` pattern
- `crates/sidecar/src/engine/mod.rs` — `std::thread::Builder::new()` + oneshot exit notification
- `crates/sidecar/src/transaction_observer/mod.rs` — `new_current_thread` runtime inside OS thread
- `crates/state-worker/src/main.rs` — `AssertUnwindSafe + catch_unwind` + fixed 1s restart backoff
