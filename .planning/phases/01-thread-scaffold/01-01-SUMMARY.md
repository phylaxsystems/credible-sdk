---
phase: 01-thread-scaffold
plan: 01
subsystem: threading
tags: [rust, tokio, os-thread, panic-recovery, shutdown, sidecar, state-worker]

# Dependency graph
requires: []
provides:
  - StateWorkerThread struct with spawn() method producing (JoinHandle, oneshot::Receiver)
  - StateWorkerError enum with RuntimeBuild, Panicked, ThreadSpawn variants
  - ErrorRecoverability classification for StateWorkerError (all Recoverable)
  - Isolated single-threaded tokio runtime inside OS thread
  - Panic isolation via catch_unwind(AssertUnwindSafe(...))
  - 1-second restart backoff with saturating restart counter
  - Graceful shutdown via Arc<AtomicBool> polled at 100ms intervals
  - crates/sidecar/src/state_worker_thread module registered in lib.rs
affects:
  - 01-02 (next plan — will wire actual state worker logic into run_blocking_inner)
  - Any plan that integrates StateWorkerThread into run_sidecar_once

# Tech tracking
tech-stack:
  added: []
  patterns:
    - OS thread with isolated single-threaded tokio runtime (matches EventSequencing/CoreEngine pattern)
    - catch_unwind(AssertUnwindSafe(|| fn())) for panic isolation
    - StateWorkerThreadResult = Result<(JoinHandle, oneshot::Receiver), StateWorkerError>
    - Error variants use String (not io::Error) for Clone compatibility with oneshot send
    - Shutdown polling: Arc<AtomicBool> loaded with Ordering::Relaxed at 100ms intervals

key-files:
  created:
    - crates/sidecar/src/state_worker_thread/error.rs
    - crates/sidecar/src/state_worker_thread/mod.rs
  modified:
    - crates/sidecar/src/lib.rs

key-decisions:
  - "Error variants use String wrapping (not Arc<io::Error>) to satisfy Clone required by oneshot send pattern"
  - "All StateWorkerError variants are Recoverable — panic/runtime/spawn failures are transient; EthRpcSource covers downtime window"
  - "spawn() takes self (not &mut self) matching EventSequencing pattern; shutdown passed as Arc<AtomicBool>"
  - "run_thread_loop is a free function (not method) so it can be called inside the spawned closure without self borrow issues"
  - "Phase 1 run_blocking_inner is a no-op poll loop; Phase 2+ will replace with actual StateWorker::run()"

patterns-established:
  - "StateWorkerThreadResult type alias: Result<(JoinHandle<Result<(), E>>, oneshot::Receiver<Result<(), E>>), E>"
  - "Restart loop: catch_unwind -> match Ok/Err/Panic -> saturating_add(1) -> sleep(1s)"
  - "Inner runtime: Builder::new_current_thread().enable_all().build().map_err(|e| Error::RuntimeBuild(e.to_string()))"

requirements-completed: [THRD-01, THRD-02, THRD-03, THRD-04, THRD-06, THRD-07]

# Metrics
duration: 3min
completed: 2026-03-25
---

# Phase 01 Plan 01: Thread Scaffold Summary

**StateWorkerThread OS thread scaffold with isolated tokio runtime, catch_unwind panic isolation, 1-second restart backoff, and 100ms shutdown polling — matching the EventSequencing/CoreEngine lifecycle pattern**

## Performance

- **Duration:** ~3 min
- **Started:** 2026-03-25T01:20:23Z
- **Completed:** 2026-03-25T01:23:43Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments

- Created `StateWorkerError` enum with 3 variants (RuntimeBuild, Panicked, ThreadSpawn) and `From<&StateWorkerError> for ErrorRecoverability` — all variants Recoverable
- Created `StateWorkerThread::spawn()` producing `(JoinHandle, oneshot::Receiver)` matching the EventSequencing convention
- Thread named `sidecar-state-worker` with explicit 8MiB stack, isolated `new_current_thread` tokio runtime, `catch_unwind` panic isolation, and 1-second restart backoff loop
- Registered `pub mod state_worker_thread` in `crates/sidecar/src/lib.rs`
- `cargo check -p sidecar` passes with zero errors

## Task Commits

Each task was committed atomically:

1. **Task 1: Create StateWorkerError enum with ErrorRecoverability classification** - `e6a1143` (feat)
2. **Task 2: Create StateWorkerThread module with spawn, restart loop, isolated runtime, panic catch, and shutdown polling** - `8f74414` (feat)

## Files Created/Modified

- `crates/sidecar/src/state_worker_thread/error.rs` — StateWorkerError enum + From<&StateWorkerError> for ErrorRecoverability
- `crates/sidecar/src/state_worker_thread/mod.rs` — StateWorkerThread struct, spawn(), run_thread_loop(), run_blocking_inner()
- `crates/sidecar/src/lib.rs` — Added `pub mod state_worker_thread` declaration

## Decisions Made

- Error variants use `String` wrapping (not `Arc<io::Error>`) to satisfy `Clone` required by the oneshot send pattern (matching EventSequencingError). RuntimeBuild's `io::Error` is converted with `.to_string()`.
- All three StateWorkerError variants are Recoverable — transient failures (panics, resource pressure, OS exhaustion) allow restart with EthRpcSource covering the window.
- `spawn()` takes `self` (not `&mut self`) matching the EventSequencing pattern exactly. The shutdown `Arc<AtomicBool>` is passed into the closure directly.
- `run_thread_loop` is a free function (not `&self` method) to avoid self-borrow issues inside the spawned closure.
- Phase 1 `run_blocking_inner` is a no-op polling loop — Phase 2+ will replace its body with `StateWorker::run()` inside the same runtime.

## Deviations from Plan

None — plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

- Thread scaffold is complete and compiles clean; every subsequent plan in phase 01 can depend on `StateWorkerThread::spawn()` existing with correct lifecycle semantics.
- Plan 01-02 will wire the actual state worker logic into `run_blocking_inner`, replacing the current no-op shutdown poll.
- No blockers.

---
*Phase: 01-thread-scaffold*
*Completed: 2026-03-25*
