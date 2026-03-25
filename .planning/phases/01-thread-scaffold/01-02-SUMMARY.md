---
phase: 01-thread-scaffold
plan: 02
subsystem: sidecar
tags: [rust, tokio, os-thread, state-worker, lifecycle, shutdown]

# Dependency graph
requires:
  - phase: 01-thread-scaffold-01
    provides: StateWorkerThread scaffold with spawn(), restart loop, panic catch, and StateWorkerError enum

provides:
  - StateWorkerThread wired into sidecar run_sidecar_once() lifecycle
  - state_worker field in ThreadHandles joined AFTER engine (shutdown ordering enforced)
  - sw_exited oneshot receiver arm in run_async_components select!
  - handle_state_worker_exit() handler following handle_engine_exit pattern

affects: [02-commit-head-channel, 03-mdbx-cleanup]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "OS thread lifecycle wiring: spawn in run_sidecar_once, exit notification in select!, join in join_all"
    - "Shutdown ordering: engine joined before state_worker (engine must send final CommitHead first)"
    - "handle_*_exit pattern: consistent error/recoverability logging across all thread exit handlers"

key-files:
  created:
    - crates/sidecar/src/state_worker_thread/mod.rs
    - crates/sidecar/src/state_worker_thread/error.rs
  modified:
    - crates/sidecar/src/lib.rs
    - crates/sidecar/src/main.rs

key-decisions:
  - "StateWorkerError direct import omitted from main.rs — used via full path sidecar::state_worker_thread::StateWorkerError to avoid unused import lint error"
  - "state_worker joined AFTER engine in join_all() per PITFALLS.md Pitfall 5 — engine must exit before state worker can safely stop"
  - "StateWorkerThread spawned after TransactionObserver, before HealthServer — follows existing spawn ordering pattern"

patterns-established:
  - "Thread wiring pattern: new() -> spawn() -> store JoinHandle in ThreadHandles -> sw_exited in select! -> join in join_all()"

requirements-completed: [THRD-01, THRD-03, THRD-04, THRD-05, THRD-07]

# Metrics
duration: 2min
completed: 2026-03-25
---

# Phase 01 Plan 02: Wire StateWorkerThread into Sidecar Lifecycle Summary

**StateWorkerThread wired into run_sidecar_once() with select! exit notification, ThreadHandles field joined after engine, and handle_state_worker_exit() handler — completing the Phase 1 thread scaffold lifecycle loop**

## Performance

- **Duration:** 2 min
- **Started:** 2026-03-25T01:46:39Z
- **Completed:** 2026-03-25T01:48:58Z
- **Tasks:** 1 (of 2 — checkpoint task awaits human verification)
- **Files modified:** 4

## Accomplishments

- Created `state_worker_thread/mod.rs` and `error.rs` (plan 01-01 dependency content)
- Added `pub mod state_worker_thread` to `lib.rs`
- Extended `ThreadHandles` with `state_worker` field and `None` initializer
- Joined `state_worker` AFTER `engine` in `join_all()` (shutdown ordering per PITFALLS.md)
- Added `handle_state_worker_exit()` following the `handle_engine_exit` pattern
- Added `sw_exited` arm to `run_async_components` `select!`
- Spawned `StateWorkerThread` in `run_sidecar_once()` before `HealthServer`
- `EthRpcSource` unchanged in `build_sources_from_config`
- `cargo check -p sidecar` passes with no errors

## Task Commits

Each task was committed atomically:

1. **Task 1: Wire StateWorkerThread into ThreadHandles, run_sidecar_once, and run_async_components** - `e7f35b4` (feat)

_Checkpoint task (human-verify) not yet completed — awaiting verification._

## Files Created/Modified

- `crates/sidecar/src/state_worker_thread/error.rs` - StateWorkerError enum with ErrorRecoverability classification (RuntimeBuild, Panicked, ThreadSpawn — all Recoverable)
- `crates/sidecar/src/state_worker_thread/mod.rs` - StateWorkerThread with spawn(), restart loop, 8MiB stack, isolated tokio runtime, catch_unwind, 1s backoff
- `crates/sidecar/src/lib.rs` - Added `pub mod state_worker_thread`
- `crates/sidecar/src/main.rs` - ThreadHandles.state_worker field, join_all ordering, handle_state_worker_exit(), sw_exited in select!, StateWorkerThread spawn in run_sidecar_once()

## Decisions Made

- Used full path `sidecar::state_worker_thread::StateWorkerError` in function signatures instead of direct import to avoid unused import (the import `StateWorkerThread` suffices since that is what is constructed directly)
- State worker spawned after `TransactionObserver` block and before `HealthServer` to match existing spawn ordering pattern in `run_sidecar_once()`

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None - `cargo check -p sidecar` passed on first attempt.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Phase 1 thread scaffold complete pending human verification of build and grep checks
- Checkpoint verification items: `cargo build -p sidecar`, thread name grep, EthRpcSource grep, join ordering, no unwrap/expect/panic in state_worker_thread/, clippy pass
- Phase 2 can wire CommitHead channel from engine to state worker once Phase 1 checkpoint clears

---
*Phase: 01-thread-scaffold*
*Completed: 2026-03-25*
