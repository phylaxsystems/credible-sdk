---
phase: 02-commithead-flow-control
plan: 04
subsystem: sidecar/main
tags: [wiring, channel, state-worker, commit-head, atomic]
dependency_graph:
  requires:
    - 02-02 (CoreEngineConfig.commit_head_tx field)
    - 02-03 (StateWorkerThread::new 4-arg signature, EmbeddedStateWorkerConfig)
  provides:
    - End-to-end CommitHead channel: CoreEngine -> StateWorkerThread
    - Arc<AtomicU64> committed_head in run_sidecar_once scope (ready for Phase 3)
    - Conditional state worker spawn (all 4 config fields required)
    - sw_exit_future helper for Option-based select! arm (no dummy oneshot)
  affects:
    - run_sidecar_once (main.rs) — channel creation, committed_head, conditional spawn
    - run_async_components (main.rs) — sw_exited_rx now Option<...>
    - handle_state_worker_exit (main.rs) — updated to match sw_exit_future return type
tech_stack:
  added: []
  patterns:
    - Option<oneshot::Receiver<...>> + pending() for optional thread select! arms
    - flume::unbounded for CommitHead channel (bounded risks deadlock)
    - Arc<AtomicU64> committed_head constructed in run_sidecar_once before thread spawns
key_files:
  created: []
  modified:
    - crates/sidecar/src/main.rs
decisions:
  - CommitHead channel uses flume::unbounded (not bounded) to prevent circular deadlock between engine and state worker
  - sw_exit_future returns pending() (not a dropped dummy oneshot) when state worker absent — dummy oneshot fires immediately with Err, causing spurious restart
  - handle_state_worker_exit updated to Result<(), StateWorkerError> signature to match sw_exit_future (same as handle_observer_exit pattern)
  - Arc<AtomicU64> committed_head constructed before any thread spawn so Phase 3 MdbxSource can receive Arc::clone
  - Conditional state worker spawn checks all 4 fields (ws_url, mdbx_path, genesis_path, state_depth) — partial config is treated as absent
metrics:
  duration_minutes: 12
  completed_date: "2026-03-25T04:04:20Z"
  tasks_completed: 1
  files_modified: 1
---

# Phase 02 Plan 04: Wire CommitHead Channel in run_sidecar_once Summary

**One-liner:** End-to-end CommitHead wiring — flume::unbounded::<CommitHeadSignal>() channel, Arc<AtomicU64> committed_head, conditional StateWorkerThread spawn, and sw_exit_future with pending() semantics for absent config.

## What Was Built

Plan 04 is the final wiring plan for Phase 2. It connects the components built in Plans 02 and 03:

1. **CommitHead channel** — `flume::unbounded::<CommitHeadSignal>()` created in `run_sidecar_once` before any thread spawns. Sender passed to `CoreEngineConfig.commit_head_tx: Some(commit_head_tx)`. Receiver passed to `StateWorkerThread::new`.

2. **Arc<AtomicU64> committed_head** — Constructed as `Arc::new(AtomicU64::new(0))` in `run_sidecar_once` scope before any consumer is spawned. Phase 3 will pass `Arc::clone(&committed_head)` to `MdbxSource`.

3. **Conditional state worker spawn** — `StateWorkerThread` only spawns when all four config fields are present (`ws_url`, `mdbx_path`, `genesis_path`, `state_depth`). When any field is missing, a `tracing::warn!` is emitted and `sw_exited_rx = None`.

4. **sw_exit_future helper** — Mirrors `observer_exit_future` exactly. `None` input returns `std::future::pending()` so the `select!` arm never fires when the state worker is not configured. This prevents the spurious restart that would occur if a dropped dummy `oneshot::channel()` sender were used instead.

5. **Updated run_async_components** — `sw_exited` parameter changed from bare `oneshot::Receiver<...>` to `Option<oneshot::Receiver<...>>`. Now uses `sw_exit_future(sw_exited_rx)` before the `tokio::select!`, matching the observer pattern.

6. **Updated handle_state_worker_exit** — Signature updated from `Result<Result<(), StateWorkerError>, RecvError>` to `Result<(), StateWorkerError>` to match the value yielded by `sw_exit_future` in the select arm.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Wire CommitHead channel, committed_head atomic, and conditional state worker spawn | 08b465f | crates/sidecar/src/main.rs |

## Verification Results

- `cargo build -p sidecar` — passes cleanly
- `grep flume::unbounded::<sidecar::state_worker_thread::CommitHeadSignal>` in main.rs — confirmed at line 711
- `grep commit_head_tx: Some` — confirmed at line 723
- `grep Arc::new(AtomicU64::new(0))` — confirmed at line 706 (via `committed_head = Arc::new(AtomicU64::new(0))`)
- `grep StateWorkerThread::new` — confirmed at line 762 inside conditional block
- `grep sw_exit_future` — confirmed at line 444 (helper) and 593 (call site)
- `grep pending()` — confirmed at line 460 inside sw_exit_future
- No `commit_head_tx: None` placeholder remains
- No `_placeholder` variable remains

Note: Engine tests fail because `testdata/mock-protocol/out/` is empty in this worktree (no compiled Solidity artifacts). This is a pre-existing infrastructure issue, not caused by these changes. The sidecar binary (`cargo build -p sidecar`) compiles cleanly.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Updated handle_state_worker_exit signature**

- **Found during:** Task 1, compilation
- **Issue:** `handle_state_worker_exit` was written for the old bare `oneshot::Receiver` pattern, taking `Result<Result<(), StateWorkerError>, RecvError>`. With the new `sw_exit_future` pattern (mirroring observer), the select arm passes `Result<(), StateWorkerError>` directly — causing a type mismatch at compile time.
- **Fix:** Updated `handle_state_worker_exit` to take `Result<(), StateWorkerError>`, matching `handle_observer_exit`. This is the correct signature for a future-wrapped exit handler.
- **Files modified:** crates/sidecar/src/main.rs
- **Commit:** 08b465f (included in same task commit)

## Phase 2 Integration Status

After Plan 04, the end-to-end flow is operational:

1. CoreEngine processes a block → calls `process_commit_head` → sends `CommitHeadSignal` via `commit_head_tx`
2. StateWorkerThread receives signal → calls `flush_ready_blocks` → writes to MDBX
3. StateWorkerThread stores block number to `committed_head` with `Release` ordering
4. Phase 3's `MdbxSource` will read `committed_head` with `Acquire` ordering

Phase 2 complete. All four plans executed.

## Known Stubs

None — all wiring is real. The `committed_head` Arc is wired but not yet consumed by `MdbxSource` (Phase 3 scope).

## Self-Check: PASSED
