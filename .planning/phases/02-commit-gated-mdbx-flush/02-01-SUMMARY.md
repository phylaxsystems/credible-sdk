---
phase: 02-commit-gated-mdbx-flush
plan: 01
subsystem: database
tags: [rust, sidecar, state-worker, mdbx, commit-head, buffering]
requires:
  - phase: 01-in-process-worker-host
    provides: sidecar-owned embedded worker supervision and restart lifecycle
provides:
  - explicit embedded worker control messages for commit-head watermarks
  - sidecar-owned watermark replay across worker restarts
  - buffered state-worker tracing with commit-gated MDBX draining
affects: [phase-02-plan-02, phase-03-mdbx-source-simplification, sidecar-engine, state-worker]
tech-stack:
  added: []
  patterns: [sidecar-owned control channel, replayed watermark recovery, buffered trace-then-drain worker loop]
key-files:
  created: [crates/state-worker/src/control.rs]
  modified:
    [
      crates/sidecar/src/engine/mod.rs,
      crates/sidecar/src/main.rs,
      crates/sidecar/src/state_worker_host.rs,
      crates/state-worker/src/host.rs,
      crates/state-worker/src/worker.rs,
    ]
key-decisions:
  - "Commit-head flush permission is delivered over an explicit in-process control message instead of polling shared state."
  - "The sidecar host caches the latest permitted watermark and replays it into each fresh worker receiver before restart recovery resumes."
  - "Standalone and direct-test worker call sites seed an explicit max watermark so Phase 2 gating only changes the embedded sidecar flow."
patterns-established:
  - "Engine-owned commit progress now fans out to the worker through a dedicated control handle after successful process_commit_head completion."
  - "StateWorker traces into an internal VecDeque and drains only blocks whose numbers are at or below the latest permitted watermark."
requirements-completed: [SYNC-01, SYNC-02]
duration: 15 min
completed: 2026-03-24
---

# Phase 02 Plan 01: Commit-Gated MDBX Flush Summary

**Explicit commit-head watermark control with restart replay and buffered MDBX drains for the embedded state worker**

## Performance

- **Duration:** 15 min
- **Started:** 2026-03-24T21:21:11Z
- **Completed:** 2026-03-24T21:36:49Z
- **Tasks:** 2
- **Files modified:** 10

## Accomplishments
- Added a dedicated `state-worker` control contract and extended the embedded host to recreate receivers on restart while replaying the latest cached watermark.
- Wired `process_commit_head` to publish flush permission only after successful engine commit-head application.
- Split worker tracing from persistence so traced `BlockStateUpdate` values buffer in memory and only drain to MDBX when the latest watermark allows it.

## Task Commits

1. **Task 1: Define the commit-head control contract and embedded-host plumbing** - `381be722` (`feat`)
2. **Task 2: Split tracing from persistence and emit flush permission from `process_commit_head`** - `b189ca56` (`feat`)

Additional direct fix:

1. **Post-task compatibility/lint fix** - `9554655a` (`fix`)

## Files Created/Modified
- `crates/state-worker/src/control.rs` - Explicit commit-head watermark messages for the embedded worker.
- `crates/sidecar/src/state_worker_host.rs` - Sidecar-owned control sender/cache and watermark replay across restarts.
- `crates/state-worker/src/host.rs` - Embedded host wiring for the control receiver and standalone compatibility replay.
- `crates/sidecar/src/engine/mod.rs` - Commit-head success path now emits flush permission to the worker handle.
- `crates/state-worker/src/worker.rs` - FIFO buffering, watermark tracking, and gated MDBX draining.
- `crates/sidecar/src/main.rs` - Passes the worker control handle into the engine configuration.

## Decisions Made
- Used a dedicated `ControlMessage::CommitHead(u64)` contract instead of atomics or polling so flush permission stays explicit and restart-replayable.
- Kept the cached latest watermark in the sidecar host, not the worker, so restart supervision remains sidecar-owned as Phase 1 established.
- Preserved standalone/direct worker callers by explicitly seeding `CommitHead(u64::MAX)` where the embedded sidecar control plane is absent.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Updated direct worker callers for the new control receiver**
- **Found during:** Task 1
- **Issue:** Extending `run_embedded_worker` to require a control receiver broke direct `StateWorker::new` call sites used by standalone execution and existing tests.
- **Fix:** Threaded the receiver through the worker constructor and seeded explicit max-watermark messages in standalone/test paths.
- **Files modified:** `crates/state-worker/src/host.rs`, `crates/state-worker/src/integration_tests/setup.rs`, `crates/state-worker/src/integration_tests/tests.rs`, `crates/state-worker/src/worker.rs`
- **Verification:** `cargo check -p sidecar -p state-worker --locked --features test`
- **Committed in:** `381be722`

**2. [Rule 1 - Bug] Tolerated closed standalone control channels after replay**
- **Found during:** Task 2 verification
- **Issue:** Direct callers send one compatibility watermark and drop the sender, which made the worker treat the closed control channel as fatal and break existing integration tests.
- **Fix:** Marked the control channel as closed after replay/disconnect and continued draining using the last known watermark.
- **Files modified:** `crates/state-worker/src/worker.rs`
- **Verification:** `cargo test -p state-worker --locked test_state_worker_ -- --test-threads=2`, `make lint`
- **Committed in:** `9554655a`

---

**Total deviations:** 2 auto-fixed (1 blocking, 1 bug)
**Impact on plan:** Both fixes were required to keep the new control path compatible with existing worker entry points and repository lint policy. No scope creep.

## Issues Encountered
- `cargo nextest` is not installed in this environment, so targeted verification used equivalent `cargo test` commands instead.
- `forge build --root testdata/mock-protocol` failed on the local `solc 0.7.6` binary with `Bad CPU type in executable (os error 86)`. `FOUNDRY_PROFILE=assertions forge build --root testdata/mock-protocol` succeeded.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- The worker now buffers traced state internally and regains flush permission after restart via sidecar replay, which gives Phase 02-02 the control seam needed for backlog/backpressure behavior.
- MDBX read visibility is still unchanged for consumers; Phase 3 can simplify `MdbxSource` against this new commit-gated write boundary.

## Self-Check: PASSED

---
*Phase: 02-commit-gated-mdbx-flush*
*Completed: 2026-03-24*
