---
phase: 03-mdbxsource-simplification-and-cleanup
plan: "02"
subsystem: database
tags: [mdbx, circular-buffer, state-worker, commit-head]

# Dependency graph
requires:
  - phase: 02-commithead-flow-control
    provides: CommitHead gating — MDBX writes only after engine signals commit_head

provides:
  - StateWriter in build_worker always opens with CircularBufferConfig::new(1)
  - state_depth config field no longer read inside build_worker (write side)

affects:
  - 03-03-PLAN.md (final cleanup — standalone binary removal)

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Hardcode mdbx CircularBufferConfig depth to 1 — depth > 1 unnecessary with CommitHead gating"

key-files:
  created: []
  modified:
    - crates/sidecar/src/state_worker_thread/mod.rs

key-decisions:
  - "StateWriter CircularBufferConfig depth hardcoded to 1 (write side, SIMP-02) — CommitHead gating makes going too far architecturally impossible so multiple replicas provide no benefit"

patterns-established:
  - "When CommitHead flow control gates all MDBX writes, circular buffer depth must be 1 — enforced at construction time with hardcoded value and explanatory comment"

requirements-completed: [SIMP-02]

# Metrics
duration: 2min
completed: 2026-03-25
---

# Phase 03 Plan 02: StateWriter Depth Hardcoded Summary

**StateWriter in the embedded state worker always opens MDBX with CircularBufferConfig::new(1) — config-driven state_depth removed from build_worker, completing SIMP-02 write side**

## Performance

- **Duration:** 2 min
- **Started:** 2026-03-25T04:45:08Z
- **Completed:** 2026-03-25T04:46:43Z
- **Tasks:** 1
- **Files modified:** 1

## Accomplishments
- Removed the `state_depth` config variable and its `ok_or_else` validation from `build_worker`
- Replaced `CircularBufferConfig::new(state_depth)` with `CircularBufferConfig::new(1)` plus a SIMP-02 comment
- SIMP-02 (write side) satisfied — namespace-rotation overhead eliminated

## Task Commits

Each task was committed atomically:

1. **Task 1: Hardcode StateWriter circular buffer depth to 1 in build_worker** - `571cae4` (feat)

**Plan metadata:** (docs commit follows)

## Files Created/Modified
- `crates/sidecar/src/state_worker_thread/mod.rs` - Removed state_depth config read; hardcoded CircularBufferConfig::new(1) with SIMP-02 rationale comment

## Decisions Made
- StateWriter CircularBufferConfig depth hardcoded to 1 (write side) — consistent with Plan 01's reader side hardcoding. Both sides now unconditionally open with depth 1, completing SIMP-02 in full.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Both StateReader (Plan 01) and StateWriter (Plan 02) now use depth 1 unconditionally
- SIMP-02 fully satisfied on both read and write sides
- Plan 03 (standalone state-worker binary removal) can proceed

## Self-Check: PASSED
- FOUND: SUMMARY.md at `.planning/phases/03-mdbxsource-simplification-and-cleanup/03-02-SUMMARY.md`
- FOUND: commit 571cae4
- FOUND: `CircularBufferConfig::new(1)` exactly once in mod.rs
- PASSED: `state_depth` fully removed from mod.rs
