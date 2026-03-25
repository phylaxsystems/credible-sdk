---
phase: 03-mdbxsource-simplification-and-cleanup
plan: "01"
subsystem: cache
tags: [mdbx, atomic, arc, sync, state-worker, circular-buffer]

# Dependency graph
requires:
  - phase: 02-commithead-flow-control
    provides: "committed_head Arc<AtomicU64> constructed in run_sidecar_once; StateWorkerThread writes it with Release ordering after each flush"
provides:
  - "Simplified MdbxSource with AtomicU64 sync check (no MDBX polling)"
  - "committed_head Arc wired from main.rs into MdbxSource construction"
  - "StateReader circular buffer depth hardcoded to 1 (SIMP-02 read side)"
affects:
  - 03-02-PLAN (StateWriter depth hardcoded to 1 — write side)
  - 03-03-PLAN (standalone state-worker binary removal)

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Arc<AtomicU64> acquire/release ordering for cross-thread sync signal: writer stores with Release, reader loads with Acquire"
    - "is_synced check: committed == 0 fast-path returns false (no blocks flushed yet)"

key-files:
  created: []
  modified:
    - crates/sidecar/src/cache/sources/mdbx/mod.rs
    - crates/sidecar/src/main.rs

key-decisions:
  - "MdbxSource::is_synced reads committed_head.load(Ordering::Acquire) — no MDBX reads, no 50ms polling latency"
  - "update_cache_status is a no-op — range intersection and CacheStatus struct removed entirely"
  - "StateReader CircularBufferConfig depth hardcoded to 1 — multiple replicas no longer needed"
  - "committed_head constructed before build_sources_from_config in run_sidecar_once — MdbxSource and StateWorkerThread share the same Arc"
  - "All DatabaseRef methods read committed_head.load(Ordering::Acquire) instead of target_block.read()"

patterns-established:
  - "Acquire/Release pairing: StateWorkerThread flush stores committed_head with Release; MdbxSource.is_synced loads with Acquire"

requirements-completed: [SIMP-01, SIMP-03, SIMP-04]

# Metrics
duration: 8min
completed: 2026-03-25
---

# Phase 03 Plan 01: MdbxSource Simplification Summary

**MdbxSource sync replaced from 50ms MDBX polling loop to Arc<AtomicU64> committed_head read; all range-intersection and 'went too far' code deleted; StateReader depth hardcoded to 1**

## Performance

- **Duration:** 8 min
- **Started:** 2026-03-25T04:33:32Z
- **Completed:** 2026-03-25T04:41:10Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- MdbxSource struct reduced from 6 fields to 2 (backend + committed_head)
- Eliminated 50ms polling loop and range-intersection latency from is_synced
- Deleted 914 lines of net code (870 lines of tests for removed logic + polling infrastructure)
- StateReader circular buffer depth hardcoded to 1 (SIMP-02 read side)
- Clean cargo build with zero errors

## Task Commits

Each task was committed atomically:

1. **Task 1: Rewrite MdbxSource to read Arc<AtomicU64> instead of polling MDBX** - `bd56c7f` (feat)
2. **Task 2: Wire committed_head Arc into MdbxSource construction in main.rs** - `e507f04` (feat)

## Files Created/Modified
- `crates/sidecar/src/cache/sources/mdbx/mod.rs` - Rewrote MdbxSource: removed polling, range-intersection, CacheStatus, and Drop impl; added committed_head: Arc<AtomicU64>; rewrote is_synced and update_cache_status; deleted all unit tests for removed functions
- `crates/sidecar/src/main.rs` - Updated build_sources_from_config signature and call sites; hardcoded CircularBufferConfig::new(1) for both MDBX reader paths; moved committed_head construction before sources build; removed Phase 3 placeholder comment

## Decisions Made
- Used `committed == 0` fast-path in is_synced as sentinel for "no blocks flushed yet" — avoids serving stale state on startup
- Kept all four DatabaseRef methods reading committed_head.load(Ordering::Acquire) directly — consistent approach, each method independently correct
- Removed the `state_worker_depth` config binding in legacy path since depth is now hardcoded — prevents unused variable warnings

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Worktree was missing phase 01 and 02 code**
- **Found during:** Pre-execution setup
- **Issue:** The worktree was checked out from gsd-cl (main), which didn't have the StateWorkerThread and committed_head code from phases 01/02
- **Fix:** Merged gsd/phase-03-mdbxsource-simplification-and-cleanup branch (fast-forward) to bring all prior phase code into the worktree
- **Files modified:** All phase 01/02 code files
- **Verification:** committed_head found at expected line in main.rs; cargo check showed only the 2 expected MdbxSource::new arity errors
- **Committed in:** git merge (not a separate commit — fast-forward merge)

---

**Total deviations:** 1 auto-fixed (1 blocking — missing prior phase code in worktree)
**Impact on plan:** Essential for correct execution. No scope creep.

## Issues Encountered
- EthRpcSource has its own `calculate_target_block` function (3 args, separate method) — acceptance criteria "zero matches in crates/sidecar" was interpreted as zero matches in the mdbx module specifically. The MdbxSource's 4-arg `calculate_target_block` is confirmed deleted.

## Next Phase Readiness
- Plan 02 can now hardcode StateWriter circular buffer depth to 1 (write side of SIMP-02)
- Plan 03 can remove standalone state-worker binary from workspace
- MdbxSource is fully wired — the Arc<AtomicU64> sync mechanism is live end-to-end

---
*Phase: 03-mdbxsource-simplification-and-cleanup*
*Completed: 2026-03-25*

## Self-Check: PASSED

- mdbx/mod.rs: FOUND
- main.rs: FOUND
- 03-01-SUMMARY.md: FOUND
- Commit bd56c7f: FOUND
- Commit e507f04: FOUND
