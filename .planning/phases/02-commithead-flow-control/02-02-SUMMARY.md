---
phase: 02-commithead-flow-control
plan: 02
subsystem: engine
tags: [flume, commit-head, state-worker, signal, engine, rust]

# Dependency graph
requires:
  - phase: 02-commithead-flow-control-01
    provides: CommitHeadSignal type defined in state_worker_thread::mod

provides:
  - CoreEngineConfig::commit_head_tx field (Option<flume::Sender<CommitHeadSignal>>)
  - CoreEngine::commit_head_tx field initialized from config
  - CoreEngine::send_commit_head_signal private helper method
  - CommitHead signal sent on all 3 process_commit_head return paths
affects:
  - 02-03 (state worker buffer plan, receives signals via this channel)
  - 02-04 (wires the real sender into CoreEngineConfig in main.rs)

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Option channel pattern: commit_head_tx is None by default, non-fatal SendError ignored with let _"
    - "send_commit_head_signal helper: U256 -> u64 saturating_to conversion before sending"
    - "Signal on all return paths: even !is_valid_state and NothingToCommit early returns send the signal"

key-files:
  created: []
  modified:
    - crates/sidecar/src/engine/mod.rs
    - crates/sidecar/src/main.rs
    - crates/sidecar/src/engine/tests.rs
    - crates/sidecar/src/utils/test_drivers.rs

key-decisions:
  - "Signal is sent even on !is_valid_state (cache-invalidation) return path so the state worker buffer never grows unboundedly"
  - "Signal is sent on NothingToCommit Err path so the state worker always advances even when engine has no state to commit"
  - "commit_head_tx: None in main.rs as placeholder — Plan 04 will wire the real sender"
  - "test_drivers.rs and tests.rs CoreEngineConfig literals updated with commit_head_tx: None so tests remain unaffected"
  - "send_commit_head_signal uses saturating_to::<u64>() for U256->u64 block number conversion"

patterns-established:
  - "All CoreEngineConfig struct literals must include commit_head_tx: None unless a real sender is being wired"
  - "Use send_commit_head_signal helper — never send directly on commit_head_tx from process_commit_head"

requirements-completed: [FLOW-02, FLOW-04]

# Metrics
duration: 18min
completed: 2026-03-25
---

# Phase 02 Plan 02: CommitHead Signal Wiring Summary

**CommitHeadSignal sender field added to CoreEngine with send_commit_head_signal helper called on all 3 process_commit_head return paths, using flume unbounded channel with non-fatal SendError handling**

## Performance

- **Duration:** 18 min
- **Started:** 2026-03-25T02:58:24Z
- **Completed:** 2026-03-25T03:16:27Z
- **Tasks:** 1
- **Files modified:** 4

## Accomplishments
- Added `commit_head_tx: Option<flume::Sender<CommitHeadSignal>>` to both `CoreEngineConfig` and `CoreEngine<DB>` struct
- Added `send_commit_head_signal(&self, block_number: U256)` private helper with U256->u64 saturating conversion
- Wired signal send on all 3 return paths: `!is_valid_state` early return, `NothingToCommit` error return, and normal `Ok(())` at end
- Added `commit_head_tx: None` placeholder in `main.rs` `CoreEngineConfig` construction for Plan 04 to replace
- Fixed all test CoreEngineConfig literals (tests.rs x2, test_drivers.rs x1) and CoreEngine::new_test() x1

## Task Commits

Each task was committed atomically:

1. **Task 1: Add commit_head_tx field, send_commit_head_signal helper, and signal calls on all process_commit_head return paths** - `f3792b7` (feat)

**Plan metadata:** (docs commit pending)

## Files Created/Modified
- `crates/sidecar/src/engine/mod.rs` - Added CommitHeadSignal import, commit_head_tx field to CoreEngineConfig and CoreEngine, send_commit_head_signal helper, 3 signal calls in process_commit_head
- `crates/sidecar/src/main.rs` - Added commit_head_tx: None placeholder to CoreEngineConfig construction
- `crates/sidecar/src/engine/tests.rs` - Added commit_head_tx: None to 2x CoreEngineConfig literals and CoreEngine::new_test() struct
- `crates/sidecar/src/utils/test_drivers.rs` - Added commit_head_tx: None to CoreEngineConfig literal

## Decisions Made
- Signal sent on `!is_valid_state` path because even when cache is invalidated the state worker needs to know the current block number to drain its buffer
- Signal sent before `return Err(EngineError::NothingToCommit)` so buffer never grows during empty-commit events
- Used `let _ = tx.send(signal)` to explicitly ignore SendError — state worker restart is expected and EthRpcSource covers the gap
- `saturating_to::<u64>()` chosen over `as u64` cast per CLAUDE.md/Clippy no-indexing policy

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed missing commit_head_tx field in CoreEngine::new_test() and 3 CoreEngineConfig literals**
- **Found during:** Task 1 (verification run `cargo test -p sidecar --lib -- engine`)
- **Issue:** Adding a new field to a struct without a Default impl causes compile errors in all test struct literals
- **Fix:** Added `commit_head_tx: None` to CoreEngine::new_test() Self block and to 3 CoreEngineConfig construction sites (tests.rs:252, tests.rs:1941, test_drivers.rs:276)
- **Files modified:** crates/sidecar/src/engine/tests.rs, crates/sidecar/src/utils/test_drivers.rs
- **Verification:** `cargo build -p sidecar --lib` passes cleanly
- **Committed in:** f3792b7 (part of task commit)

---

**Total deviations:** 1 auto-fixed (Rule 1 bug — test struct literals missing new required field)
**Impact on plan:** Required for compilation. No scope creep — strictly necessary to update test fixtures for the new field.

## Issues Encountered
- Engine tests all fail due to pre-existing missing testdata artifact (`testdata/mock-protocol/out/SimpleCounterAssertion.sol/Counter.json`). This is an environment issue unrelated to this plan's changes — `cargo build -p sidecar --lib` compiles cleanly.

## Next Phase Readiness
- Plan 03 can now create the `flume::unbounded()` channel in `StateWorkerThread` and pass the receiver to the buffer drain loop
- Plan 04 will replace `commit_head_tx: None` in main.rs with the real sender created alongside the state worker thread
- `send_commit_head_signal` is already in place — no further changes to engine/mod.rs needed for signal delivery

## Self-Check: PASSED

- FOUND: crates/sidecar/src/engine/mod.rs
- FOUND: crates/sidecar/src/main.rs
- FOUND commit: f3792b7

---
*Phase: 02-commithead-flow-control*
*Completed: 2026-03-25*
