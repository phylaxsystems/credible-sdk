---
phase: 02-commithead-flow-control
plan: 01
subsystem: state-worker
tags: [rust, mdbx, state-worker, sidecar, channel, config]

# Dependency graph
requires:
  - phase: 01-thread-scaffold
    provides: StateWorkerThread scaffold, StateWorkerError, ErrorRecoverability pattern
provides:
  - process_block returns BlockStateUpdate (not Result<()>) — buffer in Plan 03 can accumulate
  - CommitHeadSignal type — Plan 02 and Plan 03 can import it
  - EmbeddedStateWorkerConfig — Plan 04 can read ws_url, mdbx_path, genesis_path, state_depth
  - StateWorkerError variants for MDBX write, trace, config, and start block failures
affects:
  - 02-02-PLAN (CommitHeadSignal channel wiring into CoreEngine)
  - 02-03-PLAN (buffer + flush loop needs BlockStateUpdate from process_block)
  - 02-04-PLAN (StateWorkerThread constructor needs EmbeddedStateWorkerConfig)

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "process_block returns Result<BlockStateUpdate> — caller is responsible for commit"
    - "commit_update() helper in run() encapsulates commit_block + metrics + error handling"
    - "apply_system_calls short-circuits when cancun_time and prague_time are both None"
    - "Sentinel BlockStateUpdate (B256::ZERO hash, empty accounts) skips commit for genesis skip path"
    - "AtomicBool tracking writer for unit testing commit behavior without MDBX"

key-files:
  created: []
  modified:
    - crates/state-worker/src/worker.rs
    - crates/sidecar/src/state_worker_thread/mod.rs
    - crates/sidecar/src/state_worker_thread/error.rs
    - crates/sidecar/src/args/mod.rs

key-decisions:
  - "process_block returns BlockStateUpdate and does NOT commit — run() commits via commit_update()"
  - "commit_update() skips sentinel (block_number==0, empty accounts, B256::ZERO hash) to preserve genesis skip-if-exists semantics"
  - "apply_system_calls short-circuits when no fork times configured — avoids provider call in unit tests"
  - "CommitHeadSignal is a standalone type (not reusing engine::queue::CommitHead which has pub(crate) fields)"
  - "EmbeddedStateWorkerConfig uses all-Option fields — validation deferred to Plan 03/04 when consumed"
  - "All new StateWorkerError variants are Recoverable — EthRpcSource covers downtime while thread restarts"

patterns-established:
  - "Refactored process_block pattern: trace block → return update → caller commits (separation of concerns for buffer)"
  - "Unit testing with NoopWriterReader + MockTraceProvider avoids MDBX/network dependencies"

requirements-completed:
  - FLOW-01
  - FLOW-06

# Metrics
duration: 30min
completed: 2026-03-25
---

# Phase 02 Plan 01: Contractual Foundation Summary

**process_block refactored to return BlockStateUpdate (not commit), CommitHeadSignal channel type defined, EmbeddedStateWorkerConfig added to sidecar, StateWorkerError extended with four new variants**

## Performance

- **Duration:** ~30 min
- **Started:** 2026-03-25T02:00:00Z
- **Completed:** 2026-03-25T02:30:00Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments

- Refactored `StateWorker::process_block` to return `Result<BlockStateUpdate>` instead of `Result<()>` — enables the Plan 03 buffer to accumulate updates without immediately writing to MDBX
- `run()` now calls `commit_update()` (new helper) after each `process_block` call — standalone binary behavior preserved
- Defined `CommitHeadSignal { pub block_number: u64 }` in `state_worker_thread/mod.rs` — Plan 02 and Plan 03 can import it
- Added `EmbeddedStateWorkerConfig` to `args/mod.rs` and wired into `Config` and `ConfigFile` — Plan 04 can read state worker config from sidecar JSON
- Extended `StateWorkerError` with `ComputeStartBlock`, `MdbxWrite`, `Trace`, `Config` variants — all `Recoverable`
- Added two unit tests in `worker.rs` verifying the refactored signature and no-commit behavior

## Task Commits

Each task was committed atomically:

1. **Task 1: Refactor process_block to return BlockStateUpdate + adapt standalone binary** - `89ea193` (feat)
2. **Task 2: Define CommitHeadSignal, extend StateWorkerError, add StateWorkerConfig to sidecar** - `18bc325` (feat)

## Files Created/Modified

- `crates/state-worker/src/worker.rs` — process_block returns BlockStateUpdate; commit_update helper; apply_system_calls short-circuit; genesis sentinel; two unit tests
- `crates/sidecar/src/state_worker_thread/mod.rs` — CommitHeadSignal type added
- `crates/sidecar/src/state_worker_thread/error.rs` — Four new error variants: ComputeStartBlock, MdbxWrite, Trace, Config
- `crates/sidecar/src/args/mod.rs` — EmbeddedStateWorkerConfig struct; ConfigFile.state_worker field; Config.state_worker field; resolve_config passthrough

## Decisions Made

- **process_block returns BlockStateUpdate, not Result<()>**: Enables Plan 03 buffer to hold updates without committing. The caller (run() in standalone mode, flush loop in embedded mode) is responsible for committing.
- **commit_update() sentinel check for genesis**: When genesis is already committed or has no accounts, `process_genesis_block` returns a `BlockStateUpdate::new(0, B256::ZERO, B256::ZERO)`. `commit_update` detects this sentinel (block 0, empty accounts, zero hash) and skips the MDBX write to preserve the original skip-if-exists semantics.
- **apply_system_calls short-circuit**: Added early return when both `cancun_time` and `prague_time` are None. This avoids a provider HTTP call in unit tests and is a valid optimization for networks with no system calls configured.
- **CommitHeadSignal is separate from engine::queue::CommitHead**: `CommitHead` has `pub(crate)` fields; a separate type for the signal avoids coupling the state worker to the engine's internal types.
- **All-Option EmbeddedStateWorkerConfig fields**: The embedded state worker is optional in Phase 2. Fields are validated when consumed in Plan 03/04; no premature validation added here.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Added apply_system_calls short-circuit for no-fork-configured case**
- **Found during:** Task 1 (unit test execution)
- **Issue:** `apply_system_calls` unconditionally called `provider.get_block_by_number()` even when no system calls could be active (both fork times were None). This caused unit tests with mock providers (localhost:0) to fail.
- **Fix:** Added early return at the top of `apply_system_calls` if both `cancun_time` and `prague_time` are None.
- **Files modified:** `crates/state-worker/src/worker.rs`
- **Verification:** All 74 tests pass; short-circuit does not affect any configured-fork code paths.
- **Committed in:** `89ea193` (Task 1 commit)

**2. [Rule 1 - Bug] Genesis sentinel pattern for process_genesis_block**
- **Found during:** Task 1 (analyzing process_genesis_block refactoring)
- **Issue:** Original `process_genesis_block` returned `Result<()>` with early returns when genesis already exists or accounts are empty. After refactoring to return `Result<BlockStateUpdate>`, the early returns needed a sentinel value. Without a skip mechanism, `commit_update` would try to commit an empty update for block 0, potentially overwriting existing genesis data.
- **Fix:** `process_genesis_block` returns `BlockStateUpdate::new(0, B256::ZERO, B256::ZERO)` for the skip paths. `commit_update` detects this sentinel (block_number==0 AND accounts.is_empty() AND block_hash==B256::ZERO) and returns Ok(()) without calling commit_block.
- **Files modified:** `crates/state-worker/src/worker.rs`
- **Verification:** `test_state_worker_hydrates_genesis_state` passes; sentinel logic is minimal and targeted.
- **Committed in:** `89ea193` (Task 1 commit)

---

**Total deviations:** 2 auto-fixed (Rule 1 - Bug)
**Impact on plan:** Both fixes required for correctness. No scope creep.

## Issues Encountered

- `cargo test -p state-worker --lib` failed with "no library targets found" — state-worker is a binary-only crate. Used `cargo test -p state-worker` instead (tests are in `#[cfg(test)]` modules in the binary).
- `process_genesis_block` return type change required careful handling of the skip-if-exists semantic. The sentinel pattern avoids restructuring the function while preserving correctness.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `process_block` returns `BlockStateUpdate` — Plan 03 can push it to the 128-block buffer without committing
- `CommitHeadSignal` type is defined and exported — Plan 02 can import it for channel wiring
- `EmbeddedStateWorkerConfig` in `Config` — Plan 04 can read `ws_url`, `mdbx_path`, `genesis_path`, `state_depth`
- `StateWorkerError` variants cover all failure modes Plan 03 needs for its buffer/flush error handling
- Both crates compile cleanly

---
*Phase: 02-commithead-flow-control*
*Completed: 2026-03-25*
