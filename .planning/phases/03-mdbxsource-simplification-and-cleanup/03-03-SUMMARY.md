---
phase: 03-mdbxsource-simplification-and-cleanup
plan: "03"
subsystem: infra
tags: [rust, cargo, state-worker, binary, workspace]

# Dependency graph
requires:
  - phase: 02-commithead-flow-control
    provides: state-worker lib target (src/lib.rs) added so sidecar can embed StateWorker directly
provides:
  - state-worker crate reduced to library-only target (no standalone binary)
affects: [ci, helm-charts, docker, workspace-build]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Binary entry point deleted; lib target retained — state-worker is now a pure library"

key-files:
  created: []
  modified:
    - crates/state-worker/Cargo.toml

key-decisions:
  - "Removed clap, rust-tracing, and rustls from [dependencies] — all three were used exclusively by main.rs/cli.rs and are not needed by any lib module"
  - "Retained futures-util, async-trait, tokio and all other deps used by worker.rs, state/, genesis.rs, system_calls.rs"
  - "Rebased worktree onto gsd/phase-03-mdbxsource-simplification-and-cleanup before executing — worktree was at main which lacked the [lib] target and lib.rs added in phase 02-03"

patterns-established:
  - "When deleting a binary from a Rust crate that also has a lib target, remove only the [[bin]] section and main.rs; keep all lib-visible modules and their dependencies"

requirements-completed: [SIMP-05]

# Metrics
duration: 15min
completed: 2026-03-25
---

# Phase 03 Plan 03: Delete state-worker standalone binary Summary

**Standalone state-worker binary deleted; lib crate preserved with all worker modules intact; workspace builds cleanly with sidecar still linking against the state_worker lib**

## Performance

- **Duration:** ~15 min
- **Started:** 2026-03-25T00:00:00Z
- **Completed:** 2026-03-25T00:15:00Z
- **Tasks:** 1
- **Files modified:** 2 (Cargo.toml modified, main.rs deleted; Cargo.lock updated)

## Accomplishments

- Removed `[[bin]]` section from `crates/state-worker/Cargo.toml`
- Deleted `crates/state-worker/src/main.rs`
- Removed binary-only dependencies: `clap`, `rust-tracing`, `rustls`
- `cargo build --workspace` succeeds with zero errors
- `cargo build -p sidecar` succeeds — lib target still compiles and links

## Task Commits

Each task was committed atomically:

1. **Task 1: Delete the standalone state-worker binary** - `8a52584` (feat)

**Plan metadata:** (docs commit follows)

## Files Created/Modified

- `crates/state-worker/Cargo.toml` - Removed [[bin]] section; removed clap, rust-tracing, rustls from [dependencies]
- `crates/state-worker/src/main.rs` - Deleted (was binary entry point)
- `Cargo.lock` - Updated after dependency removal

## Decisions Made

- Removed `clap`, `rust-tracing`, `rustls` from dependencies — these were exclusively used by `main.rs` and `cli.rs`, which are only reachable from the binary target. All lib modules (worker.rs, state/, genesis.rs, system_calls.rs, metrics.rs) were verified to not import these.
- Retained `futures-util` — used by `worker.rs:29 use futures_util::StreamExt`
- Retained `async-trait` — used by `worker.rs`, `state/geth_provider.rs`, `state/mod.rs`
- Rebased worktree onto `gsd/phase-03-mdbxsource-simplification-and-cleanup` before executing — the worktree was created from main which predates the `[lib]` target and `lib.rs` added in phase 02 plan 03.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Rebased worktree onto phase-03 branch**
- **Found during:** Task 1 (initial file inspection)
- **Issue:** Worktree was checked out at `93c6e0d` (main), which predates all GSD phase work. The Cargo.toml lacked both `[lib]` and `[[bin]]` sections; `src/lib.rs` did not exist. Executing the plan on this base would have been invalid.
- **Fix:** Ran `git rebase gsd/phase-03-mdbxsource-simplification-and-cleanup` to bring the worktree up to the correct base (`662d475`). No conflicts.
- **Files modified:** None (rebase brought in prior phase changes)
- **Verification:** `crates/state-worker/Cargo.toml` shows both `[lib]` and `[[bin]]` sections after rebase
- **Committed in:** Pre-existing commits from prior phases (not a new commit)

---

**Total deviations:** 1 auto-fixed (blocking — wrong base branch)
**Impact on plan:** Rebase was necessary before any plan work could proceed. No scope creep.

## Issues Encountered

None beyond the worktree base branch issue documented above.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Phase 03 is now complete (all 3 plans done: MdbxSource simplification, StateWriter depth hardcoding, binary deletion)
- The state-worker crate is now a pure library; no standalone binary exists
- CI artifact surface reduced — no `state-worker` binary to build/publish/deploy
- Helm charts for the standalone state-worker process can be retired in a follow-up cleanup

---
*Phase: 03-mdbxsource-simplification-and-cleanup*
*Completed: 2026-03-25*
