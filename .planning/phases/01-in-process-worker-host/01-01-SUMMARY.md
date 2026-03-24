---
phase: 01-in-process-worker-host
plan: 01
subsystem: infra
tags: [rust, tokio, state-worker, mdbx, sidecar]
requires: []
provides:
  - reusable `state-worker` library host/bootstrap APIs
  - current-thread embedded worker runtime entrypoint
  - compatibility `state-worker` binary shim over the shared host API
affects: [sidecar, state-worker, mdbx, phase-01-02]
tech-stack:
  added: []
  patterns: [thin binary delegates to library host, embedded worker owns isolated current-thread Tokio runtime]
key-files:
  created:
    - crates/state-worker/src/lib.rs
    - crates/state-worker/src/host.rs
  modified:
    - crates/state-worker/src/main.rs
    - crates/state-worker/src/integration_tests/setup.rs
    - crates/state-worker/src/genesis.rs
    - crates/state-worker/src/geth_version.rs
    - crates/state-worker/src/state/geth.rs
    - crates/state-worker/src/state/geth_provider.rs
    - crates/state-worker/src/state/mod.rs
    - crates/state-worker/src/system_calls.rs
    - crates/state-worker/src/worker.rs
key-decisions:
  - "State-worker bootstrap now lives in `host.rs`, with an externally callable `run_embedded_worker` plus a current-thread runtime wrapper for isolated embedding."
  - "The standalone binary remains only as a compatibility shim and no longer owns restart or shutdown supervision behavior."
patterns-established:
  - "Embeddable service extraction: reusable bootstrap stays in `lib.rs`/`host.rs`, while `main.rs` only parses CLI args and delegates."
  - "MDBX resume semantics remain crate-level behavior through `StateWorker::compute_start_block`, not CLI-only wiring."
requirements-completed: [THREAD-02, THREAD-05]
duration: 8min
completed: 2026-03-24
---

# Phase 1 Plan 1: In-Process Worker Host Summary

**Embeddable state-worker host APIs with an isolated current-thread runtime and MDBX resume semantics preserved in shared library code**

## Performance

- **Duration:** 8 min
- **Started:** 2026-03-24T19:53:00Z
- **Completed:** 2026-03-24T20:01:03Z
- **Tasks:** 2
- **Files modified:** 10

## Accomplishments
- Added `crates/state-worker/src/lib.rs` and `crates/state-worker/src/host.rs` so embedders can call shared worker bootstrap APIs directly.
- Moved provider setup, geth version validation, genesis loading, shutdown wiring, and worker construction into reusable host code.
- Reduced `crates/state-worker/src/main.rs` to a compatibility shim that delegates into `state_worker::host`.

## Task Commits

Each task was committed atomically:

1. **Task 1: Create embeddable worker host surfaces** - `366c4699` (feat)
2. **Task 2: Rewire the standalone binary to call the shared host API** - `79d05f04` (refactor)
3. **Rule 3 follow-up: satisfy state-worker library lint contract** - `1237f45d` (fix)

## Files Created/Modified
- `crates/state-worker/src/lib.rs` - Exposes the public module surface for embedding.
- `crates/state-worker/src/host.rs` - Owns reusable worker bootstrap, signal handling, provider setup, and isolated runtime creation.
- `crates/state-worker/src/main.rs` - Thin CLI compatibility shim over the shared host API.
- `crates/state-worker/src/integration_tests/setup.rs` - Uses the new host-level provider connector.
- `crates/state-worker/src/genesis.rs` - Added API annotations/docs needed once the crate exposed a library target.
- `crates/state-worker/src/geth_version.rs` - Added API annotations for exported version helpers.
- `crates/state-worker/src/state/geth.rs` - Documented the exported trace conversion helper.
- `crates/state-worker/src/state/geth_provider.rs` - Added API annotations for the exported geth trace provider constructor.
- `crates/state-worker/src/state/mod.rs` - Documented exported builder helpers and trace-provider factory.
- `crates/state-worker/src/system_calls.rs` - Added API annotations/docs for exported system-call helpers.
- `crates/state-worker/src/worker.rs` - Documented the exported worker run loop.

## Decisions Made
- Kept two shared entrypoints in `host.rs`: async `run_embedded_worker` for embedders with their own shutdown channel, and `run_embedded_worker_until_shutdown` for compatibility execution on a single-threaded runtime.
- Preserved restart-from-MDBX behavior in `StateWorker::compute_start_block` so later sidecar integration inherits the same resume semantics automatically.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Library-target Clippy failures after extracting `state-worker` into a reusable crate**
- **Found during:** Plan-level verification
- **Issue:** Exposing `state-worker` as a library caused existing exported APIs to fail workspace `clippy::pedantic` checks for missing `#[must_use]` annotations and `# Errors` docs.
- **Fix:** Added the required annotations and error docs across the exported state-worker modules.
- **Files modified:** `crates/state-worker/src/genesis.rs`, `crates/state-worker/src/geth_version.rs`, `crates/state-worker/src/host.rs`, `crates/state-worker/src/state/geth.rs`, `crates/state-worker/src/state/geth_provider.rs`, `crates/state-worker/src/state/mod.rs`, `crates/state-worker/src/system_calls.rs`, `crates/state-worker/src/worker.rs`
- **Verification:** `make lint`
- **Committed in:** `1237f45d`

**2. [Rule 3 - Blocking] `cargo nextest` unavailable in the workspace shell**
- **Found during:** Task 1 verification
- **Issue:** The plan requested `cargo nextest`, but the command was not installed in the execution environment.
- **Fix:** Ran the equivalent targeted `cargo test -p state-worker --locked --no-default-features test_mdbx_bootstrap_recovery_without_diffs -- --test-threads=2` verification instead.
- **Files modified:** None
- **Verification:** targeted `cargo test` pass for `test_mdbx_bootstrap_recovery_without_diffs`
- **Committed in:** Not applicable

---

**Total deviations:** 2 auto-fixed (2 blocking)
**Impact on plan:** Both deviations were required to complete verification cleanly. No architectural scope change.

## Issues Encountered
- The extracted library surface pulled existing state-worker exports under workspace Clippy rules; fixing those annotations was necessary for the new crate shape to remain valid.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Sidecar can now depend on a shared `state-worker` host API instead of reproducing bootstrap logic.
- Phase `01-02` can focus on sidecar-owned supervision, thread lifecycle, and shutdown integration on top of the extracted host surface.

## Self-Check: PASSED

---
*Phase: 01-in-process-worker-host*
*Completed: 2026-03-24*
