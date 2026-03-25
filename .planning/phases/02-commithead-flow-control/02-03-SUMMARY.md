---
phase: 02-commithead-flow-control
plan: 03
subsystem: sidecar/state-worker
tags: [rust, mdbx, state-worker, sidecar, buffer, flush, backpressure, metrics, channel]

# Dependency graph
requires:
  - phase: 02-commithead-flow-control
    plan: 01
    provides: process_block returns BlockStateUpdate, CommitHeadSignal type, StateWorkerError variants
  - phase: 02-commithead-flow-control
    plan: 02
    provides: CommitHeadSignal sender in CoreEngine

provides:
  - run_blocking_inner with full VecDeque buffer + CommitHead flush loop
  - flush_ready_blocks function — only site calling commit_block in state_worker_thread
  - BUFFER_CAPACITY = 128 constant
  - state_worker_restarts_total counter (OBSV-02)
  - state_worker_buffer_depth gauge (OBSV-01)
  - state_worker_buffer_full_pauses_total counter (OBSV-03)
  - Arc<AtomicU64> committed_head with Release ordering in flush
  - StateWriter::Clone derive (shared DB handle)
  - state-worker as library crate (enables sidecar to embed StateWorker)

affects:
  - 02-04-PLAN (StateWorkerThread constructor now takes 4 args; Plan 04 wires real channel)
  - Phase 3 MdbxSource (reads committed_head with Acquire ordering)

# Tech tracking
tech-stack:
  added:
    - "state-worker [lib] target — enables crate to be used as library dependency"
  patterns:
    - "MDBX write gated by CommitHeadSignal — flush_ready_blocks is the ONLY flush site"
    - "VecDeque<BlockStateUpdate> bounded at 128 — backpressure when buffer full"
    - "Release ordering on committed_head.store() after each commit_block call"
    - "Best-effort shutdown flush: try_iter().last() drains final signal before exiting"
    - "Disconnected channel triggers clean exit (engine exited normally)"
    - "StateWriter::Clone via Arc<StateDb> — clone shares database handle safely"
    - "state-worker lib.rs re-exports genesis, metrics, state, system_calls, worker"

key-files:
  created:
    - crates/state-worker/src/lib.rs
  modified:
    - crates/sidecar/src/state_worker_thread/mod.rs
    - crates/sidecar/src/main.rs
    - crates/mdbx/src/writer.rs
    - crates/state-worker/src/worker.rs
    - crates/state-worker/Cargo.toml
    - crates/sidecar/Cargo.toml
    - Cargo.toml

key-decisions:
  - "state-worker gets [lib] target so sidecar can use StateWorker directly — avoid duplicating trace logic"
  - "StateWriter derives Clone (Arc<StateDb> is cheaply cloneable) — enables separate flush handle"
  - "compute_start_block made pub — sidecar resume-from-MDBX requires calling it directly (FLOW-06)"
  - "flush_ready_blocks is the ONLY commit_block call site — FLOW-04 invariant enforced architecturally"
  - "committed_head stores with Release ordering now — Phase 3 MdbxSource reads with Acquire"
  - "Plan 04 placeholder in main.rs: disconnected flume channel until real wiring in Plan 04"

patterns-established:
  - "Buffer+flush architecture: trace accumulates in VecDeque, MDBX writes gated by CommitHead"
  - "Backpressure: buffer.len() >= BUFFER_CAPACITY triggers recv_timeout wait instead of tracing"
  - "Shutdown flush: try_iter().last() grabs highest CommitHead signal; flush error ignored on shutdown"

requirements-completed:
  - FLOW-01
  - FLOW-03
  - FLOW-04
  - FLOW-05
  - FLOW-06
  - OBSV-01
  - OBSV-02
  - OBSV-03

# Metrics
duration: 21min
completed: 2026-03-25
---

# Phase 02 Plan 03: Buffer+Flush Architecture Summary

**Full buffer+flush loop replacing Phase 1 no-op inner loop: VecDeque<BlockStateUpdate> bounded at 128, CommitHeadSignal-gated MDBX flush, backpressure when full, Release-ordered committed_head, three observability metrics**

## Performance

- **Duration:** ~21 min
- **Started:** 2026-03-25T03:28:47Z
- **Completed:** 2026-03-25T03:49:37Z
- **Tasks:** 2
- **Files modified:** 7 (+ 1 created)

## Accomplishments

- Replaced the Phase 1 no-op `run_blocking_inner` (shutdown-poll loop) with the full buffer+flush architecture
- `StateWorkerThread` struct extended with `commit_head_rx`, `config`, `committed_head` fields; `new()` updated to 4-arg constructor
- `flush_ready_blocks` function added — the ONLY site that calls `commit_block` on the MDBX writer (FLOW-04 invariant)
- `BUFFER_CAPACITY = 128` constant defines the bounded buffer depth
- `VecDeque<BlockStateUpdate>` buffer accumulates traced blocks without writing to MDBX
- Backpressure branch pauses tracing when `buffer.len() >= 128`; waits on `recv_timeout(100ms)` for CommitHead signal
- Best-effort shutdown flush using `try_iter().last()` to grab the highest pending signal
- Clean exit when `CommitHeadSignal` sender disconnects (engine exited normally)
- `committed_head.store(update.block_number, Ordering::Release)` after each successful `commit_block` call
- Three metrics wired: `state_worker_buffer_depth` gauge, `state_worker_restarts_total` counter, `state_worker_buffer_full_pauses_total` counter
- `build_worker` async helper constructs `StateWorker<StateWriter>` from `EmbeddedStateWorkerConfig`
- `compute_start_block(None)` called on each `run_blocking_inner` invocation for FLOW-06 resume

## Task Commits

1. **Prerequisites: state-worker lib target, StateWriter Clone, compute_start_block pub** — `8420a23`
2. **Task 1+2: extend StateWorkerThread and implement run_blocking_inner** — `a22c254`
3. **Rule 3 fix: update main.rs for new 4-arg StateWorkerThread::new()** — `03d79d5`

## Files Created/Modified

- `crates/state-worker/src/lib.rs` — new library target re-exporting genesis, metrics, state, system_calls, worker
- `crates/sidecar/src/state_worker_thread/mod.rs` — full buffer+flush implementation (Tasks 1 and 2)
- `crates/sidecar/src/main.rs` — updated StateWorkerThread::new() call with placeholder channel and committed_head
- `crates/mdbx/src/writer.rs` — `#[derive(Clone)]` added to StateWriter
- `crates/state-worker/src/worker.rs` — `compute_start_block` made `pub`
- `crates/state-worker/Cargo.toml` — `[lib]` and `[[bin]]` sections added
- `crates/sidecar/Cargo.toml` — `state-worker.workspace = true` dependency added
- `Cargo.toml` — `state-worker` path added to `[workspace.dependencies]`

## Decisions Made

- **state-worker as library crate**: Adding `[lib]` target is the minimal structural change to enable sidecar embedding. The research note saying "no Cargo.toml changes required" was incorrect — state-worker had no lib target. Adding one is minimal and non-breaking for the existing binary.
- **StateWriter::Clone**: `StateWriter` wraps `StateReader` which contains `StateDb` which is `Arc<DatabaseEnv>`. The Arc makes clone O(1) and safe — both the worker's internal copy and the flush loop share the same database handle.
- **compute_start_block pub**: The embedded run_blocking_inner must call it to resume from MDBX on restart (FLOW-06). Making it pub is minimal; the function has no unsafe behavior.
- **flush_ready_blocks only flush site**: Enforced architecturally — `commit_block` only appears inside `flush_ready_blocks`, never in the trace loop. This is the FLOW-04 invariant.
- **Plan 04 placeholder in main.rs**: Created a disconnected flume channel so the binary compiles. When the sender is dropped immediately, the state worker will exit on `Disconnected` error, which is handled gracefully. Plan 04 wires the real channel.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] state-worker has no [lib] target**
- **Found during:** Task 2 (attempting to import state_worker::worker::StateWorker)
- **Issue:** state-worker is a binary-only crate; sidecar cannot depend on it as a library
- **Fix:** Added `[lib]` target with `lib.rs` re-exporting required modules; added `state-worker` to workspace deps and sidecar Cargo.toml
- **Files modified:** `crates/state-worker/Cargo.toml`, `crates/state-worker/src/lib.rs`, `Cargo.toml`, `crates/sidecar/Cargo.toml`
- **Commit:** `8420a23`

**2. [Rule 3 - Blocking] StateWriter has no Clone impl**
- **Found during:** Task 2 (build_worker needs to return both worker and writer separately)
- **Issue:** Plan's design requires two references to the writer (one inside StateWorker for reading, one for flush_ready_blocks for writing). StateWriter didn't implement Clone.
- **Fix:** Added `#[derive(Clone)]` to StateWriter — safe because StateDb is Arc-backed
- **Files modified:** `crates/mdbx/src/writer.rs`
- **Commit:** `8420a23`

**3. [Rule 3 - Blocking] compute_start_block is private**
- **Found during:** Task 2 (run_blocking_inner calls worker.compute_start_block(None))
- **Issue:** Function is `fn compute_start_block` (private) in worker.rs; sidecar cannot call it
- **Fix:** Changed to `pub fn compute_start_block`
- **Files modified:** `crates/state-worker/src/worker.rs`
- **Commit:** `8420a23`

**4. [Rule 3 - Blocking] main.rs StateWorkerThread::new() call signature mismatch**
- **Found during:** After Task 1 (binary failed to compile)
- **Issue:** main.rs had the old 1-arg `StateWorkerThread::new(shutdown)` call; new signature requires 4 args
- **Fix:** Added placeholder disconnected flume channel and Arc<AtomicU64> committed_head; Plan 04 will replace with real channel
- **Files modified:** `crates/sidecar/src/main.rs`
- **Commit:** `03d79d5`

**5. [Rule 2 - Auto-fix] Missing Provider trait import in mod.rs**
- **Found during:** First build attempt
- **Issue:** `.root()` method requires `Provider` trait in scope; `commit_block` requires `Writer` trait in scope
- **Fix:** Added `use alloy_provider::Provider;` and `use mdbx::Writer;` to imports
- **Files modified:** `crates/sidecar/src/state_worker_thread/mod.rs`
- **Committed in:** `a22c254`

## Known Stubs

None — all functionality is implemented. The Plan 04 placeholder in main.rs (`_commit_head_tx_placeholder`) is intentional and documented. The state worker will exit cleanly via `Disconnected` path until Plan 04 wires the real channel.

---
*Phase: 02-commithead-flow-control*
*Completed: 2026-03-25*

## Self-Check: PASSED

- FOUND: `crates/state-worker/src/lib.rs`
- FOUND: `crates/sidecar/src/state_worker_thread/mod.rs`
- FOUND: `.planning/phases/02-commithead-flow-control/02-03-SUMMARY.md`
- FOUND: `8420a23` (prerequisites commit)
- FOUND: `a22c254` (Task 1+2 implementation commit)
- FOUND: `03d79d5` (main.rs fix commit)
