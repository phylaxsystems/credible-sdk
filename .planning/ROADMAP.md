# Roadmap: Credible SDK — State Worker Thread Integration

## Overview

This milestone replaces the two-process sidecar/state-worker architecture with a single sidecar process that owns the state worker as a dedicated OS thread. The work proceeds in three sequential phases that mirror the R1/R2/R3 dependency chain: first the thread scaffold must exist and be lifecycle-correct (Phase 1), then CommitHead flow control delivers the core invariant that MDBX writes never exceed commit_head (Phase 2), and finally MdbxSource polling and the now-obsolete standalone binary are removed (Phase 3). Each phase is independently verifiable and each depends on the prior.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Thread Scaffold** - Embed state worker as a named OS thread with isolated tokio runtime, panic isolation, restart backoff, and graceful shutdown (completed 2026-03-25)
- [ ] **Phase 2: CommitHead Flow Control** - Wire CommitHead mpsc channel from CoreEngine to state worker, add bounded 128-block buffer, and gate all MDBX writes behind commit_head
- [ ] **Phase 3: MdbxSource Simplification and Cleanup** - Replace 50ms polling with Arc<AtomicU64>, remove range-intersection logic, reduce circular buffer depth to 1, and delete standalone state-worker binary

## Phase Details

### Phase 1: Thread Scaffold
**Goal**: State worker runs as a lifecycle-correct OS thread inside the sidecar — it starts with the sidecar, catches its own panics, restarts with backoff, and shuts down cleanly when the sidecar stops
**Depends on**: Nothing (first phase)
**Requirements**: THRD-01, THRD-02, THRD-03, THRD-04, THRD-05, THRD-06, THRD-07
**Success Criteria** (what must be TRUE):
  1. Sidecar starts and the state worker thread appears in thread listings named `sidecar-state-worker`
  2. Killing the state worker thread (or triggering a panic inside it) does not crash the sidecar — the sidecar logs the failure and restarts the thread with backoff
  3. Sending SIGTERM to the sidecar causes the state worker to exit cleanly within the normal shutdown window — no hang on join
  4. EthRpcSource serves state without interruption when the state worker is down or restarting
  5. `StateWorkerError` variants are classified as Recoverable or Unrecoverable and drive the restart vs. abort decision
**Plans**: 2 plans

Plans:
- [x] 01-01-PLAN.md — StateWorkerError enum + StateWorkerThread scaffold (error types, spawn, isolated runtime, panic catch, shutdown polling, restart backoff)
- [x] 01-02-PLAN.md — Wire StateWorkerThread into sidecar main.rs (ThreadHandles, run_sidecar_once, run_async_components select!, EthRpcSource fallback preserved)

### Phase 2: CommitHead Flow Control
**Goal**: MDBX writes are gated by CommitHead signals from the engine — the state worker buffers traced blocks in memory and only flushes to MDBX when authorized, so MDBX height never exceeds commit_head.block_number
**Depends on**: Phase 1
**Requirements**: FLOW-01, FLOW-02, FLOW-03, FLOW-04, FLOW-05, FLOW-06, OBSV-01, OBSV-02, OBSV-03
**Success Criteria** (what must be TRUE):
  1. MDBX height never advances past the current commit_head.block_number — observable by querying MDBX height and commit_head under normal operation and during artificial lag
  2. When the in-memory buffer reaches 128 entries, the state worker stops tracing new blocks until the buffer drains — no unbounded memory growth under engine stall
  3. After a state worker restart, tracing resumes from the last committed block in MDBX — no gap, no duplicate writes
  4. `state_worker_buffer_depth`, `state_worker_buffer_full_pauses_total`, and `state_worker_restarts_total` metrics are present and update in real time
**Plans**: 4 plans

Plans:
- [x] 02-01-PLAN.md — Foundation: refactor process_block to return BlockStateUpdate, define CommitHeadSignal type, add EmbeddedStateWorkerConfig to sidecar args, extend StateWorkerError
- [x] 02-02-PLAN.md — CoreEngine CommitHead sender: add commit_head_tx field to CoreEngineConfig + CoreEngine, send signal on all process_commit_head return paths
- [x] 02-03-PLAN.md — State worker buffer+flush loop: replace no-op run_blocking_inner with full outer-sync-loop, flush_ready_blocks, backpressure at 128, best-effort shutdown flush, all 3 metrics
- [ ] 02-04-PLAN.md — Wire run_sidecar_once: create flume::unbounded channel, Arc<AtomicU64> committed_head, conditional state worker spawn with config

### Phase 3: MdbxSource Simplification and Cleanup
**Goal**: MdbxSource reads MDBX height from an in-process atomic instead of polling, all range-overlap logic is removed, the circular buffer depth is 1, and the standalone state-worker binary no longer exists in the workspace
**Depends on**: Phase 2
**Requirements**: SIMP-01, SIMP-02, SIMP-03, SIMP-04, SIMP-05
**Success Criteria** (what must be TRUE):
  1. `spawn_block_range_poller` and the 50ms polling timer are gone from the codebase — `MdbxSource::is_synced` reads `Arc<AtomicU64>` directly
  2. `calculate_target_block` and all call sites are deleted — grep finds no references
  3. `crates/state-worker/` directory is removed from the workspace and `cargo build` succeeds cleanly
  4. Sidecar processes blocks end-to-end with MDBX circular buffer depth 1 — no "went too far" panics or range errors under normal operation
**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2 → 3

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Thread Scaffold | 2/2 | Complete   | 2026-03-25 |
| 2. CommitHead Flow Control | 3/4 | In Progress|  |
| 3. MdbxSource Simplification and Cleanup | 0/? | Not started | - |
