# Roadmap: Sidecar In-Process State Worker Integration

## Overview

This roadmap takes the existing sidecar/state-worker split and incrementally collapses it into a single supervised sidecar process. The work starts by hosting the worker safely inside sidecar, then introduces commit-gated MDBX persistence, then simplifies `MdbxSource` around in-process height tracking, and finishes with fallback, latency, and regression hardening so the new architecture can replace the current two-process coordination path.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [ ] **Phase 1: In-Process Worker Host** - Embed the state worker into sidecar with supervised thread lifecycle and safe restart behavior
- [ ] **Phase 2: Commit-Gated MDBX Flush** - Buffer traced updates and flush MDBX only when commit head permits them
- [ ] **Phase 3: Simplify MDBX Source** - Replace polling/range logic with in-process height tracking and single-replica reads
- [ ] **Phase 4: Fallback and Regression Hardening** - Validate fallback behavior, latency, and operational safety of the integrated design

## Phase Details

### Phase 1: In-Process Worker Host
**Goal**: Run the state worker as a supervised OS thread inside sidecar with its own single-threaded runtime, panic containment, restart backoff, and restart-from-MDBX semantics.
**Depends on**: Nothing (first phase)
**Requirements**: [THREAD-01, THREAD-02, THREAD-03, THREAD-04, THREAD-05]
**Success Criteria** (what must be TRUE):
  1. Starting sidecar also starts the state worker inside the same process without requiring a separate `state-worker` deployment.
  2. If the worker panics or returns an error, sidecar stays alive and the worker is restarted automatically with backoff.
  3. After a worker restart, block ingestion resumes from the last committed MDBX block instead of replaying from scratch or skipping ahead.
**Plans**: 3 plans

Plans:
- [x] 01-01-PLAN.md — Move worker startup/runtime bootstrap into embeddable library surfaces
- [x] 01-02-PLAN.md — Add sidecar-owned worker host, lifecycle controls, and shutdown wiring
- [x] 01-03-PLAN.md — Add restart supervision, panic isolation, and resume-from-MDBX coverage

### Phase 2: Commit-Gated MDBX Flush
**Goal**: Decouple tracing from persistence by buffering traced updates in memory and allowing MDBX writes only up to the engine's committed head.
**Depends on**: Phase 1
**Requirements**: [SYNC-01, SYNC-02, SYNC-03, SYNC-04, SYNC-05]
**Success Criteria** (what must be TRUE):
  1. Traced block updates can accumulate in an in-memory buffer before being written to MDBX.
  2. The worker writes only blocks at or below the commit head the engine has explicitly permitted.
  3. When the update buffer is full, the worker applies backpressure instead of letting MDBX or memory growth run away.
**Plans**: 3 plans

Plans:
- [x] 02-01-PLAN.md — Define commit-head control contracts and split tracing from gated flushing
- [ ] 02-02-PLAN.md — Enforce bounded commit-gated draining, backpressure, and backlog telemetry
- [ ] 02-03-PLAN.md — Add integration coverage for watermark delivery, max-height invariants, and full-buffer pause behavior

### Phase 3: Simplify MDBX Source
**Goal**: Rework `MdbxSource` around shared in-process MDBX height so sidecar reads no longer depend on polling, overlap math, or replica-depth workarounds.
**Depends on**: Phase 2
**Requirements**: [MDBX-01, MDBX-02, MDBX-03, MDBX-04]
**Success Criteria** (what must be TRUE):
  1. `MdbxSource` gets readable MDBX height from shared in-process state rather than a 50 ms polling task.
  2. Sidecar MDBX reads no longer rely on multiple state replicas or range intersection calculations.
  3. The old "went too far" and related depth-management logic is removed without breaking read correctness.
**Plans**: 2 plans

Plans:
- [ ] 03-01: Replace MDBX range polling with shared atomic height/state exposure
- [ ] 03-02: Remove replica-depth, overlap, and "went too far" logic from `MdbxSource`

### Phase 4: Fallback and Regression Hardening
**Goal**: Prove the integrated design keeps `EthRpcSource` fallback working and does not regress block processing latency or operational safety.
**Depends on**: Phase 3
**Requirements**: [FALL-01, FALL-02, VAL-01, VAL-02, VAL-03]
**Success Criteria** (what must be TRUE):
  1. When the worker is down or MDBX lags behind, the sidecar still serves reads through `EthRpcSource`.
  2. Regression coverage exercises worker restart, commit-gated flush behavior, and simplified MDBX source behavior end-to-end.
  3. Benchmarks or equivalent latency checks show no unacceptable block processing latency regression relative to the existing design.
**Plans**: 3 plans

Plans:
- [ ] 04-01: Wire and verify fallback behavior when integrated worker state is unavailable or behind
- [ ] 04-02: Add regression/integration coverage for failure handling and MDBX source behavior
- [ ] 04-03: Measure latency and clean up deployment/config assumptions made obsolete by the old split architecture

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2 → 3 → 4

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. In-Process Worker Host | 3/3 | Complete | 2026-03-24 |
| 2. Commit-Gated MDBX Flush | 0/3 | Not started | - |
| 3. Simplify MDBX Source | 0/2 | Not started | - |
| 4. Fallback and Regression Hardening | 0/3 | Not started | - |
