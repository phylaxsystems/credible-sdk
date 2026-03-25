# Requirements: Credible SDK — State Worker Thread Integration

**Defined:** 2026-03-25
**Core Value:** MDBX writes never exceed the current commit head — the core engine controls exactly when state becomes visible.

## v1 Requirements

Requirements for this milestone. Each maps to roadmap phases.

### Thread Integration

- [x] **THRD-01**: State worker runs as OS thread via `std::thread::Builder` inside sidecar process (named `sidecar-state-worker`)
- [x] **THRD-02**: State worker has own single-threaded tokio runtime, isolated from sidecar main runtime
- [x] **THRD-03**: Panics in state worker thread are caught and do not crash the sidecar
- [x] **THRD-04**: State worker auto-restarts on failure with backoff matching existing EventSequencing/CoreEngine pattern
- [x] **THRD-05**: EthRpcSource remains as active fallback when state worker is down or behind
- [x] **THRD-06**: `StateWorkerError` enum with `From<&StateWorkerError> for ErrorRecoverability` classification
- [x] **THRD-07**: State worker checks shared `Arc<AtomicBool>` shutdown flag and exits cleanly on sidecar shutdown

### Flow Control

- [x] **FLOW-01**: State worker buffers `BlockStateUpdate` in a bounded `VecDeque` after tracing each block
- [x] **FLOW-02**: Core engine sends `CommitHeadSignal { block_number }` via mpsc channel after each `process_commit_head`
- [ ] **FLOW-03**: State worker flushes buffered updates where `block_number <= commit_head` to MDBX on signal
- [x] **FLOW-04**: MDBX height never exceeds `current_commit_head.block_number`
- [ ] **FLOW-05**: Buffer bounded at 128 blocks — state worker pauses tracing when buffer is full (backpressure)
- [x] **FLOW-06**: On restart, state worker resumes from last committed block in MDBX via `compute_start_block`

### Simplification

- [ ] **SIMP-01**: MdbxSource reads current MDBX height in-process via shared `Arc<AtomicU64>` with Release/Acquire ordering — no 50ms polling
- [ ] **SIMP-02**: Circular buffer depth reduced to 1 — multiple state replicas no longer required
- [ ] **SIMP-03**: `calculate_target_block` range intersection and state depth logic removed
- [ ] **SIMP-04**: MdbxSource "went too far" handling removed
- [ ] **SIMP-05**: Standalone `state-worker` binary crate removed from workspace (Cargo.toml, crates/state-worker/)

### Observability

- [ ] **OBSV-01**: Buffer utilization gauge (`state_worker_buffer_depth`) updated after each trace/flush
- [ ] **OBSV-02**: Restart counter (`state_worker_restarts_total`) incremented on each thread restart
- [ ] **OBSV-03**: Buffer-full pause counter (`state_worker_buffer_full_pauses_total`) incremented when tracing pauses due to full buffer

## v2 Requirements

Deferred to future milestones. Tracked but not in current roadmap.

### Reliability

- **RELI-01**: Cache invalidation recovery via state worker (dirty/clean DB marking)
- **RELI-02**: State diff cross-validation between engine and state worker

### Observability

- **OBSV-04**: CommitHead lag gauge (`state_worker_commit_head_lag_blocks`)
- **OBSV-05**: Health status gauge (`state_worker_thread_healthy`) exposed via sidecar health endpoint
- **OBSV-06**: Tracing duration histogram (`state_worker_trace_duration_seconds`)

## Out of Scope

Explicitly excluded. Documented to prevent scope creep.

| Feature | Reason |
|---------|--------|
| Removing EthRpcSource fallback | Intentionally kept as safety net for when state worker is down/behind |
| Feature-flagged parallel standalone mode | Clean cut migration — dual mode doubles ops complexity and risks concurrent MDBX writes |
| Changes to shadow-driver, pcl, assertion-da, or verifier-service | Unrelated binaries; out of scope for this milestone |
| State diff cross-validation | Future work — adds complexity without immediate correctness benefit |
| Unbounded buffer | Anti-feature — memory grows without limit if engine stalls |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| THRD-01 | Phase 1 | Complete |
| THRD-02 | Phase 1 | Complete |
| THRD-03 | Phase 1 | Complete |
| THRD-04 | Phase 1 | Complete |
| THRD-05 | Phase 1 | Complete |
| THRD-06 | Phase 1 | Complete |
| THRD-07 | Phase 1 | Complete |
| FLOW-01 | Phase 2 | Complete |
| FLOW-02 | Phase 2 | Complete |
| FLOW-03 | Phase 2 | Pending |
| FLOW-04 | Phase 2 | Complete |
| FLOW-05 | Phase 2 | Pending |
| FLOW-06 | Phase 2 | Complete |
| SIMP-01 | Phase 3 | Pending |
| SIMP-02 | Phase 3 | Pending |
| SIMP-03 | Phase 3 | Pending |
| SIMP-04 | Phase 3 | Pending |
| SIMP-05 | Phase 3 | Pending |
| OBSV-01 | Phase 2 | Pending |
| OBSV-02 | Phase 2 | Pending |
| OBSV-03 | Phase 2 | Pending |

**Coverage:**
- v1 requirements: 21 total
- Mapped to phases: 21
- Unmapped: 0

---
*Requirements defined: 2026-03-25*
*Last updated: 2026-03-25 after roadmap creation*
