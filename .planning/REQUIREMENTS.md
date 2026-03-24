# Requirements: Sidecar In-Process State Worker Integration

**Defined:** 2026-03-24
**Core Value:** The sidecar must consume fresh MDBX-backed state without cross-process coordination complexity or allowing MDBX to advance past the sidecar's committed head.

## v1 Requirements

### Thread Integration

- [x] **THREAD-01**: Sidecar starts the state worker as a dedicated OS thread inside the sidecar process
- [x] **THREAD-02**: The in-process state worker owns a single-threaded Tokio runtime isolated from the sidecar main runtime
- [x] **THREAD-03**: A panic or recoverable failure in the state worker does not crash the sidecar process
- [x] **THREAD-04**: The sidecar automatically restarts the state worker with backoff after worker failure
- [x] **THREAD-05**: On restart, the state worker resumes from the last committed MDBX block

### Commit-Gated Persistence

- [x] **SYNC-01**: The state worker buffers `BlockStateUpdate` values in memory after tracing each block
- [x] **SYNC-02**: The core engine signals the worker to flush state updates only after successful `process_commit_head` progress
- [ ] **SYNC-03**: The worker flushes only updates where `block_number <= commit_head.block_number`
- [ ] **SYNC-04**: MDBX readable height never exceeds the current commit head block number
- [ ] **SYNC-05**: The buffered update queue is bounded and the worker pauses tracing when the buffer is full

### MDBX Source Simplification

- [ ] **MDBX-01**: `MdbxSource` reads the current MDBX height in-process through a shared `Arc<AtomicU64>`
- [ ] **MDBX-02**: `MdbxSource` no longer polls MDBX on a timer to discover block range
- [ ] **MDBX-03**: Sidecar MDBX reads no longer require multiple state replicas or circular buffer depth greater than one
- [ ] **MDBX-04**: `MdbxSource` removes "went too far" handling, range-overlap calculations, and target-range depth logic tied to the old decoupled design

### Fallback and Latency

- [ ] **FALL-01**: The sidecar continues to use `EthRpcSource` as a fallback when the state worker is down or MDBX is behind
- [ ] **FALL-02**: Integrated state worker changes do not regress block processing latency versus the pre-integration baseline

### Validation

- [ ] **VAL-01**: Automated coverage verifies in-process worker startup, restart handling, and panic isolation
- [ ] **VAL-02**: Automated coverage verifies commit-head-gated flushing and bounded buffering behavior
- [ ] **VAL-03**: Automated coverage verifies `MdbxSource` in-process height tracking and fallback behavior without timer polling

## v2 Requirements

### Hardening

- **HARD-01**: MDBX dirty/clean database marking and recovery is driven by the in-process worker lifecycle
- **HARD-02**: Engine and state worker cross-validate traced state diffs before making MDBX state available

## Out of Scope

| Feature | Reason |
|---------|--------|
| Removing `EthRpcSource` fallback | Explicitly retained for resilience in this milestone |
| Cache invalidation recovery via state worker | Deferred until after the in-process integration is proven |
| State diff cross-validation between engine and worker | Valuable hardening work, but not required to land the architectural cutover |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| THREAD-01 | Phase 1 | Complete |
| THREAD-02 | Phase 1 | Complete |
| THREAD-03 | Phase 1 | Complete |
| THREAD-04 | Phase 1 | Complete |
| THREAD-05 | Phase 1 | Complete |
| SYNC-01 | Phase 2 | Complete |
| SYNC-02 | Phase 2 | Complete |
| SYNC-03 | Phase 2 | Pending |
| SYNC-04 | Phase 2 | Pending |
| SYNC-05 | Phase 2 | Pending |
| MDBX-01 | Phase 3 | Pending |
| MDBX-02 | Phase 3 | Pending |
| MDBX-03 | Phase 3 | Pending |
| MDBX-04 | Phase 3 | Pending |
| FALL-01 | Phase 4 | Pending |
| FALL-02 | Phase 4 | Pending |
| VAL-01 | Phase 4 | Pending |
| VAL-02 | Phase 4 | Pending |
| VAL-03 | Phase 4 | Pending |

**Coverage:**
- v1 requirements: 19 total
- Mapped to phases: 19
- Unmapped: 0 ✓

---
*Requirements defined: 2026-03-24*
*Last updated: 2026-03-24 after Phase 1 completion*
