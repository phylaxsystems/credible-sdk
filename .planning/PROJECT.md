# Credible SDK — State Worker Thread Integration

## What This Is

Refactoring the Credible Layer sidecar to embed the state worker as an in-process OS thread instead of running it as a separate binary. The state worker currently runs standalone, writing chain state diffs to MDBX on disk while the sidecar polls that same MDBX every 50ms. This work eliminates the two-process architecture by integrating the state worker into the sidecar with a CommitHead flow-control mechanism, removing polling, circular buffer depth logic, and range-overlap calculations.

## Core Value

MDBX writes never exceed the current commit head — the core engine controls exactly when state becomes visible, eliminating all "went too far" and range-synchronization bugs.

## Requirements

### Validated

- ✓ Sidecar receives transactions via gRPC and validates them against assertion contracts — existing
- ✓ CoreEngine, EventSequencing, TransactionObserver run as dedicated OS threads with panic recovery — existing
- ✓ MdbxSource implements Source trait to provide state to the executor — existing
- ✓ EthRpcSource serves as fallback state source — existing
- ✓ State worker traces blocks via prestateTracer and writes diffs to MDBX — existing
- ✓ Error recoverability classification (Recoverable/Unrecoverable) drives restart behavior — existing

### Active

- [ ] State worker runs as OS thread inside sidecar (R1)
- [ ] State worker has own single-threaded tokio runtime, isolated from sidecar main runtime (R1)
- [ ] State worker panics are caught and do not crash the sidecar (R1)
- [ ] State worker auto-restarts on failure with backoff matching existing OS thread patterns (R1)
- [ ] EthRpcSource remains as fallback when state worker is down or behind (R1)
- [ ] State worker buffers BlockStateUpdates in memory after tracing each block (R2)
- [ ] Core engine signals "flush up to block N" via mpsc channel after each process_commit_head (R2)
- [ ] State worker flushes buffered updates where block_number <= N to MDBX (R2)
- [ ] MDBX height never exceeds current commit_head.block_number (R2)
- [ ] Buffer is bounded at 128 blocks — state worker pauses tracing when full (R2)
- [ ] On restart, state worker resumes from last committed block in MDBX (R2)
- [ ] MdbxSource reads MDBX height in-process via shared Arc<AtomicU64> — no polling (R3)
- [ ] Multiple state replicas (circular buffer depth > 1) no longer required (R3)
- [ ] MdbxSource does not handle "went too far" case (R3)
- [ ] calculate_target_block range intersection and state depth logic removed (R3)
- [ ] Standalone state-worker binary crate removed from workspace (cleanup)

### Out of Scope

- Cache invalidation recovery via state worker (dirty/clean DB marking) — future work
- State diff cross-validation between engine and state worker — future work
- Removing EthRpcSource fallback — intentionally kept as safety net
- Changes to shadow-driver, pcl, assertion-da, or verifier-service

## Context

- **ENG-2101 spike completed** — architecture validated, this is the implementation
- **Existing thread patterns** — EventSequencing and CoreEngine already run as dedicated OS threads with panic catching and restart backoff; state worker will follow the same pattern
- **Sidecar uses flume channels** for inter-component communication; CommitHead signal uses mpsc channel (tokio or std) from engine to state worker thread
- **MDBX circular buffer** in `crates/mdbx` currently supports configurable depth and namespace rotation — depth > 1 becomes unnecessary
- **State worker currently standalone** at `crates/state-worker/src/main.rs` — subscribes to execution client newHeads, traces blocks, writes to MDBX
- **MdbxSource currently polls** MDBX every 50ms to discover available block range; this polling and the associated range-overlap logic are the primary complexity being removed
- **Clean cut migration** — no feature flag, no parallel standalone mode during rollout

## Constraints

- **Tech stack**: Rust nightly-2026-01-07, tokio, MDBX via reth-libmdbx — no new major dependencies
- **Threading**: State worker must have its own single-threaded tokio runtime (same isolation as EventSequencing/CoreEngine)
- **Compatibility**: EthRpcSource fallback must continue working when state worker is unavailable
- **Performance**: No regression in block processing latency
- **Safety**: Workspace Clippy denies panic/unwrap/expect — all error paths must be explicit

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Embed state worker as OS thread, not tokio task | Match existing EventSequencing/CoreEngine pattern; dedicated runtime isolation prevents one component starving another | — Pending |
| mpsc channel for CommitHead signal | Ordered delivery of commit head updates from engine to state worker; simpler than atomic + notify for sequenced messages | — Pending |
| 128-block bounded buffer | Balanced memory/headroom tradeoff; enough runway for commit lag without excessive memory | — Pending |
| Remove standalone binary entirely | Clean cut — no parallel mode, no feature flag; simplifies workspace and Helm charts | — Pending |
| Match existing backoff strategy | Consistency with EventSequencing/CoreEngine restart patterns; one behavior to understand | — Pending |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd:transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd:complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-03-25 after initialization*
