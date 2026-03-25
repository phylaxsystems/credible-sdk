# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-25)

**Core value:** MDBX writes never exceed the current commit head — the core engine controls exactly when state becomes visible, eliminating all "went too far" and range-synchronization bugs.
**Current focus:** Phase 1 — Thread Scaffold

## Current Position

Phase: 1 of 3 (Thread Scaffold)
Plan: 0 of ? in current phase
Status: Ready to plan
Last activity: 2026-03-25 — Roadmap created, Phase 1 ready for planning

Progress: [░░░░░░░░░░] 0%

## Performance Metrics

**Velocity:**
- Total plans completed: 0
- Average duration: —
- Total execution time: —

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| - | - | - | - |

**Recent Trend:**
- Last 5 plans: —
- Trend: —

*Updated after each plan completion*

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- [Init]: Embed state worker as OS thread (not tokio task) — matches EventSequencing/CoreEngine pattern; isolated runtime prevents starvation
- [Init]: Use flume::unbounded() for CommitHead signal channel — bounded channel risks deadlock between engine and state worker
- [Init]: 128-block bounded buffer — memory/headroom tradeoff; backpressure when engine stalls
- [Init]: Remove standalone binary entirely — clean cut, no parallel mode, no feature flag

### Pending Todos

None yet.

### Blockers/Concerns

- [Research]: catch_unwind soundness for MDBX mid-write panics — MEDIUM confidence; add integration test that panics mid-commit_block and confirms MDBX height consistency after restart (address in Phase 1)
- [Research]: tokio select! shutdown interruption of in-flight prestateTracer RPC — verify behavior with integration test in Phase 1 or Phase 2
- [Research]: Circular buffer depth reduction config location and downstream effects on MDBX namespace creation — confirm at start of Phase 3

## Session Continuity

Last session: 2026-03-25
Stopped at: Roadmap and STATE.md initialized; no plans written yet
Resume file: None
