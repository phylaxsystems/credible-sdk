# Sidecar

This file covers `crates/sidecar`.

## What the sidecar is

`sidecar` is a long-running process that:

- accepts rollup-driver events over gRPC,
- sequences them into a valid execution order,
- executes candidate transactions against EVM state,
- validates those transactions against registered assertions,
- exposes transaction results and health endpoints,
- indexes assertions from external sources,
- optionally reports invalidating transactions to external services,
- supervises the integrated state-worker runtime that traces blocks and advances durable MDBX visibility.

It runs in a restart loop in `src/main.rs`. `run_sidecar_once` starts a full sidecar instance; if a recoverable component exits, the outer loop restarts the process and increments `sidecar_restarts_total`.

`sidecar` is now the only supported production topology for state ingestion. The standalone `state-worker` binary remains available only for bootstrap, manual catch-up, and recovery workflows.

## Runtime schema

```text
execution node ---> integrated state worker ---> traced block buffer
       ^                        |                         |
       |                        |                         v
       |                        |                flush permission from CommitHead
       |                        |                         |
       |                        v                         v
       |                     MDBX <---------------- CoreEngine <--- EventSequencing <--- gRPC transport <--- driver
       |                        ^                         |                                      |
       |                        |                         +--> TransactionsState <---------------+
       |                        |                         |
       |                        |                         +--> IncidentObserver
       |                        |                         |
       |                        |                         +--> AssertionExecutor
       |                        |                                   |
       |                        |                                   v
       |                        +--------------------------- OverlayDb + Sources
       |                                                            |
       |                                  +-------------------------+----------------------+
       |                                  |                                                |
       |                                  v                                                v
       |                             MdbxSource                                       EthRpcSource
       |                                  |                                                |
       +----------------------------------+------------------------------------------------+

side tasks:
GraphQL event source -> sidecar indexer -> AssertionStore
Assertion DA client  -> reachability monitor
Axum health server   -> /health
```

## Internal topology

`run_sidecar_once` wires together:

- gRPC transport,
- event sequencing thread,
- core engine thread,
- integrated state-worker supervisor thread,
- optional transaction observer thread,
- health server,
- assertion indexer task,
- assertion DA reachability monitor task.

The queue topology is:

1. transport -> sequencing (`flume`)
2. sequencing -> engine (`flume`)
3. engine -> optional observer (`flume`)
4. engine -> result stream (`flume`) -> transport subscribers

## Transport

Current transport is gRPC only.

Important behavior:

- `StreamEvents` carries `CommitHead`, `NewIteration`, `Tx`, and `Reorg`.
- `SubscribeResults` streams transaction results as they become available.
- point lookups exist for one or multiple transaction results.

The transport deduplicates event IDs and records RPC duration metrics.

`TransactionsState` is the transport-facing shared state:

- `accepted_txs`: txs accepted by transport but not yet completed by the engine,
- `transaction_results`: completed results keyed by `TxExecutionId`,
- optional result-event sender for push streaming.

Accepted txs are TTL-pruned in a background task so in-flight markers do not grow without bound.

## Event sequencing

`EventSequencing` is an ordering and validation layer between transport and engine.

Its job is not execution. Its job is to prevent obviously invalid orderings from reaching the engine.

Key rules:

- `CommitHead` is forwarded immediately.
- non-commit events are buffered in block-scoped context until dependencies are satisfied.
- events for blocks `<= current_head` are ignored once the first commit head has been received.
- reorgs are validated against the tail of already-sent tx events.

The engine still performs defense-in-depth validation. Sequencing reduces invalid input but does not make engine validation unnecessary.

## Core engine

`CoreEngine` is the stateful execution core.

It owns:

- a shared `OverlayDb<DB>` cache over external state sources,
- per-iteration `VersionDb<OverlayDb<DB>>`,
- the assertion executor,
- transaction result bookkeeping,
- cache invalidation policy,
- system-call application for new iterations,
- reorg handling,
- block and tx metrics.

The engine processes four event types:

- `NewIteration`
- `CommitHead`
- `Tx`
- `Reorg`

## Iterations and block building

An iteration is a candidate build of block `current_head + 1`.

Each `NewIteration` creates a `BlockIterationData` containing:

- a fresh `VersionDb` rooted at the current overlay state,
- an ordered list of executed txs,
- the `BlockEnv`.

Before any tx executes, the engine applies system-contract writes (EIP-4788 first, then EIP-2935). Those writes are applied into the iteration fork, not directly to canonical overlay state. See [eips.md](eips.md) for execution order, storage layout, and slot calculation details.

## Transaction execution behavior

For a tx:

1. the engine checks that at least one synced source is available, unless source checks can be skipped because nothing needs fetching;
2. it finds the target iteration;
3. it rejects txs targeting the wrong block;
4. it rejects tx processing if the iteration's last commit was empty (`NothingToCommit`);
5. it executes the tx through `AssertionExecutor`.

The engine distinguishes:

- `ValidationCompleted { execution_result, is_valid }`
- `ValidationError(String)`

Important nuance:

- EVM execution failure inside the tx is not necessarily an engine failure; reverted or halted txs can still become `ValidationCompleted`.
- pre-execution validation failures like nonce, funds, or gas issues become `ValidationError`.
- assertion-system failures are treated as fatal or unrecoverable.

If assertions fail:

- the tx is marked invalid,
- the engine emits incident reports if enabled,
- the state delta is still tracked in the per-iteration `VersionDb` commit log as returned by the executor.

## Commit head behavior

`CommitHead` is the point where the driver declares which iteration should become canonical for that block.

On `CommitHead` the engine first validates cache coherence:

- commit head must be exactly `current_head + 1`,
- the selected iteration must exist,
- the last tx hash must match,
- valid tx count must match.

If validation fails, the engine invalidates all in-memory iteration state and clears the overlay cache instead of committing the block. It also must not advance flush permission for the integrated worker, so MDBX visibility stays pinned at the previous durable head.

If validation succeeds:

1. the selected iteration's fork is merged into the underlying overlay cache,
2. block hash is cached for `BLOCKHASH`,
3. the block becomes `flush-permitted` for the integrated worker,
4. metrics are finalized,
5. current head advances,
6. all block-iteration data is cleared.

Sidecar does not keep multi-block speculative state in memory. Iteration state is only for the next block.

## State lifecycle and durable visibility

The integrated worker and `CoreEngine` share one visibility contract:

1. `traced`: the worker has traced block `N` and built a `BlockStateUpdate`, but it is still in memory only.
2. `flush-permitted`: `CommitHead` has accepted block `N`, so the worker may now flush that update.
3. `durably flushed`: `mdbx::Writer::commit_block` for block `N` succeeds, so the committed MDBX window advances and becomes authoritative.
4. `worker-behind`: sidecar needs a block range that the durable MDBX window no longer covers, even if the worker has already traced farther ahead in memory.
5. `fallback/return`: `Sources` serves from `EthRpcSource` while MDBX is behind, then returns to `MdbxSource` once the durable committed window covers the required range again.

Two rules follow from that contract:

- durable MDBX visibility is authoritative; traced-but-unflushed state must never become readable through `MdbxSource`;
- restart resumes from the last durable MDBX block already stored in `crates/mdbx`, and any unflushed traced updates are discarded.

## Reorg behavior

Reorgs here are tx-tail rollbacks inside the current iteration, not full chain reorg support.

Behavior:

- the engine validates reorg request depth and tx hash tail,
- `VersionDb::rollback_to` rebuilds state to the requested commit depth,
- removed tx results are deleted from `TransactionsState`.

Important limitation:

- sidecar iteration reorgs are supported,
- chain-level reorging of already-committed blocks is not what this path handles.

## External state sources

`sidecar::cache::Sources` is a source multiplexer. Each source implements `DatabaseRef`.

The relevant source types are:

- `MdbxSource`: reads from the durable MDBX database owned by the integrated runtime,
- `EthRpcSource`: falls back to JSON-RPC and WS-backed reads.

Selection policy:

- sources are queried in priority order,
- only sources considered synced for the relevant block range are eligible,
- the first successful response wins.

`MdbxSource` should be driven by an in-process committed-visibility window:

- initialize that window once from MDBX metadata at startup,
- advance it only after successful durable flushes,
- report unsynced whenever the durable window does not cover the cache-required range.

`EthRpcSource` tracks latest head from WS subscriptions and uses HTTP for reads.

This means fallback is based on durable coverage, not worker liveness alone. A healthy worker that has only traced ahead in memory does not make MDBX eligible until the corresponding blocks are durably flushed.

## Assertion indexer

The assertion indexer is a separate async task inside `sidecar`.

Per sync cycle it:

1. fetches the external indexer head with retry and backoff,
2. refuses to move backward if the upstream head regresses,
3. fetches added and removed events concurrently,
4. fetches DA payloads and extracts assertion contracts for add events,
5. applies all resulting modifications to `AssertionStore`,
6. updates current synced block and metrics.

The sidecar treats event-source reachability as important. `init_indexer_config` performs a startup health check before the runtime is allowed to proceed.

## Transaction observer

The observer is optional and only starts if all required config is present.

It receives `IncidentReport`s from the engine, persists them immediately to its own MDBX database, and asynchronously republishes them to:

- the dapp API,
- an optional Aeges endpoint.

Persistence-first behavior is deliberate:

- report receipt is treated as mission-critical,
- reports are fsynced to disk immediately,
- deletion only happens after positive publish success,
- duplicate delivery is acceptable; silent loss is not.

## Failure model

The sidecar explicitly classifies component errors as recoverable vs unrecoverable.

Typical unrecoverable cases:

- database corruption or fatal DB errors,
- assertion execution system failures,
- transport bind or server failures.

Typical recoverable cases:

- queue or channel issues,
- transient source sync starvation,
- iteration or reorg validation issues,
- certain transport or client failures.

Recoverable engine errors usually trigger cache invalidation and let the outer supervisor restart the process.

Worker failure is also a recoverable degraded-mode condition: the sidecar process stays live, but MDBX can fall behind and `EthRpcSource` may become the only serving source until the worker restarts or catches back up.

## Readiness and observability

The health contract is no longer "process is running". Operators should treat the runtime as:

- live while the sidecar process and supervisor loop are still running,
- ready only while at least one state source can satisfy the cache's required block range.

This means:

- worker failure alone should not crash sidecar;
- readiness should degrade when neither durable MDBX nor `EthRpcSource` can serve the required range;
- metrics should continue to expose the existing `state_worker_*` series for continuity, alongside sidecar-owned metrics for supervisor state, restart count, traced head, flush-permitted head, durable flushed head, and fallback activation.

## Open rollout questions

Two rollout questions remain intentionally unresolved in the docs because this repository snapshot does not answer them:

- the benchmark or SLO threshold that should trigger rollout acceptance for the integrated topology is still open;
- the repository does not identify any remaining external standalone-`state-worker` consumers, so operators must explicitly confirm whether any out-of-repo deployments still depend on the utility binary before removing dual-process runbooks.
