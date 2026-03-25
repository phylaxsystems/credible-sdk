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
- optionally reports invalidating transactions to external services.

It runs in a restart loop in `src/main.rs`. `run_sidecar_once` starts a full sidecar instance; if a recoverable component exits, the outer loop restarts the process and increments `sidecar_restarts_total`.

## Runtime schema

```text
driver
  |
  v
gRPC transport
  |
  v
EventSequencing
  |
  +-----------------------> CommitHead / NewIteration / Tx / Reorg
  |                                                   |
  v                                                   v
TransactionsState <----------------------------- CoreEngine ------------------> IncidentObserver
  ^                                                   |
  |                                                   |
  +---------------- SubscribeResults -----------------+
                                                      |
                                                      v
                                            AssertionExecutor
                                                      |
                                                      v
                                           OverlayDb + Sources
                                                      |
                            +-------------------------+----------------------+
                            |                                                |
                            v                                                v
                       MdbxSource                                       EthRpcSource
                            |                                                |
                            v                                                v
                           MDBX                                       execution RPC

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

Before any tx executes, the engine applies system-contract writes for:

- EIP-4788 if Cancun is active,
- EIP-2935 if Prague is active.

Those writes are applied into the iteration fork, not directly to canonical overlay state.

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

If validation fails, the engine invalidates all in-memory iteration state and clears the overlay cache instead of committing the block.

If validation succeeds:

1. the selected iteration's fork is merged into the underlying overlay cache,
2. block hash is cached for `BLOCKHASH`,
3. metrics are finalized,
4. current head advances,
5. all block-iteration data is cleared.

Sidecar does not keep multi-block speculative state in memory. Iteration state is only for the next block.

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

- `MdbxSource`: reads from the `state-worker` MDBX database,
- `EthRpcSource`: falls back to JSON-RPC and WS-backed reads.

Selection policy:

- sources are queried in priority order,
- only sources considered synced for the relevant block range are eligible,
- the first successful response wins.

`MdbxSource` continuously polls the MDBX available block range and computes a target block as the intersection of:

- cache-required range `[min_synced_block, latest_head]`,
- state-worker-available range `[oldest, observed_head]`.

`EthRpcSource` tracks latest head from WS subscriptions and uses HTTP for reads.

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
