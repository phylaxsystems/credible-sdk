# Architecture

This document is aimed at agents working in this repository. It describes the current behavior of the runtime components that matter for state ingestion, state serving, transaction validation, and snapshot hydration:

- `crates/sidecar`
- `crates/mdbx`
- `crates/state-worker`
- `crates/assertion-executor`
- `crates/assertion-da/da-client`
- `crates/assertion-da/da-core`
- `crates/assertion-da/da-server`
- `crates/pcl/cli`
- `crates/pcl/common`
- `crates/pcl/core`
- `crates/pcl/phoundry`
- `scripts/geth_snapshot` (the repo's snapshot component)

It is intentionally behavior-first. It focuses on what the code does now, what invariants it relies on, and where component boundaries actually are.

## Scope and system shape

At a high level:

1. `state-worker` ingests chain state block-by-block from an execution node and writes it into an MDBX circular buffer.
2. `geth_snapshot` can bootstrap that MDBX database from a one-shot Geth dump or a JSONL dump.
3. `sidecar` consumes driver events over gRPC, executes transactions against an in-memory overlay plus external state sources, and validates them with `assertion-executor`.
4. `assertion-executor` executes the transaction, derives triggers from the resulting trace, selects matching assertions from the assertion store, and runs those assertions in parallel against forked EVM state.
5. `sidecar` also runs an assertion indexer that keeps the assertion store synchronized from an external event source plus DA fetches.
6. `assertion-da` is the artifact-availability service for assertions: it accepts Solidity submissions, compiles them, stores them, signs their deployment hash, and serves the resulting source plus bytecode back over JSON-RPC.
7. `pcl` is the operator/developer CLI stack: it authenticates against the app backend, builds and tests assertions through Foundry/phoundry, stores assertion artifacts in Assertion DA, and submits or applies them to projects.

The important architectural split is:

- `state-worker` and `geth_snapshot` produce state.
- `mdbx` stores and serves state.
- `sidecar` is the transaction admission and orchestration process.
- `assertion-executor` is the EVM/assertion engine used by `sidecar`.
- `assertion-da` is the durable assertion-artifact service used by both indexing and authoring flows.
- `pcl` is the user-facing CLI layer that packages local assertion code and pushes it into the DA/app ecosystem.

## High-level schemas

### End-to-end system schema

```text
                  assertion events + DA payloads
        +----------------------------------------------+
        |                                              v
+----------------+     read/write state      +--------------------+
| geth_snapshot  | ------------------------> |        MDBX        |
| state bootstrap|                           | circular buffer DB |
+----------------+                           +--------------------+
        ^                                              ^
        |                                              |
        | live geth dump / JSONL                       | StateReader
        |                                              |
+----------------+     commit_block()        +--------------------+
| state-worker   | ------------------------> | sidecar MdbxSource |
| block tracer   |                           +--------------------+
+----------------+                                      |
        ^                                               |
        | debug_trace / newHeads                        |
        |                                               v
  +-------------------+                        +--------------------+
  | execution client  |                        |      sidecar       |
  |   (Geth / node)   |                        | transport + engine |
  +-------------------+                        +--------------------+
                                                         |
                                                         | validate tx
                                                         v
                                                +--------------------+
                                                | assertion-executor |
                                                | tx + assertions    |
                                                +--------------------+
```

### Sidecar runtime schema

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

## Cross-component data flow

### State path

1. `state-worker` traces each block with Geth `prestateTracer` in diff mode.
2. It converts per-tx trace output into a per-block `mdbx::BlockStateUpdate`.
3. It merges EIP-2935 and EIP-4788 system-contract state changes into that update.
4. It commits the update via `mdbx::StateWriter`.
5. `sidecar` can read this state through `StateReader`, wrapped as a `MdbxSource`.

### Transaction validation path

1. External driver sends `CommitHead`, `NewIteration`, `Tx`, and `Reorg` events to `sidecar` gRPC.
2. gRPC transport forwards events into the internal queue.
3. `EventSequencing` enforces ordering/dependencies and forwards only validly ordered events to the engine.
4. `CoreEngine` builds per-iteration `VersionDb` state on top of an `OverlayDb<Sources>`.
5. For each tx, `AssertionExecutor` executes the tx and runs triggered assertions.
6. Engine stores a `TransactionResult`, emits optional incident reports, and may later commit or roll back the iteration based on `CommitHead` or `Reorg`.

### Assertion lifecycle path

1. `sidecar::indexer` polls a GraphQL event source for added/removed assertion events.
2. For added events it fetches proof/bytecode from DA, extracts the deployed assertion contract plus recorded triggers, and creates `PendingModification::Add`.
3. For removed events it creates `PendingModification::Remove`.
4. `AssertionStore` applies those modifications per adopter and maintains an expiry index for prunable inactive assertions.
5. During tx execution, the call tracer produces triggers and the store returns matching active assertions for that adopter and block.

### State production schema

```text
execution node
  |
  +--> newHeads subscription ------------------------------+
  |                                                        |
  +--> debug_trace_block_by_number(prestateTracer diff)    |
                                                           v
                                                   state-worker
                                                           |
                                                           +--> trace collapse
                                                           |
                                                           +--> system call merge
                                                           |
                                                           v
                                                  BlockStateUpdate
                                                           |
                                                           v
                                                    mdbx::StateWriter
                                                           |
                                                           v
                                                         MDBX
```

### Transaction validation schema

```text
driver event stream
  |
  v
GrpcTransport
  |
  v
EventSequencing
  |
  +--> buffer / dependency checks
  +--> reorg tail validation
  |
  v
CoreEngine
  |
  +--> build/find iteration VersionDb
  +--> apply EIP-4788 / EIP-2935 at iteration start
  +--> execute tx
  |
  v
AssertionExecutor
  |
  +--> execute user tx with CallTracer
  +--> derive triggers
  +--> read AssertionStore
  +--> run matching assertion fns in parallel
  |
  v
TxValidationResult
  |
  +--> TransactionsState / result streaming
  +--> optional IncidentReport
  +--> possible later rollback on Reorg
  +--> possible later canonicalization on CommitHead
```

### Assertion indexing schema

```text
GraphQL event source
  |
  +--> AssertionAdded
  +--> AssertionRemoved
  |
  v
sidecar::indexer
  |
  +--> fetch DA payload / proof
  +--> extract assertion contract
  +--> extract triggers() and assertion spec
  |
  v
PendingModification::{Add,Remove}
  |
  v
AssertionStore
  |
  +--> active assertions by adopter
  +--> expiry index for later pruning
```

### Assertion DA schema

```text
sidecar indexer
  |
  v
assertion-da-client
  |
  v
assertion-da-server (JSON-RPC over HTTP)
  |
  +--> verify JSON-RPC envelope + method + body size
  |
  +--> da_submit_solidity_assertion
  |      |
  |      +--> compile Solidity in Dockerized solc
  |      +--> ABI-encode constructor args
  |      +--> keccak256(deployment bytecode + args) => assertion id
  |      +--> sign id with DA private key
  |      +--> persist StoredAssertion
  |
  +--> da_get_assertion
  |      |
  |      +--> fetch StoredAssertion by id
  |
  v
db task over mpsc
  |
  +--> Sled
  |
  +--> Redis
```

### PCL schema

```text
developer / operator
  |
  v
pcl CLI
  |
  +--> auth   -> app auth endpoints -> store tokens in CliConfig
  |
  +--> build  -> pcl-phoundry -> Foundry compile
  |
  +--> test   -> pcl-phoundry -> phorge / Foundry tests
  |
  +--> apply  -> credible.toml + build_and_flatten -> app releases endpoint
```

## `crates/sidecar`

### What the sidecar is

`sidecar` is a long-running process that:

- accepts rollup-driver events over gRPC,
- sequences them into a valid execution order,
- executes candidate transactions against EVM state,
- validates those transactions against registered assertions,
- exposes transaction results and health endpoints,
- indexes assertions from external sources,
- optionally reports invalidating transactions to external services.

It runs in a restart loop in `src/main.rs`. `run_sidecar_once` starts a full sidecar instance; if a recoverable component exits, the outer loop restarts the process and increments `sidecar_restarts_total`.

### Internal topology

`run_sidecar_once` wires these pieces together:

- gRPC transport
- event sequencing thread
- core engine thread
- optional transaction observer thread
- health server
- assertion indexer task
- assertion DA reachability monitor task

The queue topology is:

1. transport -> sequencing (`flume`)
2. sequencing -> engine (`flume`)
3. engine -> optional observer (`flume`)
4. engine -> result stream (`flume`) -> transport subscribers

### Main runtime responsibilities

#### 1. Transport

Current transport is gRPC only.

Important endpoints/behavior:

- `StreamEvents`: bidirectional stream carrying `CommitHead`, `NewIteration`, `Tx`, and `Reorg`.
- `SubscribeResults`: server stream of transaction results as they become available.
- point lookups for one or multiple transaction results.

The transport also deduplicates event IDs and records RPC duration metrics.

`TransactionsState` is transport-facing shared state:

- `accepted_txs`: txs accepted by transport but not yet completed by engine
- `transaction_results`: completed results keyed by `TxExecutionId`
- optional result-event sender for push streaming

Accepted txs are TTL-pruned in a background task. This avoids unbounded buildup of "in-flight" markers if callers disappear or the engine never produces a result.

#### 2. Event sequencing

`EventSequencing` is an ordering/validation layer between transport and engine.

Its job is not execution. Its job is to prevent obviously invalid orderings from reaching the engine.

Key rules:

- `CommitHead` is forwarded immediately.
- non-commit events are buffered in block-scoped context until dependencies are satisfied.
- events for blocks `<= current_head` are ignored once the first commit head has been received.
- reorgs are validated against the tail of already-sent tx events.

Important consequence: the engine still performs defense-in-depth validation. Sequencing reduces invalid inputs; it does not make engine validation unnecessary.

#### 3. Core engine

`CoreEngine` is the stateful execution core.

It owns:

- a shared `OverlayDb<DB>` cache over external state sources,
- per-iteration `VersionDb<OverlayDb<DB>>`,
- the assertion executor,
- transaction result bookkeeping,
- cache invalidation policy,
- system-call application for new iterations,
- reorg handling,
- block/tx metrics.

The engine processes four event types:

- `NewIteration`
- `CommitHead`
- `Tx`
- `Reorg`

#### Iterations and block building

An iteration is a candidate build of block `current_head + 1`.

Each `NewIteration` creates a new `BlockIterationData` containing:

- a fresh `VersionDb` rooted at the current overlay state,
- an ordered list of executed txs,
- the `BlockEnv`.

Before any tx executes, engine applies system-contract writes for:

- EIP-4788 if Cancun active
- EIP-2935 if Prague active

Those writes are applied into the iteration fork, not directly to canonical overlay state.

#### Transaction execution behavior

For a tx:

1. engine checks that at least one synced source is available, unless source checks are disabled because nothing needs to be fetched;
2. it finds the target iteration;
3. it rejects txs targeting the wrong block;
4. it rejects tx processing if the iteration's last commit was empty (`NothingToCommit`);
5. it executes the tx through `AssertionExecutor`.

Engine distinguishes:

- `ValidationCompleted { execution_result, is_valid }`
- `ValidationError(String)`

Important nuance:

- tx execution failure inside the EVM is not necessarily an engine failure; reverted or halted txs can still become `ValidationCompleted`.
- pre-execution validation failures like nonce/funds/gas issues become `ValidationError`.
- assertion-system failures are treated as fatal/unrecoverable.

If assertions fail:

- the tx is marked invalid,
- engine emits incident reports if enabled,
- state delta is still tracked in the per-iteration `VersionDb` commit log as returned by the executor.

#### Commit head behavior

`CommitHead` is the point where the driver declares which iteration should become canonical for that block.

On `CommitHead` the engine first validates cache coherence:

- commit head must be exactly `current_head + 1`,
- selected iteration must exist,
- last tx hash must match,
- valid tx count must match.

If that validation fails, engine invalidates all in-memory iteration state and clears overlay cache instead of committing the block.

If validation succeeds:

1. the selected iteration's fork is merged into the underlying overlay cache,
2. block hash is cached for `BLOCKHASH`,
3. metrics are finalized,
4. current head advances,
5. all block-iteration data is cleared.

This means sidecar does not keep multi-block speculative state in memory. Iteration state is per next block only.

#### Reorg behavior

Reorgs are tx-tail rollbacks inside the current iteration, not full chain reorg support.

Behavior:

- engine validates reorg request depth and tx hash tail,
- `VersionDb::rollback_to` rebuilds state to the requested commit depth,
- removed tx results are deleted from `TransactionsState`.

Important limitation:

- sidecar iteration reorgs are supported;
- chain-level reorging of already-committed blocks is not what this engine path handles.

#### External state sources

`sidecar::cache::Sources` is a source multiplexer. Each source implements `DatabaseRef`.

The two relevant source types here are:

- `MdbxSource`: reads from `state-worker` MDBX
- `EthRpcSource`: falls back to JSON-RPC/WS-backed reads

Selection policy:

- sources are queried in priority order,
- only sources considered synced for the relevant block range are eligible,
- first successful response wins.

`MdbxSource` continuously polls the MDBX available block range and computes a target block as the intersection of:

- cache-required range `[min_synced_block, latest_head]`
- state-worker-available range `[oldest, observed_head]`

`EthRpcSource` tracks latest head from WS subscriptions and uses HTTP for reads.

#### Indexer

The assertion indexer is a separate async task.

Per sync cycle it:

1. fetches external indexer head with retry/backoff,
2. refuses to move backward if upstream head regresses,
3. fetches added and removed events concurrently,
4. fetches DA payloads and extracts assertion contracts for add events,
5. applies all resulting modifications to `AssertionStore`,
6. updates current synced block and metrics.

The sidecar treats event-source reachability as important. `init_indexer_config` performs a startup health check before the runtime is allowed to proceed.

#### Transaction observer

The observer is optional and only starts if all config is present.

It receives `IncidentReport`s from the engine, persists them immediately to its own MDBX database, and asynchronously republishes them to:

- dapp API
- optional Aeges endpoint

Persistence-first behavior is deliberate:

- report receipt is treated as mission-critical,
- reports are fsynced to disk immediately,
- deletion only happens after positive publish success,
- duplicate delivery is acceptable; silent loss is not.

### Failure model

The sidecar explicitly classifies component errors as recoverable vs unrecoverable.

Typical unrecoverable cases:

- database corruption or fatal DB errors,
- assertion execution system failures,
- transport bind/server failures.

Typical recoverable cases:

- queue/channel issues,
- transient source sync starvation,
- iteration/reorg validation issues,
- certain transport/client failures.

Recoverable engine errors usually trigger cache invalidation and let the outer supervisor restart the process.

## `crates/mdbx`

### What it is

`mdbx` is a custom blockchain-state storage layer over `reth`'s MDBX bindings.

It exposes:

- `StateReader`
- `StateWriter`
- a `Reader` trait
- a `Writer` trait

The storage model is a circular buffer of block states.

### Data model

The database tables are:

- `NamespaceBlocks`
- `NamespacedAccounts`
- `NamespacedStorage`
- `Bytecodes`
- `BlockMetadata`
- `StateDiffs`
- `Metadata`

Core idea:

- a block maps to a namespace via `block_number % buffer_size`,
- that namespace stores the full reconstructed state for the currently assigned block,
- diffs for blocks are stored separately so rotated namespaces can be rebuilt.

### Circular buffer semantics

This is the most important non-obvious behavior.

When a new block lands in namespace `N`:

1. writer computes the current block's write batch from the diff,
2. it loads the namespace's current base state,
3. if the namespace already contains an old block, it applies intermediate diffs from the old block + 1 up to the new block - 1,
4. then it applies the new block diff,
5. then it atomically writes the resulting namespace contents and metadata.

This means the namespace holds a reconstructed full state for its assigned block, even though only diffs are persisted across blocks.

### Read model

Reads are snapshot-consistent because MDBX MVCC is used.

Reader behavior:

- verify that the requested block is still the current occupant of its namespace,
- if not, return `BlockNotFound`,
- if yes, all account/storage/code reads for that namespace are considered consistent.

This is why `verify_block_available` is critical. Namespace identity alone is not enough; the namespace must still correspond to the requested block number.

### Write model

`StateWriter::commit_block` is atomic and durable.

Pipeline:

1. validate unique accounts in `BlockStateUpdate`,
2. convert update to binary diff representation,
3. build a write batch in parallel,
4. load intermediate diffs / copied base state,
5. overlay batches in order,
6. sort and deduplicate,
7. execute all writes in one MDBX transaction,
8. update diff table and metadata,
9. delete metadata/diff entries that fell out of the buffer.

Important details:

- duplicate accounts in a single block update are treated as an error,
- bytecode is namespaced by `(namespace, code_hash)`, not globally by code hash alone,
- `BlockMetadata` and `StateDiffs` are pruned once older than the buffer horizon,
- namespace contents themselves are overwritten through rotation rather than separately pruned.

### Bootstrap behavior

MDBX supports snapshot bootstrap through:

- `bootstrap_from_snapshot`
- `begin_bootstrap`
- `bootstrap_from_iterator`

Streaming bootstrap writes every account to all namespaces for the chosen starting block. This is intentional: immediately after bootstrap, every namespace points to the same snapshot block until live writes rotate the buffer forward.

`get_available_block_range` therefore computes the range from `NamespaceBlocks`, not from `latest_block - buffer_size + 1`, because after bootstrap the theoretical range would be wrong.

### Metadata repair

`StateWriter::fix_block_metadata` can rewrite:

- `Metadata.latest_block`
- all `NamespaceBlocks`
- the keyed `BlockMetadata` entry

This is used by `geth_snapshot` when snapshot hydration started with placeholder metadata and the true block metadata becomes known later.

## `crates/state-worker`

### What it is

`state-worker` is the continuous chain-state ingestor that produces the MDBX state sidecar can consume.

It is designed for:

- replay from persisted latest block,
- catch-up to head,
- then steady-state head following.

It assumes a Geth-like execution node with debug APIs.

### Startup behavior

On startup it:

1. installs tracing and rustls provider,
2. parses CLI/env config,
3. connects to WS provider,
4. validates Geth version if the client identifies as Geth,
5. opens `StateWriter`,
6. reads and parses a genesis JSON file,
7. builds `SystemCalls` from genesis fork timestamps,
8. constructs a `GethTraceProvider`,
9. enters a restart-on-failure loop.

Geth version must be at least `1.16.6` if the client is Geth. This is required because earlier versions incorrectly report post-Cancun `SELFDESTRUCT` in `prestateTracer` diff mode.

### Ingestion model

`StateWorker::run` has two phases:

- catch-up: sequentially process blocks until caught up to current head,
- streaming: subscribe to `newHeads` and process new blocks in order.

Important current limitation:

- missing blocks during streaming are treated as an error and cause retry,
- already-processed stale headers are skipped,
- chain reorg handling is not implemented.

### Block processing

For each block:

1. fetch block state via trace provider,
2. apply EIP-2935/EIP-4788 state updates,
3. commit block to MDBX,
4. update metrics.

Block `0` is special:

- if DB is empty and a genesis file was provided, worker hydrates block 0 from the genesis file instead of tracing it.

### Trace semantics

`GethTraceProvider` uses `debug_trace_block_by_number` with Geth built-in `PreStateTracer` in `diff_mode = true`.

Trace conversion logic in `state::geth`:

- accumulates touched accounts across all tx traces in the block,
- carries forward baseline pre-state account fields,
- uses post-state for final changed values,
- infers storage deletions when a slot exists in pre but not post,
- infers account deletion when account exists in pre but not post,
- correctly treats post-Cancun `SELFDESTRUCT` balance-only semantics.

Final output is a block-level `BlockStateUpdate` keyed by address hash and slot-hash.

### System calls

State-worker computes EIP system-contract state independently from traces and merges it into the block update.

This is intentionally defensive:

- if tracing already includes those writes, merge logic overwrites consistently,
- if tracing omits them, state-worker still persists expected canonical state.

If system-call computation fails, worker logs a warning and continues instead of failing the block. The assumption in code is that traces may already contain the writes.

## `crates/assertion-executor`

### What it is

`assertion-executor` is the EVM execution and assertion-validation library used by sidecar.

Its relevant subareas are:

- executor
- assertion store
- DB layering (`OverlayDb`, `ForkDb`, `VersionDb`, `MultiForkDb`)
- inspectors and precompiles

### Validation model

For a tx, the executor does:

1. execute the user tx against a fork DB while collecting a `CallTracer`,
2. if tx execution itself is not successful, skip assertions and return valid-without-assertions,
3. derive triggers from the call tracer,
4. query the assertion store for matching active assertions for the tx block,
5. execute matched assertion functions in parallel,
6. aggregate failures and build `TxValidationResult`.

Important behavior:

- only successful tx execution leads to assertion execution,
- assertion contracts are treated as read-only checks over forked state,
- if any executed assertion function fails, the tx is marked invalid,
- tx state may still be committed into the fork DB depending on caller behavior.

### Assertion execution mechanics

For each assertion contract:

1. persistent accounts from the assertion contract are inserted into the tx fork DB,
2. a `MultiForkDb` is created from the tx fork DB plus post-tx journal,
3. each assertion function selector is executed in parallel on a rayon pool,
4. each function runs as a call to `ASSERTION_CONTRACT`,
5. phevm inspector state and console logs are captured,
6. gas/selector/failure data is aggregated.

The parallelism is per assertion function, not per transaction.

### Trigger selection

The assertion store stores, per assertion:

- activation block
- optional inactivation block
- extracted deployed assertion contract
- recorded triggers
- assertion spec

At runtime:

- call tracer exposes triggers grouped by adopter,
- store filters assertions active at the current block,
- matching selectors are the union of:
  - exact trigger matches
  - `AllCalls` if any call trigger matched
  - `AllStorageChanges` if any storage-change trigger matched

This is the actual trigger expansion behavior agents need to preserve.

### Assertion extraction

When indexing a newly added assertion:

1. deployment bytecode is executed in an in-memory DB,
2. deployed assertion contract is recovered from the post-state account at `ASSERTION_CONTRACT`,
3. the `triggers()` function is called against a trigger recorder account,
4. trigger recorder output becomes persisted matching metadata,
5. spec recorder output becomes the assertion spec.

If extraction fails, the sidecar indexer skips that assertion event rather than crashing the whole index cycle.

### Assertion store behavior

The store supports:

- in-memory backend
- sled backend

It groups assertions by adopter address.

Modification model:

- `PendingModification::Add` adds or reactivates an assertion
- `PendingModification::Remove` sets `inactivation_block`

Pruning model:

- inactive assertions are not removed immediately,
- an expiry index keyed by `(inactivation_block, adopter, assertion_id)` is maintained,
- background task prunes assertions when `current_block > retention_blocks`,
- pruning removes assertions whose inactivation block is older than the retention horizon.

This retention window exists to tolerate reorg-style needs in upstream indexing.

### DB layering

#### `OverlayDb`

Shared in-memory cache over an underlying `DatabaseRef`.

Used by sidecar as the long-lived state cache.

Properties:

- shared `DashMap`,
- caches account, storage, code, and block hash entries,
- can be invalidated wholesale,
- can spawn monitoring metrics task,
- can create `ForkDb`s and `ActiveOverlay`s.

#### `ForkDb`

Per-execution mutable overlay over another DB.

Used for:

- tx execution on top of overlay/canonical state,
- assertion execution forks.

It stores account/storage/code mutations and falls back to inner DB on misses.

#### `VersionDb`

Adds rollback semantics on top of `ForkDb`.

Used by sidecar per iteration.

Key behavior:

- keeps `base_state`,
- keeps live `state`,
- records commit log as `Vec<Option<EvmState>>`,
- `None` entries represent failed tx slots with no state changes,
- `rollback_to(depth)` rebuilds live state by replaying the log from base state.

This `None`-slot design is important because reorg depth is defined over tx positions, not only stateful commits.

#### `MultiForkDb`

Manages multiple logical forks for assertion execution.

Used when assertions or cheatcodes need pre/post-tx and pre/post-call fork views.

It can switch between forks and preserve persistent accounts across fork transitions.

## `scripts/geth_snapshot`

### What it is

`geth_snapshot` is the one-shot bootstrap/hydration CLI for MDBX.

It is the "snapshot" component in this repo.

It can:

- read directly from a Geth datadir by shelling out to `geth`,
- read from newline-delimited JSON dumps,
- write JSONL output,
- stream state into MDBX using `StateWriter::begin_bootstrap`,
- fix block metadata on an existing MDBX database.

### Input modes

There are two mutually exclusive inputs:

- live Geth via `--datadir`
- JSONL via `--json`

If reading from Geth:

- backend can be `snapshot`, `trie`, or `auto`,
- `auto` tries snapshot first then trie and has specialized fallback logic.

### Parsing model

The parser is intentionally fail-hard.

If any account JSON or account contents are malformed, the tool aborts with a fatal integrity error instead of continuing with partial state.

Important parsing behavior:

- balances can be hex or decimal,
- storage values from Geth dumps are RLP-decoded before turning into `U256`,
- code hash is validated against provided code,
- missing code requires code hash to be zero or `KECCAK256_EMPTY`.

### Streaming write model

The tool does not accumulate the full state in memory.

Main path:

1. optional JSON output sink is opened,
2. optional MDBX bootstrap session is started with placeholder metadata,
3. each parsed account is written immediately to output sinks,
4. after input completes, final block metadata is applied and bootstrap is finalized.

For JSON input, parsing and writing are parallelized with a bounded channel:

- parser thread reads and deserializes,
- main thread writes accounts to sinks.

### Geth fallback behavior

`run_with_fallback` is the heart of the live dump mode.

Behavior:

- `snapshot` backend runs `geth snapshot dump`
- `trie` backend runs `geth dump --iterative --incompletes`
- `auto` tries snapshot first, then trie if snapshot failure looks retryable

There is special detection for pruned historical state:

- parse snapshot-head mismatch error,
- parse missing trie node/state-not-available error,
- if they correlate to the same missing root, emit a clear archive/pruning diagnosis.

### Metadata repair mode

`--fix-metadata` opens an existing MDBX DB and rewrites block metadata without re-hydrating state.

This is used when a bootstrap was done with incomplete metadata and later needs exact block number/hash/state root alignment.

## `crates/assertion-da/*`

### What it is

The Assertion DA stack is a JSON-RPC service plus client and shared request types:

- `da-core` defines the request/response contract used by both sides.
- `da-client` issues JSON-RPC requests and validates response envelopes.
- `da-server` compiles, stores, signs, and serves assertion artifacts.

Its role is narrow but important: it is the artifact handoff point between assertion authoring and later assertion indexing. The service does not execute assertions. It only turns a Solidity assertion submission into a durable, fetchable artifact bundle.

### Shared request model

`crates/assertion-da/da-core/src/lib.rs` defines three core payloads:

- `DaSubmission`: Solidity source, compiler version, assertion contract name, constructor args, and constructor ABI signature.
- `DaSubmissionResponse`: assertion `id` plus `prover_signature`.
- `DaFetchResponse`: original source, creation bytecode without constructor args, encoded constructor args, constructor ABI signature, and the same prover signature.

The important behavioral detail is that fetch returns `bytecode` and `encoded_constructor_args` separately. Consumers are expected to append the encoded constructor args to the creation bytecode when reconstructing deployment data.

### Client behavior

`da-client` exposes two JSON-RPC methods:

- `da_submit_solidity_assertion`
- `da_get_assertion`

It is intentionally strict:

- it always sends JSON-RPC 2.0 envelopes,
- it checks that the response is JSON-RPC 2.0,
- it verifies that the response `id` matches the request `id`,
- it treats transport failures and malformed envelopes as client errors.

`DaClientError::is_reachable_da_error()` is used by other components, especially `sidecar`'s DA reachability monitor, to distinguish "the DA answered with an application error" from "the DA is unreachable".

### Server startup and process model

`da-server` is configured by `crates/assertion-da/da-server/src/config.rs`.

At startup it:

1. binds a TCP listener from the configured address,
2. connects to Docker,
3. chooses a storage backend,
4. loads the DA signing private key,
5. constructs a `DaServer`.

The storage backend selection is dynamic:

- if `DA_REDIS_URL` is configured and reachable, Redis is used;
- otherwise the server falls back to a local Sled database under the configured or XDG-derived path.

Runtime execution splits into two cooperating tasks:

- an API task that accepts HTTP connections and processes JSON-RPC requests,
- a DB task that listens on an in-process channel and serializes `Get` / `Insert` operations against the chosen backend.

That split keeps the API layer backend-agnostic. API handlers never talk to Sled or Redis directly; they only send `DbOperation`s over a channel and wait for the reply.

### HTTP, readiness, and request validation

The server exposes:

- `/health`
- `/ready`

Readiness is not a trivial always-true probe. It verifies:

- the cancellation token has not already been triggered,
- Docker is reachable,
- the DB task can round-trip a request successfully.

The JSON-RPC path is served over Hyper. Before method dispatch, request processing validates:

- the request body exists and parses as JSON,
- the JSON-RPC version is correct,
- the method shape is valid,
- the body stays under the configured size limit,
- the expected params are present for the selected method.

Invalid envelopes are rejected before they reach any business logic.

### Submission behavior

The primary write path is `da_submit_solidity_assertion`.

Its behavior is:

1. parse `DaSubmission`,
2. compile the Solidity source in a Dockerized `solc`,
3. ABI-encode constructor args using the provided ABI signature,
4. append encoded constructor args to the creation bytecode,
5. compute `keccak256(deployment_data)` as the assertion id,
6. sign that id with the DA private key,
7. persist a `StoredAssertion`,
8. return the id and signature.

This means the DA identity is bound to deployable creation data, not just to source text. Changing constructor args changes the encoded deployment data and therefore changes the assertion id.

### Compilation model

Compilation is delegated to Docker rather than to an in-process Solidity compiler.

`source_compilation.rs` is responsible for:

- selecting the correct `solc` image,
- respecting optional platform overrides,
- ensuring the image exists locally,
- pulling it when necessary,
- creating and later removing the compiler container,
- extracting compilation outputs needed by the API layer.

The operational consequence is that DA readiness and submission success both depend on Docker being healthy and on the selected compiler image being runnable.

### Fetch behavior

The primary read path is `da_get_assertion`.

Given an assertion id, the server returns:

- original Solidity source,
- creation bytecode without constructor args appended,
- ABI-encoded constructor args,
- constructor ABI signature,
- DA prover signature.

Consumers can therefore reproduce the exact deployable creation payload and independently verify the signed assertion id.

### Storage model

`da-server` treats persistence as a key-value store over assertion id.

The currently supported backends are:

- Redis
- Sled

Both are hidden behind the same `Database` trait. All API writes and reads pass through `listen_for_db()`, which processes:

- `DbOperation::Insert`
- `DbOperation::Get`

The important system property is that API correctness should not depend on backend choice. Backend choice changes durability and operations characteristics, but not API semantics.

## `crates/pcl/*`

### What it is

The PCL stack is the user/operator interface to the wider system.

It is split into four crates:

- `pcl/cli`: top-level binary and command routing,
- `pcl/core`: app auth, DA storage, submission, apply, and config persistence,
- `pcl/common`: shared assertion helpers and argument types,
- `pcl/phoundry`: wrappers around Foundry/phoundry build and test flows.

PCL does not replace `sidecar`. It serves a different boundary:

- `sidecar` validates transactions at runtime,
- PCL prepares and publishes assertion artifacts ahead of runtime.

### CLI command model

`crates/pcl/cli/src/main.rs` reads `CliConfig`, dispatches the selected subcommand, and writes config back to disk after successful command execution.

The current top-level commands are:

- `test`
- `store`
- `submit`
- `apply`
- `auth`
- `config`
- `build`

The CLI supports JSON output in error paths, but command implementations are still mostly written as interactive terminal flows.

### Local config model

`crates/pcl/core/src/config.rs` defines `CliConfig`.

The key persisted state is:

- `auth: Option<UserAuth>` — tokens, user ID, optional wallet address, optional email

Users authenticate once and keep tokens locally. The `apply` command then uses these tokens when submitting releases to the app backend.

### Auth flow

`pcl core::auth` implements a device-style login flow against the app backend.

The login path is:

1. request a CLI auth code from the app,
2. open a browser to the verification page with the returned session id,
3. poll the auth-status endpoint until the session is verified,
4. persist access token, refresh token, address, expiration, and optional user metadata in `CliConfig`.

`logout` and `status` operate only on local config state. They do not add separate protocol semantics beyond clearing or inspecting what is already persisted.

### Build and test behavior

`pcl-phoundry` wraps Foundry/phoundry operations instead of reimplementing compiler behavior.

The main roles are:

- `build`: compile contracts from the expected assertion source tree,
- `build_and_flatten`: compile, find the produced artifact, collect ABI and bytecode details, and flatten Solidity source,
- `compile`: enforce source-directory assumptions and invoke Foundry project compilation,
- `phorge_test`: run Foundry/phorge tests in a blocking task.

This means PCL's correctness for local artifact generation depends heavily on Foundry project layout conventions and on phoundry/Foundry being installed correctly.

### Apply flow

The `apply` command consumes declarative project configuration, currently centered around `credible.toml`.

It assembles a release payload by:

- loading the declared contracts, environments, and assertions,
- building and flattening referenced assertion contracts,
- selecting or resolving the target project,
- submitting the resulting release payload to the app backend.

The code shows that preview-oriented behavior is still incomplete or intentionally deferred; the main implemented path is direct release submission.

### Common assertion path handling

`pcl/common` provides small but important normalization helpers.

One example is `Assertion::get_paths()`, which resolves assertion source candidates by trying both:

- `<contract>.a.sol`
- `<contract>.sol`

That fallback is part of current CLI behavior. Agents modifying assertion discovery should preserve it unless they are deliberately changing supported naming conventions.

## Key invariants and limitations

### Invariants worth preserving

- `sidecar` only accepts tx execution after a valid iteration and first commit head.
- `sidecar` treats `CommitHead` as the canonicalization point; before that, iteration state is speculative.
- `VersionDb` commit log depth must stay aligned with tx ordering, including txs that produced no state change.
- `mdbx` readers must verify the namespace still maps to the requested block.
- `mdbx` writer assumes each account appears at most once in a block update.
- `state-worker` system-call writes are merged into traced block updates before commit.
- assertion trigger matching is block-bounded and adopter-bounded.
- inactive assertions are retained until prune horizon, not removed immediately.
- `geth_snapshot` treats parse errors as fatal state-integrity failures.

### Current limitations

- `state-worker` does not implement chain reorg handling.
- `sidecar` relies on driver events and iteration cache invalidation rather than reconstructing historical canonical state locally.
- `MdbxSource` serves only the buffer window available from the state-worker DB.
- `geth_snapshot` bootstraps all namespaces with the same block on initial hydrate; that is expected, not corruption.
- assertion indexing currently depends on an external event source plus DA source being reachable and consistent.
- Assertion DA submission depends on Dockerized Solidity compilation, so Docker health is part of the effective control plane.

## Where to start when modifying behavior

If changing tx execution semantics:

- start in `crates/sidecar/src/engine/mod.rs`
- then inspect `crates/assertion-executor/src/executor/mod.rs`

If changing state-source behavior:

- start in `crates/sidecar/src/cache/mod.rs`
- then inspect `crates/sidecar/src/cache/sources/mdbx/mod.rs`
- and `crates/sidecar/src/cache/sources/eth_rpc_source.rs`

If changing persisted state format or retention behavior:

- start in `crates/mdbx/src/writer.rs`
- and `crates/mdbx/src/reader.rs`

If changing ingestion correctness:

- start in `crates/state-worker/src/worker.rs`
- `crates/state-worker/src/state/geth.rs`
- `crates/state-worker/src/system_calls.rs`

If changing snapshot hydration:

- start in `scripts/geth_snapshot/src/main.rs`

If changing Assertion DA behavior:

- start in `crates/assertion-da/da-server/src/api/mod.rs`
- then inspect `crates/assertion-da/da-server/src/api/assertion_submission.rs`
- and `crates/assertion-da/da-server/src/source_compilation.rs`
- plus `crates/assertion-da/da-client/src/lib.rs`

If changing PCL workflows:

- start in `crates/pcl/cli/src/main.rs`
- then inspect `crates/pcl/core/src/apply.rs`
- `crates/pcl/core/src/auth.rs`
- and `crates/pcl/core/src/config.rs`
