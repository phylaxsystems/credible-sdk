# Assertion Executor

This file covers `crates/assertion-executor`.

## Transaction validation schema

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

## What it is

`assertion-executor` is the EVM execution and assertion-validation library used by `sidecar`.

Relevant subareas:

- executor,
- assertion store,
- DB layering (`OverlayDb`, `ForkDb`, `VersionDb`, `MultiForkDb`),
- inspectors and precompiles.

## Validation model

For a tx, the executor:

1. executes the user tx against a fork DB while collecting a `CallTracer`,
2. if tx execution itself is not successful, skips assertions and returns valid-without-assertions,
3. derives triggers from the call tracer,
4. queries the assertion store for matching active assertions for the tx block,
5. executes matched assertion functions in parallel,
6. aggregates failures and builds `TxValidationResult`.

Important behavior:

- only successful tx execution leads to assertion execution,
- assertion contracts are treated as read-only checks over forked state,
- if any executed assertion function fails, the tx is marked invalid,
- tx state may still be committed into the fork DB depending on caller behavior.

## Assertion execution mechanics

For each assertion contract:

1. persistent accounts from the assertion contract are inserted into the tx fork DB,
2. a `MultiForkDb` is created from the tx fork DB plus the post-tx journal,
3. each assertion function selector is executed in parallel on a rayon pool,
4. each function runs as a call to `ASSERTION_CONTRACT`,
5. phevm inspector state and console logs are captured,
6. gas, selector, and failure data are aggregated.

The parallelism is per assertion function, not per transaction.

## Trigger selection

The assertion store stores, per assertion:

- activation block,
- optional inactivation block,
- extracted deployed assertion contract,
- recorded triggers,
- assertion spec.

At runtime:

- the call tracer exposes triggers grouped by adopter,
- the store filters assertions active at the current block,
- matching selectors are the union of:
  - exact trigger matches,
  - `AllCalls` if any call trigger matched,
  - `AllStorageChanges` if any storage-change trigger matched.

That trigger expansion behavior is the real contract agents need to preserve.

## Assertion extraction

When indexing a newly added assertion:

1. deployment bytecode is executed in an in-memory DB,
2. the deployed assertion contract is recovered from the post-state account at `ASSERTION_CONTRACT`,
3. the `triggers()` function is called against a trigger recorder account,
4. trigger recorder output becomes persisted matching metadata,
5. spec recorder output becomes the assertion spec.

If extraction fails, the sidecar indexer skips that assertion event rather than crashing the entire index cycle.

## Assertion store behavior

The store supports:

- an in-memory backend,
- a sled backend.

It groups assertions by adopter address.

Modification model:

- `PendingModification::Add` adds or reactivates an assertion,
- `PendingModification::Remove` sets `inactivation_block`.

Pruning model:

- inactive assertions are not removed immediately,
- an expiry index keyed by `(inactivation_block, adopter, assertion_id)` is maintained,
- a background task prunes assertions when `current_block > retention_blocks`,
- pruning removes assertions whose inactivation block is older than the retention horizon.

That retention window exists to tolerate reorg-style needs in upstream indexing.

## DB layering

### `OverlayDb`

Shared in-memory cache over an underlying `DatabaseRef`.

Used by sidecar as the long-lived state cache.

Properties:

- shared `DashMap`,
- caches account, storage, code, and block hash entries,
- can be invalidated wholesale,
- can spawn a monitoring metrics task,
- can create `ForkDb`s and `ActiveOverlay`s.

### `ForkDb`

Per-execution mutable overlay over another DB.

Used for:

- tx execution on top of overlay or canonical state,
- assertion execution forks.

It stores account, storage, and code mutations and falls back to the inner DB on misses.

### `VersionDb`

Adds rollback semantics on top of `ForkDb`.

Used by sidecar per iteration.

Key behavior:

- keeps `base_state`,
- keeps live `state`,
- records the commit log as `Vec<Option<EvmState>>`,
- `None` entries represent failed tx slots with no state changes,
- `rollback_to(depth)` rebuilds live state by replaying the log from base state.

The `None`-slot design matters because reorg depth is defined over tx positions, not just stateful commits.

### `MultiForkDb`

Manages multiple logical forks for assertion execution.

Used when assertions or cheatcodes need pre and post-tx plus pre and post-call fork views.

It can switch between forks and preserve persistent accounts across fork transitions.
