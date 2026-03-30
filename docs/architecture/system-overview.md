# System Overview

This file is the top-level map of the runtime. Read this first if you need the system topology or the data flow across components.

## Scope

This architecture split covers:

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
- `scripts/geth_snapshot`

## High-level split

- `geth_snapshot` bootstraps durable state into MDBX.
- `sidecar` is the only supported production runtime and hosts the transaction engine, transport, indexer, health endpoints, and the integrated state-worker supervisor.
- `mdbx` stores and serves state.
- `assertion-executor` is the EVM/assertion engine used by `sidecar`.
- `assertion-da` is the durable assertion-artifact service used by both indexing and authoring flows.
- `pcl` is the user-facing CLI layer that packages local assertion code and deploys it via declarative releases.
- `state-worker` remains available as a standalone bootstrap/recovery utility, not as a separate production sidecar companion.

## End-to-end system schema

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
        | live geth dump / JSONL                       | durable committed window
        |                                              |
  +-------------------+    debug_trace / newHeads      |
  | execution client  | -------------------------------+
  |   (Geth / node)   |                                |
  +-------------------+                                |
           ^                                           |
           |                                           |
           |                                  +--------------------+
           |                                  |      sidecar       |
           |                                  | transport + engine |
           |                                  | + worker supervisor|
           |                                  +--------------------+
           |                                            |
           |                                            +--> traced block buffer
           |                                            |
           |                                            +--> CommitHead grants flush permission
           |                                            |
           |                                            +--> mdbx::Writer::commit_block()
           |                                            |
           |                                            v
           |                                  +--------------------+
           +--------------------------------> | sidecar MdbxSource |
                                              +--------------------+
                                                         |
                                                         | validate tx / fallback
                                                         v
                                                +--------------------+
                                                | assertion-executor |
                                                | tx + assertions    |
                                                +--------------------+
```

## Cross-component data flow

### State path

1. The integrated worker inside `sidecar` traces each block with Geth `prestateTracer` in diff mode and materializes a per-block `mdbx::BlockStateUpdate`.
2. That update is kept in memory as `traced` state until `CoreEngine::process_commit_head` has accepted the corresponding block and advanced the flush-permitted head.
3. After flush permission is granted, the integrated worker durably writes the block through `mdbx::StateWriter::commit_block`.
4. MDBX visibility is authoritative only after that durable write succeeds. Unflushed traced state is explicitly ephemeral and is discarded on restart.
5. `sidecar` reads MDBX through `StateReader`, wrapped as `MdbxSource`, and falls back to `EthRpcSource` whenever the durable MDBX window no longer covers the cache's required range.

### Transaction validation path

1. An external driver sends `CommitHead`, `NewIteration`, `Tx`, and `Reorg` events to `sidecar` gRPC.
2. gRPC transport forwards events into the internal queue.
3. `EventSequencing` enforces ordering and dependency checks before the engine sees them.
4. `CoreEngine` builds per-iteration `VersionDb` state on top of an `OverlayDb<Sources>`.
5. For each tx, `AssertionExecutor` executes the tx and runs triggered assertions.
6. The engine stores a `TransactionResult`, emits optional incident reports, and later commits or rolls back the iteration based on `CommitHead` or `Reorg`.

### Durable visibility lifecycle

The integrated runtime treats MDBX visibility as a five-state lifecycle:

1. `traced`: the worker has produced a `BlockStateUpdate` in memory, but nothing is readable from MDBX yet.
2. `flush-permitted`: `CommitHead` has validated the block and granted permission for that block to become durable.
3. `durably flushed`: `mdbx::Writer::commit_block` has succeeded, so the committed visibility window advances and MDBX becomes the authoritative source for that block.
4. `worker-behind`: the durable MDBX window exists, but it no longer covers the range the sidecar cache requires.
5. `fallback/return`: `Sources` serves reads from `EthRpcSource` while MDBX is behind, then returns to `MdbxSource` automatically once the durable window catches back up.

### Assertion lifecycle path

1. `sidecar::indexer` polls a GraphQL event source for added and removed assertion events.
2. For added events it fetches source and deployment data from Assertion DA, extracts the deployed assertion contract plus recorded triggers, and creates `PendingModification::Add`.
3. For removed events it creates `PendingModification::Remove`.
4. `AssertionStore` applies those modifications per adopter and maintains an expiry index for later pruning.
5. During tx execution, the call tracer produces triggers and the store returns matching active assertions for that adopter and block.

## Other docs

- Read `state-pipeline.md` for state ingestion and storage internals.
- Read `sidecar.md` for event sequencing, execution orchestration, and indexing.
- Read `assertion-executor.md` for tx validation and DB layering.
- Read `assertion-da.md` for artifact submission and fetch behavior.
- Read `pcl.md` for CLI/operator flows.
