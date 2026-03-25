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

- `state-worker` and `geth_snapshot` produce state.
- `mdbx` stores and serves state.
- `sidecar` is the transaction admission and orchestration process.
- `assertion-executor` is the EVM/assertion engine used by `sidecar`.
- `assertion-da` is the durable assertion-artifact service used by both indexing and authoring flows.
- `pcl` is the user-facing CLI layer that packages local assertion code and pushes it into the DA/app ecosystem.

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

## Cross-component data flow

### State path

1. `state-worker` traces each block with Geth `prestateTracer` in diff mode.
2. It converts per-tx trace output into a per-block `mdbx::BlockStateUpdate`.
3. It merges EIP-2935 and EIP-4788 system-contract state changes into that update.
4. It commits the update via `mdbx::StateWriter`.
5. `sidecar` reads this state through `StateReader`, wrapped as a `MdbxSource`.

### Transaction validation path

1. An external driver sends `CommitHead`, `NewIteration`, `Tx`, and `Reorg` events to `sidecar` gRPC.
2. gRPC transport forwards events into the internal queue.
3. `EventSequencing` enforces ordering and dependency checks before the engine sees them.
4. `CoreEngine` builds per-iteration `VersionDb` state on top of an `OverlayDb<Sources>`.
5. For each tx, `AssertionExecutor` executes the tx and runs triggered assertions.
6. The engine stores a `TransactionResult`, emits optional incident reports, and later commits or rolls back the iteration based on `CommitHead` or `Reorg`.

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
