# Architecture

**Analysis Date:** 2026-03-25

## System Overview

Credible SDK is the Phylax Credible Layer -- a blockchain security system that runs alongside Ethereum execution clients (sidecars) to validate transactions against user-defined "assertions" (EVM smart contracts that express invariants). The system intercepts transactions from a sequencer/builder, re-executes them in a shadow EVM, runs assertions in parallel, and reports violations (incidents) to external APIs. It also provides a CLI (`pcl`) for developers to write, test, store, and deploy assertions.

## Architecture Pattern

**Overall:** Modular monorepo / multi-binary system

The project is a Cargo workspace producing **5 independent binaries** and **~20 library crates**. The binaries operate as separate long-running services that communicate via gRPC, HTTP/JSON-RPC, WebSocket subscriptions, and shared MDBX databases on disk.

## Component Map

```
                                 +-----------------+
                                 |   Sequencer /   |
                                 |   Block Builder |
                                 +-------+---------+
                                         |
                                    gRPC | (StreamEvents)
                                         |
+------------------+             +-------v---------+            +------------------+
|   state-worker   |  MDBX(r/w) |                  |  HTTP/RPC  | assertion-da     |
|  (syncs chain    +<----------->    sidecar       +<---------->| (DA server)      |
|   state to MDBX) |            |  (core engine)   |            | (stores/compiles |
+------------------+            |                  |            |  assertion code) |
                                +---+---------+----+            +------------------+
                                    |         |
                          HTTP/REST |         | gRPC (results)
                                    |         |
                          +---------v--+  +---v--------------+
                          | Dapp API   |  | shadow-driver    |
                          | (incident  |  | (WS listener ->  |
                          |  reports)  |  |  gRPC -> sidecar)|
                          +------------+  +------------------+

+------------------+            +------------------+
|   pcl (CLI)      |  HTTP/RPC  | verifier-service |
|  (test, store,   +----------->| (validates       |
|   submit, apply) |            |  assertion code) |
+------------------+            +------------------+
```

## Key Modules

| Module | Responsibility | Dependencies |
|--------|---------------|--------------|
| `sidecar` | Main runtime: receives txs via gRPC, sequences events, executes txs in shadow EVM, validates assertions, reports incidents | `assertion-executor`, `assertion-da-client`, `dapp-api-client`, `mdbx`, `eip-system-calls`, `credible-utils` |
| `assertion-executor` | Core EVM execution engine: deploys assertion contracts, executes transactions, runs assertions in parallel via rayon, manages fork/overlay DBs | `assertion-da-client`, `revm`, `alloy`, `rayon` |
| `assertion-da/da-server` | Data Availability server: stores/retrieves assertion Solidity source + bytecode, compiles via Docker solc, signs artifacts | `assertion-da-core`, `pcl-phoundry`, `sled`, `redis`, `bollard` |
| `assertion-da/da-client` | HTTP client for DA server JSON-RPC API | `assertion-da-core`, `reqwest` |
| `assertion-da/da-core` | Shared types for DA client/server (request/response models) | `alloy`, `serde` |
| `pcl/cli` | CLI binary (`pcl`): test, store, submit, apply, auth, config, build commands | `pcl-core`, `pcl-phoundry`, `pcl-common` |
| `pcl/core` | CLI business logic: assertion submission to dapp, DA storage, auth, apply workflow | `pcl-phoundry`, `pcl-common`, `assertion-da-client` |
| `pcl/phoundry` | Foundry/Phoundry integration: builds and tests assertion contracts using the Phylax fork of Foundry | `forge`, `foundry-config`, `foundry-cli`, `foundry-common` |
| `pcl/common` | Shared CLI argument types | `clap`, `serde_json` |
| `shadow-driver` | Standalone binary: subscribes to execution client `newHeads`, replays blocks by streaming txs to sidecar via gRPC, queries results | `sidecar` (proto types), `mdbx`, `credible-utils` |
| `state-worker` | Standalone binary: subscribes to execution client `newHeads`, traces blocks via `prestateTracer`, writes state diffs to MDBX circular buffer | `mdbx`, `eip-system-calls`, `credible-utils` |
| `mdbx` | MDBX database abstraction: circular buffer for blockchain state, `Reader`/`Writer` traits, reth-compatible encoding | `reth-db`, `reth-libmdbx`, `reth-codecs` |
| `state-checker` | Validates MDBX state consistency against on-chain state root via Merkle trie | `mdbx`, `alloy-trie`, `revm` |
| `verifier-service` | HTTP microservice: accepts assertion bytecode, verifies deployability and trigger registration | `assertion-verification`, `assertion-executor`, `axum` |
| `assertion-verification` | Pure library: verifies assertion bytecode can deploy, register triggers, and be stored | `assertion-executor` |
| `dapp-api-client` | Auto-generated OpenAPI client for the Phylax dapp API (progenitor-generated) | `progenitor-client`, `reqwest` |
| `credible-utils` | Shared utilities: `critical!` macro, shutdown signal handling | `tokio`, `tracing` |
| `eip-system-calls` | Shared EIP system call constants (beacon root, history storage, consolidation, etc.) | `alloy-eips`, `alloy-primitives` |
| `benchmark-utils` | ERC20/UniswapV3 test fixtures for benchmarking assertion execution | -- |
| `int-test-utils` | Integration test helpers: contract deployment, DA deployment, mock servers | -- |

## Data Flow

### Transaction Validation (Sidecar Core Loop)

1. **External Driver** (sequencer or `shadow-driver`) sends transactions and `BlockEnv` via gRPC `StreamEvents` bidirectional stream to the sidecar
2. **GrpcTransport** (`crates/sidecar/src/transport/grpc/`) receives proto messages, converts to `TxQueueContents`, sends to EventSequencing via flume channel
3. **EventSequencing** (`crates/sidecar/src/event_sequencing/mod.rs`) runs on a dedicated OS thread; orders events by block number and iteration ID, handles reorgs, sends ordered events to CoreEngine via flume channel
4. **CoreEngine** (`crates/sidecar/src/engine/mod.rs`) runs on a dedicated OS thread; processes `NewIteration`, `Transaction`, `Reorg`, and `CommitHead` events; executes each tx in the shadow EVM using `AssertionExecutor`
5. **AssertionExecutor** (`crates/assertion-executor/src/executor/mod.rs`) executes the transaction via `revm`, then runs all registered assertion functions **in parallel** using a rayon thread pool against `ForkDb` snapshots
6. **AssertionStore** (`crates/assertion-executor/src/store/`) provides the assertion bytecode and trigger mappings; populated by the **Indexer**
7. **TransactionsState** (`crates/sidecar/src/transactions_state.rs`) records validation results (pass/fail) and streams them back to the driver via `SubscribeResults` gRPC stream
8. **TransactionObserver** (`crates/sidecar/src/transaction_observer/mod.rs`) runs on a dedicated OS thread; receives `IncidentReport`s from CoreEngine, persists to sled DB, publishes to Dapp API and Aeges

### Assertion Indexing (Sidecar Background)

1. **Indexer** (`crates/sidecar/src/indexer.rs`) polls a GraphQL event source for `AssertionAdded`/`AssertionRemoved` on-chain events
2. For added assertions, fetches bytecode from DA server via `DaClient`
3. Extracts assertion contracts using `extract_assertion_contract()` from `assertion-executor`
4. Applies modifications to the in-memory `AssertionStore`

### State Synchronization (state-worker)

1. **state-worker** binary connects to execution client via WebSocket
2. Subscribes to `newHeads` and traces each block using `prestateTracer` in diff mode
3. Writes account state diffs into MDBX circular buffer via `Writer::commit_block()`
4. Sidecar reads this state via `MdbxSource` implementing `Source` trait -> `DatabaseRef`

### CLI Workflow (pcl)

1. `pcl test` -- uses `pcl-phoundry` to compile and run assertion tests via the Phylax fork of Foundry
2. `pcl store` -- compiles assertion, submits Solidity source to DA server via `DaClient`
3. `pcl submit` -- submits assertion metadata to the Dapp API via `dapp-api-client`
4. `pcl apply` -- reads `credible.toml` config, runs store+submit in sequence

## Key Abstractions

**`Transport` trait** (`crates/sidecar/src/transport/mod.rs`):
- Purpose: Abstraction for sidecar-to-driver communication
- Implementations: `GrpcTransport` (production), `MockTransport` (testing)
- Methods: `new()`, `run()`, `stop()`

**`Source` trait** (`crates/sidecar/src/cache/sources/mod.rs`):
- Purpose: Blockchain state data provider for the sidecar cache layer
- Implementations: `MdbxSource` (reads from MDBX written by state-worker), `EthRpcSource` (queries execution client directly via JSON-RPC)
- Key: Must implement `DatabaseRef` (revm) + sync awareness

**`Reader` / `Writer` traits** (`crates/mdbx/src/lib.rs`):
- Purpose: Abstract storage backend for blockchain state in circular buffer
- Implementations: `StateWriter` (combined reader+writer), `StateReader` (read-only)
- Pattern: Circular buffer with configurable depth; automatic namespace rotation

**`EventSource` trait** (`crates/assertion-executor/src/store/event_source.rs`):
- Purpose: Provides assertion add/remove events from an external indexer
- Implementations: `GraphqlEventSource` (HTTP/GraphQL polling)

**`AssertionExecutor`** (`crates/assertion-executor/src/executor/mod.rs`):
- Purpose: Core EVM execution engine; executes transactions and validates assertions
- Pattern: Uses rayon thread pool to execute assertions in parallel via `ForkDb` snapshots
- Key types: `ExecutorConfig`, `TxValidationResult`, `AssertionContract`

**`OverlayDb<DB>`** (`crates/assertion-executor/src/db/overlay/mod.rs`):
- Purpose: In-memory LFU cache layered on top of a `DatabaseRef` backend (e.g., `Sources`)
- Pattern: `Buffer -> TinyLFU Hashmap -> Disk`

**`VersionDb<DB>`** (`crates/assertion-executor/src/db/version_db.rs`):
- Purpose: Versioned database with rollback support for block iteration state management

**`Database` trait / `db` module** (`crates/assertion-executor/src/db/`):
- Purpose: Database abstractions for EVM state (wraps revm's `Database`/`DatabaseRef`)
- Includes: `ForkDb` (snapshot-based fork for parallel reads), `MultiForkDb` (multiple forks), `OverlayDb`, `VersionDb`

## Entry Points

**`sidecar` binary:**
- Location: `crates/sidecar/src/main.rs`
- Triggers: Run directly as the main credible layer sidecar service
- Responsibilities: Orchestrates transport, event sequencing, core engine, indexer, health server, transaction observer, DA reachability monitor

**`assertion-da` binary:**
- Location: `crates/assertion-da/da-server/bin/assertion-da.rs`
- Triggers: Run as the Data Availability service
- Responsibilities: HTTP JSON-RPC server for storing/retrieving assertion source code and compiled bytecode

**`state-worker` binary:**
- Location: `crates/state-worker/src/main.rs`
- Triggers: Run alongside an execution client
- Responsibilities: Traces blocks and writes state diffs to MDBX circular buffer

**`shadow-driver` binary:**
- Location: `crates/shadow-driver/src/main.rs`
- Triggers: Run to drive the sidecar from an execution client's block stream
- Responsibilities: Listens for new blocks via WebSocket, streams transactions to sidecar via gRPC

**`pcl` binary:**
- Location: `crates/pcl/cli/src/main.rs`
- Triggers: Developer CLI tool
- Responsibilities: Test, build, store, submit, and apply assertions

**`verifier-service` binary:**
- Location: `crates/verifier-service/src/main.rs`
- Triggers: Run as an HTTP microservice
- Responsibilities: Validates assertion bytecode can be deployed and run

## Error Handling

**Strategy:** Typed error enums with `thiserror` + recoverability classification

**Patterns:**
- Each major component defines its own error enum (e.g., `EngineError`, `EventSequencingError`, `TransactionObserverError`, `IndexerError`)
- All component errors implement `From<&Error> for ErrorRecoverability` to classify errors as `Recoverable` or `Unrecoverable`
- Recoverable errors trigger a sidecar restart loop (`run_sidecar_once()` in `main.rs`); unrecoverable errors use the `critical!` macro for alerting
- The workspace-level Clippy lints **deny**: `panic`, `unwrap_used`, `expect_used`, `unreachable`, `todo`, `unimplemented`, `indexing_slicing` -- enforcing explicit error handling everywhere
- `anyhow::Result` is used in binary entry points and integration tests; `thiserror` for library error types
- `color-eyre` is used in the `pcl` CLI for user-friendly error display

## State Management

**Sidecar in-memory state:**
- `OverlayDb<Sources>`: LFU cache over external state sources; lives for the duration of one `run_sidecar_once()` iteration
- `VersionDb<OverlayDb<Sources>>`: Per-block-iteration versioned state with rollback support (one per `BlockIterationData`)
- `AssertionStore`: Persistent sled database mapping assertion IDs to contracts/triggers
- `TransactionsState`: In-memory TTL cache of transaction validation results; shared between engine and transport

**MDBX circular buffer (state-worker / mdbx crate):**
- Circular buffer with configurable depth (number of historical blocks)
- Each "namespace" in the buffer holds a complete snapshot of relevant account state
- Rotation writes state diffs during namespace eviction to reconstruct intermediate states
- Both `state-worker` (writer) and `sidecar` (reader via `MdbxSource`) access the same MDBX database on disk

**Assertion store (sled):**
- Persistent key-value store (`crates/assertion-executor/src/store/assertion_store.rs`)
- Keys: `(adopter_address, assertion_id)` -> `AssertionState` containing contract bytecode, triggers, and activation/inactivation blocks

## Threading Model

- **sidecar**: Main tokio runtime for async components (transport, health server, indexer, DA monitor). Three dedicated OS threads for blocking pipelines: EventSequencing, CoreEngine, TransactionObserver. Rayon thread pool for parallel assertion execution
- **state-worker**: Main tokio runtime for WebSocket subscription and RPC calls
- **shadow-driver**: Main tokio runtime for WS subscription + gRPC client
- **assertion-da**: Main tokio runtime for HTTP server

## Cross-Cutting Concerns

**Logging:** `tracing` crate with custom `rust-tracing` subscriber from Phylax's shared crate (git dependency). Structured logging with span-based instrumentation. `critical!` macro (from `credible-utils`) for unrecoverable errors.

**Metrics:** `metrics` crate (0.24) for counters, gauges, and histograms throughout the sidecar, engine, transport, transaction observer, and indexer.

**Configuration:** Sidecar uses a layered config system: JSON config file + CLI args (via `clap`) + env vars. Default config embedded at compile time via `include_str!`. PCL CLI uses `CliConfig` read/written to a local TOML file.

**Serialization:** `serde`/`serde_json` for JSON serialization throughout. `bincode` for compact binary serialization (incident persistence, assertion store). `prost`/`tonic` for protobuf/gRPC. `progenitor` for OpenAPI client generation.

**Feature flags:**
- `optimism`: Enables OP Stack support via `op-revm` in `assertion-executor`
- `test`: Enables test utilities and `ctor`-based test infrastructure
- `cache_validation`: Enables cache checker using trace API
- `tokio-console`: Enables tokio-console and tokio-metrics instrumentation

---

*Architecture analysis: 2026-03-25*
