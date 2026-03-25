# Project Structure

**Analysis Date:** 2026-03-25

## Directory Layout

```
credible-sdk/
├── Cargo.toml                  # Workspace manifest (all crates, shared deps, profiles, lints)
├── Cargo.lock                  # Lockfile
├── Makefile                    # Build, test, lint, docker, regenerate targets
├── deny.toml                   # cargo-deny configuration
├── rust-toolchain.toml         # Rust edition 2024, nightly channel
├── rustfmt.toml                # Formatting rules
├── flake.nix / flake.lock      # Nix flake for reproducible dev environment
├── LICENSE                     # BSL 1.1
├── README.md                   # Project documentation
├── SECURITY.md                 # Security contact info
├── crates/                     # All Rust crates (binaries + libraries)
│   ├── sidecar/                # MAIN BINARY: credible layer sidecar
│   ├── assertion-executor/     # LIB: core EVM execution + assertion engine
│   ├── assertion-da/           # DA subsystem
│   │   ├── da-server/          # BINARY: assertion Data Availability server
│   │   ├── da-client/          # LIB: HTTP client for DA server
│   │   └── da-core/            # LIB: shared DA types
│   ├── pcl/                    # PCL CLI subsystem
│   │   ├── cli/                # BINARY: pcl CLI entry point
│   │   ├── core/               # LIB: CLI business logic
│   │   ├── phoundry/           # LIB: Foundry/Phoundry integration
│   │   └── common/             # LIB: shared CLI args
│   ├── shadow-driver/          # BINARY: block listener -> sidecar gRPC driver
│   ├── state-worker/           # BINARY: chain state tracer -> MDBX writer
│   ├── state-checker/          # BINARY: MDBX state root verifier
│   ├── mdbx/                   # LIB: MDBX circular buffer abstraction
│   ├── verifier-service/       # BINARY: assertion verification HTTP service
│   ├── assertion-verification/ # LIB: assertion verification logic
│   ├── dapp-api-client/        # LIB: auto-generated Dapp API client
│   ├── credible-utils/         # LIB: shared macros + shutdown utils
│   ├── eip-system-calls/       # LIB: EIP system call constants
│   ├── benchmark-utils/        # LIB: ERC20/UniswapV3 test fixtures
│   └── int-test-utils/         # LIB: integration test helpers
├── docker/                     # Docker compose files for full deployments
│   ├── maru-besu-sidecar/      # Besu + sidecar compose setup
│   └── shovel/                 # Shovel indexer compose setup
├── dockerfile/                 # Dockerfiles for each binary
│   ├── Dockerfile.sidecar
│   ├── Dockerfile.sidecar-debug
│   ├── Dockerfile.sidecar.host
│   ├── Dockerfile.da
│   ├── Dockerfile.shadow-driver
│   ├── Dockerfile.state-worker
│   ├── Dockerfile.state-checker
│   └── Dockerfile.verifier-service
├── etc/                        # Runtime configuration
│   ├── docker-compose.yaml
│   ├── docker-compose-dev.yaml
│   └── genesis/
├── testdata/                   # Test fixtures
│   └── mock-protocol/          # Foundry project with mock Solidity assertions
├── lib/                        # Git submodules
│   └── credible-layer-contracts/
├── phorge/                     # Phoundry configuration
│   └── config.toml
├── examples/
│   └── test_http_client.rs
├── fuzz/                       # Fuzz testing (excluded from workspace)
├── scripts/                    # Build/run scripts
│   ├── geth_snapshot/
│   ├── run-sidecar-host.sh
│   └── test-no-full.sh
└── .github/
    ├── workflows/
    ├── ISSUE_TEMPLATE/
    └── CODEOWNERS
```

## Key Directories

| Directory | Purpose | Key Files |
|-----------|---------|-----------|
| `crates/sidecar/` | Main sidecar binary and library | `src/main.rs`, `src/lib.rs`, `src/engine/mod.rs`, `src/transport/grpc/sidecar.proto` |
| `crates/assertion-executor/` | Core assertion execution library | `src/lib.rs`, `src/executor/mod.rs`, `src/store/assertion_store.rs`, `src/db/overlay/mod.rs` |
| `crates/assertion-da/da-server/` | DA server binary | `bin/assertion-da.rs`, `src/server.rs`, `src/api/` |
| `crates/assertion-da/da-client/` | DA client library | `src/lib.rs` |
| `crates/pcl/cli/` | PCL CLI binary | `src/main.rs`, `src/cli.rs` |
| `crates/pcl/core/` | PCL business logic | `src/lib.rs`, `src/apply.rs`, `src/assertion_da.rs`, `src/assertion_submission.rs`, `src/auth.rs` |
| `crates/pcl/phoundry/` | Foundry integration | Wraps `forge`, `foundry-config`, `foundry-cli` from Phylax Phoundry fork |
| `crates/shadow-driver/` | Shadow driver binary | `src/main.rs`, `src/listener.rs`, `src/mdbx_store.rs` |
| `crates/state-worker/` | State worker binary | `src/main.rs`, `src/worker.rs`, `src/state/` |
| `crates/mdbx/` | MDBX abstraction library | `src/lib.rs`, `src/reader.rs`, `src/writer.rs`, `src/db.rs` |
| `crates/verifier-service/` | Verifier HTTP service | `src/main.rs`, `src/rpc.rs` |
| `crates/dapp-api-client/` | Generated API client | `src/generated/` (auto-generated), `build.rs` |
| `dockerfile/` | Container images for all binaries | One Dockerfile per binary |
| `testdata/mock-protocol/` | Foundry project with test assertions | Solidity contracts + Forge artifacts |

## Module Organization

### Dependency Hierarchy (top-down)

```
Binaries (top level):
  sidecar ─────────┬─> assertion-executor ──┬─> assertion-da-client ──> assertion-da-core
                    ├─> assertion-da-client  │
                    ├─> dapp-api-client      ├─> revm, alloy
                    ├─> mdbx                 └─> rayon, sled
                    ├─> eip-system-calls
                    └─> credible-utils

  shadow-driver ───┬─> sidecar (lib)
                   ├─> mdbx
                   └─> credible-utils

  state-worker ────┬─> mdbx (writer)
                   ├─> eip-system-calls
                   └─> credible-utils

  assertion-da ────┬─> assertion-da-core
                   ├─> pcl-phoundry
                   └─> sled, redis, bollard

  pcl (cli) ───────┬─> pcl-core ──┬─> pcl-phoundry
                   ├─> pcl-common │  ├─> pcl-common
                   └─> pcl-phoundry  └─> assertion-da-client

  verifier-service ─> assertion-verification ──> assertion-executor

Shared foundations:
  credible-utils     (macros, shutdown)
  eip-system-calls   (EIP constants)
  assertion-da-core  (DA types)
  pcl-common         (CLI args)
  int-test-utils     (test infra)
  benchmark-utils    (bench fixtures)
```

### Sidecar Internal Architecture

```
sidecar/src/
├── main.rs                    # Binary entry: config, service orchestration, restart loop
├── lib.rs                     # Public API: re-exports CoreEngine, Sources, TransactionsState
├── args/                      # Configuration system
│   ├── mod.rs                 # Unified Config struct (JSON file + CLI + env)
│   └── cli.rs                 # CLI arg definitions (clap)
├── config.rs                  # Init helpers for executor, assertion store, indexer
├── transport/                 # External communication layer
│   ├── mod.rs                 # Transport trait definition
│   ├── grpc/                  # gRPC transport implementation
│   │   ├── mod.rs             # GrpcTransport, feature docs
│   │   ├── server.rs          # tonic service impl
│   │   ├── config.rs          # gRPC config
│   │   └── sidecar.proto      # Protobuf definitions
│   ├── mock.rs                # Mock transport for testing
│   ├── event_id_deduplication.rs
│   └── rpc_metrics.rs
├── event_sequencing/          # Event ordering pipeline
│   ├── mod.rs                 # EventSequencing: orders events, handles reorgs
│   ├── event_metadata/        # Event metadata types
│   └── tests.rs
├── engine/                    # Core execution engine
│   ├── mod.rs                 # CoreEngine: block building, tx execution, assertion validation
│   ├── queue.rs               # TxQueueContents: NewIteration, Transaction, Reorg, CommitHead
│   ├── transactions_results.rs# TransactionsResults: per-block result tracking
│   ├── system_calls.rs        # EIP system calls (beacon root, etc.)
│   ├── monitoring/            # Source health monitoring
│   └── tests.rs
├── cache/                     # State caching layer
│   ├── mod.rs                 # Sources: multi-source fallback cache
│   └── sources/               # Individual source implementations
│       ├── mod.rs             # Source trait
│       ├── mdbx/              # MDBX-backed source
│       ├── eth_rpc_source.rs  # Direct JSON-RPC source
│       └── json_rpc_db.rs     # JSON-RPC -> DatabaseRef adapter
├── indexer.rs                 # AssertionIndexer: polls events, fetches DA, updates store
├── graphql_event_source.rs    # GraphQL event source impl for indexer
├── transaction_observer/      # Incident reporting
│   ├── mod.rs                 # TransactionObserver: persists + publishes incidents
│   ├── client.rs              # Dapp API + Aeges HTTP clients
│   ├── db.rs                  # sled-based incident persistence
│   ├── payload.rs             # Incident payload builder
│   └── tests.rs
├── transactions_state.rs      # TransactionsState: TTL result cache
├── da_reachability.rs         # DA health monitor
├── execution_ids.rs           # Block/Tx execution ID types
├── health.rs                  # Health server (axum)
├── metrics.rs                 # Metrics definitions
└── utils/                     # Utilities
    ├── mod.rs                 # ErrorRecoverability enum
    ├── instance.rs            # Instance metadata
    ├── local_instance_db.rs   # Test-only local state DB
    ├── profiling.rs           # dhat profiling support
    ├── test_util.rs           # Test utilities
    ├── test_drivers.rs        # Test driver helpers
    └── test-macros/           # Proc macro crate for test setup
```

## Entry Points

| Binary | Entry Point | Config | Purpose |
|--------|------------|--------|---------|
| `sidecar` | `crates/sidecar/src/main.rs` | JSON config + CLI + env vars | Main credible layer service |
| `assertion-da` | `crates/assertion-da/da-server/bin/assertion-da.rs` | CLI args (clap) | Data Availability server |
| `state-worker` | `crates/state-worker/src/main.rs` | CLI args (clap) | Chain state sync to MDBX |
| `shadow-driver` | `crates/shadow-driver/src/main.rs` | CLI args (clap) | Block replay driver for sidecar |
| `pcl` | `crates/pcl/cli/src/main.rs` | CLI args + `~/.pcl/config.toml` | Developer CLI tool |
| `verifier-service` | `crates/verifier-service/src/main.rs` | CLI args (clap) | Assertion verification API |

## Configuration Files

| File | Purpose |
|------|---------|
| `Cargo.toml` | Workspace manifest: members, shared dependencies, lints, build profiles |
| `rust-toolchain.toml` | Rust toolchain pinning (edition 2024) |
| `rustfmt.toml` | Formatting rules |
| `deny.toml` | cargo-deny license/advisory/bans configuration |
| `Makefile` | Build, test, lint, docker, pre-commit targets |
| `flake.nix` | Nix dev environment |
| `phorge/config.toml` | Phoundry-specific configuration |
| `crates/sidecar/default_config.json` | Default sidecar configuration (embedded at compile time) |
| `crates/sidecar/src/transport/grpc/sidecar.proto` | gRPC/protobuf service + message definitions |
| `.gitmodules` | Git submodule references (credible-layer-contracts) |

## Naming Conventions

**Files:**
- Snake_case for all Rust source files: `assertion_store.rs`, `event_sequencing.rs`
- `mod.rs` for module root files in directories
- `lib.rs` for library crate roots; `main.rs` for binary crate roots
- Test files: co-located `tests.rs` or `#[cfg(test)] mod tests` within source files

**Crates:**
- Kebab-case for crate names in `Cargo.toml`: `assertion-executor`, `da-client`
- Directories match crate names: `crates/assertion-executor/`, `crates/assertion-da/da-client/`

**Directories:**
- Lowercase with hyphens for crate directories
- Lowercase with underscores for module directories within `src/`

## Where to Add New Code

**New sidecar component:**
- Implementation: `crates/sidecar/src/{component_name}/mod.rs`
- Register module in: `crates/sidecar/src/lib.rs`
- Wire into main: `crates/sidecar/src/main.rs` (in `run_sidecar_once()` or `run_async_components()`)
- Tests: Co-located `tests.rs` within the module directory

**New assertion executor feature:**
- Implementation: `crates/assertion-executor/src/{module}/`
- Register module in: `crates/assertion-executor/src/lib.rs`
- Tests: Co-located within source or `#[cfg(test)]` blocks

**New EVM precompile/inspector:**
- Implementation: `crates/assertion-executor/src/inspectors/precompiles/{name}/mod.rs`
- Register in: `crates/assertion-executor/src/inspectors/precompiles/mod.rs`

**New cache/state source:**
- Implementation: `crates/sidecar/src/cache/sources/{source_name}.rs` or `{source_name}/mod.rs`
- Must implement `Source` trait from `crates/sidecar/src/cache/sources/mod.rs`
- Register in: `crates/sidecar/src/main.rs` (`build_sources_from_config()`)

**New PCL CLI command:**
- Command definition: `crates/pcl/cli/src/cli.rs` (add to `Commands` enum)
- Business logic: `crates/pcl/core/src/{command_name}.rs`
- Wire into: `crates/pcl/cli/src/main.rs` (match arm in `cli.command`)

**New workspace crate:**
- Create directory under `crates/`
- Add to `[workspace.members]` in root `Cargo.toml`
- Use `workspace = true` for shared dependencies

**New database abstraction:**
- Implementation: `crates/assertion-executor/src/db/{name}.rs`
- Must implement `Database` or `DatabaseRef` from `crates/assertion-executor/src/db/mod.rs`

**New integration test:**
- Location: Within the crate's `tests/` directory or `#[cfg(test)]` module
- Use `int-test-utils` for shared test infrastructure
- For sidecar tests, use `sidecar-test-macros` proc macro for test setup

**New Docker image:**
- Dockerfile: `dockerfile/Dockerfile.{service-name}`
- Add `docker-build` target to `Makefile` if needed

## Special Directories

**`target/`:**
- Purpose: Cargo build artifacts (debug/release/bench)
- Generated: Yes
- Committed: No (in `.gitignore`)

**`lib/credible-layer-contracts/`:**
- Purpose: Git submodule containing on-chain Solidity contracts for the credible layer
- Generated: No (submodule checkout)
- Committed: Yes (as submodule reference in `.gitmodules`)

**`testdata/mock-protocol/`:**
- Purpose: Foundry project with mock Solidity assertion contracts for testing
- Generated: Partially (Forge `out/` directory is generated)
- Committed: Source files committed, artifacts may be gitignored

**`crates/dapp-api-client/src/generated/`:**
- Purpose: Auto-generated OpenAPI client code via `progenitor`
- Generated: Yes (via `cargo build --features regenerate` or `make regenerate`)
- Committed: Yes (generated code is checked in)

**`fuzz/`:**
- Purpose: Fuzz testing crate
- Generated: No
- Committed: Yes, but excluded from workspace via `exclude = ["fuzz"]`

---

*Structure analysis: 2026-03-25*
