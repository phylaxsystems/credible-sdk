# Technology Stack

**Analysis Date:** 2026-03-25

## Languages

**Primary:**
- Rust (nightly-2026-01-07) - All application code, services, CLI tools, libraries
- Edition 2024 specified in `Cargo.toml` workspace config

**Secondary:**
- Solidity (^0.8.x) - Smart contracts for assertions and test fixtures in `lib/credible-layer-contracts/` and `testdata/mock-protocol/`
- Protocol Buffers (proto3) - gRPC service definitions in `crates/sidecar/src/transport/grpc/sidecar.proto`
- Nix - Development environment in `flake.nix`

## Runtime

**Environment:**
- Rust nightly-2026-01-07 (pinned in `rust-toolchain.toml`)
- Tokio async runtime (full features) for all services
- Ubuntu 24.04 base image for production Docker containers

**Package Manager:**
- Cargo (workspace-based, resolver v2)
- Lockfile: `Cargo.lock` present, `--locked` enforced in builds and CI

## Frameworks

**Core:**
| Name | Version | Purpose | Critical? |
|------|---------|---------|-----------|
| tokio | 1.48.0 | Async runtime (full features + test-util) | Yes |
| axum | 0.8.7 | HTTP server framework (health endpoints, verifier-service) | Yes |
| tonic | 0.14.2 | gRPC server/client framework (sidecar transport) | Yes |
| clap | 4.5.53 | CLI argument parsing (all binaries) | Yes |
| revm | 33.1.0 | Rust EVM execution engine | Yes |
| alloy | 1.1.3 | Ethereum types, providers, RPC client | Yes |
| hyper | 1.8.1 | HTTP primitives (DA server) | Yes |

**EVM / Blockchain:**
| Name | Version | Purpose | Critical? |
|------|---------|---------|-----------|
| alloy-provider | 1.1.3 | Ethereum JSON-RPC/WS provider (ws, anvil, trace, debug APIs) | Yes |
| alloy-primitives | 1.5.0 | Ethereum primitive types (Address, B256, U256) | Yes |
| alloy-sol-types | 1.5.0 | Solidity type encoding/decoding | Yes |
| alloy-evm | 0.25.2 | EVM abstraction layer | Yes |
| op-revm | 14.1.0 | Optimism-specific REVM extensions | Yes |
| reth-db / reth-libmdbx | v1.9.3 (git tag) | Database layer from Reth (MDBX bindings) | Yes |
| alloy-consensus | 1.1.3 | Consensus types with k256 signatures | Yes |

**Solidity Tooling:**
| Name | Version | Purpose | Critical? |
|------|---------|---------|-----------|
| forge (phoundry) | git:main | Phylax fork of Foundry for assertion compilation/testing | Yes |
| foundry-config | git:main | Forge configuration parsing | Yes |
| foundry-compilers | 0.19.10 | Solidity compilation infrastructure | Yes |
| solar-compiler | git (patched) | Solar Solidity compiler (paradigmxyz fork, pinned rev 1f28069) | Yes |

**Testing:**
| Name | Version | Purpose | Critical? |
|------|---------|---------|-----------|
| cargo-nextest | (CI installed) | Test runner (replaces `cargo test` in CI) | Yes |
| criterion | 0.5.1 | Benchmarking framework | No |
| wiremock | 0.6.5 | HTTP mock server | No |
| testcontainers | 0.26.0 | Docker-based test infrastructure (Redis) | No |
| httpmock | 0.8 | HTTP mock server (sidecar/dapp-api tests) | No |
| mockito | 1.2 | HTTP mock server (pcl-core tests) | No |
| tracing-test | 0.2.5 | Test-scoped tracing subscriber | No |

**Build/Dev:**
| Name | Version | Purpose | Critical? |
|------|---------|---------|-----------|
| cargo-chef | (CI installed) | Docker layer caching for Rust builds | Yes |
| cargo-deny | (configured via `deny.toml`) | Dependency auditing (advisories, licenses, bans) | No |
| cargo-shear | (configured via metadata) | Unused dependency detection | No |
| cargo-fuzz | (nix devshell) | Fuzz testing support | No |
| cargo-flamegraph | (nix devshell) | Performance profiling | No |
| sccache | (CI) | Shared build cache (GitHub Actions) | No |
| vergen-gix | 1.0.9 | Build-time version/git info embedding (PCL CLI) | No |
| protoc-bin-vendored | 3 | Vendored protobuf compiler (sidecar gRPC codegen) | Yes |
| tonic-prost-build | 0.14.2 | gRPC/protobuf code generation | Yes |
| progenitor | 0.8 | OpenAPI client code generation (dapp-api-client) | Yes |

## Key Dependencies

**Critical:**
| Package | Version | Why It Matters |
|---------|---------|----------------|
| revm | 33.1.0 | Core EVM execution for assertion verification |
| alloy | 1.1.3 | All Ethereum type encoding, RPC, and provider infrastructure |
| reth-libmdbx | v1.9.3 | MDBX database for state persistence (state-worker/sidecar) |
| sled | 1.0.0-alpha.124 | Embedded KV store for assertion storage (alpha release) |
| tonic/prost | 0.14.2 | gRPC transport between block builder and sidecar |
| reqwest | 0.12.26 | HTTP client (DA client, GraphQL event source, dapp API) |
| bollard | 0.19.4 | Docker API client (DA server runs solc in Docker containers) |

**Infrastructure:**
| Package | Version | Purpose |
|---------|---------|---------|
| tracing / tracing-subscriber | 0.1.43 / 0.3.22 | Structured logging with env-filter; release_max_level_debug |
| rust-tracing | 0.1.3 (git) | Phylax custom tracing wrapper (OpenTelemetry integration) |
| metrics | 0.24.3 | Prometheus-compatible metrics (counters, gauges, histograms) |
| rustls | 0.23.0 | TLS implementation (aws_lc_rs backend) |
| moka | 0.12.11 | Thread-safe concurrent cache |
| dashmap | 6.1.0 | Concurrent hash map |
| rayon | 1.11.0 | Data parallelism (assertion execution, MDBX) |
| flume | 0.12.0 | MPMC channels (inter-component communication in sidecar) |
| parking_lot | 0.12.5 | Faster mutex/rwlock implementations |
| redis | 1.0.1 | Optional Redis backend for DA server |

## Configuration

**Environment:**
- All services use `clap` with `env` feature for CLI args + env var config
- Sidecar supports JSON config files (`--config-file-path`) with defaults embedded via `include_str!`
- DA server configurable via CLI flags: `DA_DB_PATH`, `DA_CACHE_SIZE`, `DA_LISTEN_ADDR`, `DA_PRIVATE_KEY`, `DA_LOG_LEVEL`, `DA_METRICS_ADDR`, `DA_REDIS_URL`
- State worker configurable via CLI: `--ws-url`, `--mdbx-path`, `--state-depth`, `--file-to-genesis`, `--start-block`
- Verifier service: `VERIFIER_SERVICE_BIND_ADDR` (default `127.0.0.1:8200`)
- PCL CLI reads config from `~/.pcl/config.json` (via `dirs` crate)
- Tracing configured via `RUST_LOG`, `OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT` env vars

**Build:**
- `Cargo.toml` - Workspace root with all dependency versions
- `rust-toolchain.toml` - Pinned nightly + components (rustfmt, clippy, rust-analyzer, rust-src)
- `rustfmt.toml` - Formatting rules (vertical imports, block indent, doc comment formatting)
- `deny.toml` - Dependency audit config (advisories, licenses, bans, sources)
- `flake.nix` - Nix dev shell with LLVM/clang toolchain, OpenSSL, Rust nightly

**Build Profiles:**
- `release` - Standard release profile
- `maxperf` - Fat LTO, single codegen unit, no incremental (production builds)
- `debug-perf` - Release with frame pointers and debug symbols for profiling
- `bench` - Debug info enabled, low optimization for benchmarks

## Platform Requirements

**Development:**
- Rust nightly-2026-01-07 (pinned)
- Clang/LLVM (required for C dependencies, MDBX)
- OpenSSL development headers
- Docker (for DA server tests, testcontainers, running solc)
- Foundry/Forge (for Solidity contract compilation)
- Protobuf compiler (vendored via `protoc-bin-vendored` for gRPC codegen)
- Nix (optional, provides complete dev environment via `flake.nix`)
- macOS (ARM64) or Linux (x86_64/ARM64) supported

**Production:**
- Ubuntu 24.04 (Docker base image)
- Docker socket access (DA server needs Docker to run solc containers)
- Kubernetes (GKE) for sidecar + state-worker deployments via Helm
- Railway for verifier-service deployment
- GHCR (ghcr.io/phylaxsystems/credible-sdk/*) for container images

**CI/CD:**
- GitHub Actions (self-hosted runner group: `big-bois-x86`)
- Shared workflows from `phylaxsystems/actions` repository
- Separate Helm chart repo: `phylaxsystems/credible-sidecar-helm-chart`
- Environment config repo: `phylaxsystems/credible-layer-env-config`
- Deployment environments: `linea-internal`, `shadow-layer`, `platform-internal`

## Workspace Members (Crate Inventory)

| Crate | Path | Type | Description |
|-------|------|------|-------------|
| sidecar | `crates/sidecar` | binary + library | Core sidecar service (block builder integration) |
| assertion-da-server | `crates/assertion-da/da-server` | binary | Assertion data availability server |
| assertion-da-client | `crates/assertion-da/da-client` | library | HTTP client for DA server |
| assertion-da-core | `crates/assertion-da/da-core` | library | Shared DA types |
| assertion-executor | `crates/assertion-executor` | library | EVM-based assertion execution engine |
| assertion-verification | `crates/assertion-verification` | library | Assertion verification logic |
| verifier-service | `crates/verifier-service` | binary | HTTP verification service (axum) |
| state-worker | `crates/state-worker` | binary | Chain state synchronization worker |
| state-checker | `crates/state-checker` | binary | State verification tool |
| shadow-driver | `crates/shadow-driver` | binary | Shadow chain block listening + replay |
| mdbx | `crates/mdbx` | library | MDBX database abstraction (reader/writer features) |
| pcl (CLI) | `crates/pcl/cli` | binary | Phylax Credible Layer CLI tool |
| pcl-core | `crates/pcl/core` | library | PCL CLI core logic |
| pcl-common | `crates/pcl/common` | library | Shared PCL types |
| pcl-phoundry | `crates/pcl/phoundry` | library | Phoundry (Foundry fork) integration |
| dapp-api-client | `crates/dapp-api-client` | library | Auto-generated dApp API client (progenitor) |
| credible-utils | `crates/credible-utils` | library | Shared utilities (shutdown signals) |
| eip-system-calls | `crates/eip-system-calls` | library | EIP system call logic (EIP-4788, EIP-2935) |
| int-test-utils | `crates/int-test-utils` | library | Integration test utilities |
| benchmark-utils | `crates/benchmark-utils` | library | Benchmark test utilities |
| geth_snapshot | `scripts/geth_snapshot` | binary | Geth snapshot utility |

## Key Technical Decisions

- **Rust nightly**: Required for Edition 2024, let-chains, and other unstable features
- **REVM over native EVM**: In-process EVM execution for assertion checking without full node overhead
- **Alloy over ethers-rs**: Modern Ethereum library ecosystem (alloy is the ethers-rs successor)
- **MDBX (via Reth)**: High-performance embedded database for blockchain state storage, chosen over pure-Rust alternatives for throughput
- **Sled (alpha)**: Embedded KV store for assertion storage; alpha version accepted for simplicity
- **gRPC streaming**: Bidirectional streaming for block builder <-> sidecar communication (replaces earlier RPC-based approach)
- **OS threads for critical paths**: Engine, event sequencing, and transaction observer run on dedicated OS threads (not just tokio tasks) for predictable latency
- **Flume channels**: MPMC channels between components instead of tokio channels for cross-thread communication
- **Feature flags for test isolation**: `full-test` and `test` features gate Docker-dependent and integration tests
- **Optimism support**: Optional via `optimism` feature flag in assertion-executor and sidecar
- **Solar compiler patches**: Patched solar-compiler from paradigmxyz fork required by phoundry

---

*Stack analysis: 2026-03-25*
