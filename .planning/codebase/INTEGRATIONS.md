# External Integrations

**Analysis Date:** 2026-03-25

## APIs & External Services

**Ethereum JSON-RPC / WebSocket:**
- Ethereum Execution Client (Geth, Besu, etc.) - State synchronization, block subscriptions, trace APIs
  - SDK/Client: `alloy-provider` with WebSocket transport (`WsConnect`)
  - Used by: `crates/state-worker/src/main.rs` (primary provider), `crates/sidecar/src/cache/sources/eth_rpc_source.rs` (state source)
  - Auth: Connection URL via CLI args (`--ws-url` for state-worker, config JSON for sidecar)
  - APIs used: `debug_traceBlockByNumber` (prestateTracer diffMode), `web3_clientVersion`, `eth_subscribe` (newHeads), standard eth_ namespace
  - Geth version requirement: >= 1.16.6 (validated in `crates/state-worker/src/geth_version.rs` for EIP-6780 correctness)

**GraphQL Event Source (Sidecar Indexer):**
- Sidecar-indexer GraphQL API - Queries assertion added/removed events
  - SDK/Client: `reqwest::Client` (POST to GraphQL endpoint)
  - Implementation: `crates/sidecar/src/graphql_event_source.rs`
  - Config: `event_source_url` in sidecar config JSON
  - Timeout: 10 seconds per request

**Phylax dApp API:**
- Phylax Platform API - Incident reporting, assertion management
  - SDK/Client: Auto-generated via `progenitor` from OpenAPI spec
  - Client crate: `crates/dapp-api-client/`
  - Generated code: `crates/dapp-api-client/src/generated/client.rs`
  - OpenAPI spec source: `https://dapp.phylax.systems/api/v1/openapi` (production) or `http://localhost:3000/api/v1/openapi` (dev)
  - OpenAPI spec cache: `crates/dapp-api-client/openapi/spec.json`
  - Regeneration: `make regenerate` (production) or `make regenerate-dev` (development)
  - Auth: Bearer token (`transaction_observer_auth_token` in sidecar config)
  - Used by: `crates/sidecar/src/transaction_observer/client.rs` for incident publishing

**Aeges Reporting Service:**
- Aeges - Additional incident/event reporting endpoint
  - SDK/Client: `reqwest::blocking::Client`
  - Implementation: `crates/sidecar/src/transaction_observer/client.rs` (`build_aeges_client`)
  - Config: Optional `aeges_url` in sidecar credible config
  - Auth: Not documented (URL-based)

## Data Storage

**MDBX (via Reth):**
- Purpose: High-performance embedded database for blockchain state storage
  - Crate: `crates/mdbx/` (wraps `reth-libmdbx` v1.9.3)
  - Features: `reader` (sidecar), `writer` (state-worker, state-checker)
  - Connection: File path via config (`--mdbx-path` for state-worker, `state.sources[].mdbx_path` for sidecar)
  - Used by: `crates/state-worker/` (writes state), `crates/sidecar/` (reads state via `MdbxSource`)
  - Config: Circular buffer depth configurable via `--state-depth` / `state.sources[].depth`

**Sled:**
- Purpose: Embedded KV store for assertion storage and DA server persistence
  - Version: 1.0.0-alpha.124 (alpha)
  - Used by: `crates/sidecar/src/config.rs` (assertion store), `crates/assertion-da/da-server/src/config.rs` (DA persistence)
  - Connection: File path via config (`assertion_store_db_path` for sidecar, `DA_DB_PATH` for DA server)
  - Config: `cache_capacity_bytes`, `flush_every_ms`, prune config (interval_ms, retention_blocks)

**Redis (Optional):**
- Purpose: Alternative backend for DA server storage (instead of Sled)
  - Version: redis crate 1.0.1 with r2d2 connection pooling
  - Used by: `crates/assertion-da/da-server/src/config.rs` (`RedisDb`)
  - Connection: `DA_REDIS_URL` env var (optional; falls back to Sled if unset)
  - Test infra: `testcontainers-modules` with Redis 7-alpine for integration tests

**File Storage:**
- Genesis files: JSON genesis state files loaded by state-worker (`--file-to-genesis`)
  - Example: `etc/genesis/linea-sepolia.json`
- Assertion store: Local filesystem path for assertion DB
- Config files: JSON config files for sidecar (`--config-file-path`)

**Caching:**
- Moka cache: Thread-safe concurrent cache in sidecar (`moka` 0.12.11 with `sync` feature)
  - Used for: EVM state overlay caching
- DashMap: Concurrent hash map for in-memory state (`dashmap` 6.1.0)

## Authentication & Identity

**Ethereum Signing:**
- DA server uses a private key for assertion signing
  - Config: `DA_PRIVATE_KEY` env var (hex-encoded private key)
  - SDK: `alloy::signers::local::PrivateKeySigner`
  - Used in: `crates/assertion-da/da-server/src/config.rs`

**dApp API Auth:**
- Bearer token authentication for incident publishing
  - Config: `transaction_observer_auth_token` in sidecar config (stored as `SecretString`)
  - Implementation: `crates/sidecar/src/args/mod.rs` (`SecretString` type with redacted Debug/Display)

**PCL CLI Auth:**
- OAuth/token-based auth for Phylax platform
  - Config: Stored in `~/.pcl/config.json` via `pcl auth` command
  - Implementation: `crates/pcl/core/src/config.rs` (`CliConfig`)

## Monitoring & Observability

**Metrics (Prometheus):**
- Framework: `metrics` crate (0.24.3) with Prometheus exporter
  - Default endpoint: `0.0.0.0:9000` (configurable via `TRACING_METRICS_PORT`)
  - DA server metrics: Port 9002 (`DA_METRICS_ADDR`)
  - Implementation: `crates/sidecar/src/metrics.rs` (block metrics, transaction metrics, state metrics, source metrics)
  - Key metric prefixes: `sidecar_*`, `db_size_mb`

**Tracing / Logging:**
- Framework: `tracing` + `tracing-subscriber` with `env-filter`
  - Custom wrapper: `rust-tracing` (Phylax git dependency, tag 0.1.3) integrates OpenTelemetry
  - Config: `RUST_LOG` for log level, `TRACING_LOG_JSON=true` for JSON output
  - Release builds: `release_max_level_debug` (debug and below compiled out)

**OpenTelemetry:**
- Protocol: OTLP (HTTP/protobuf)
  - Config: `OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_PROTOCOL`
  - Collector: OpenTelemetry Collector 0.99.0 (in dev docker-compose)
  - Traces backend: Grafana Tempo 2.4.1

**Observability Stack (Development):**
- Full stack in `docker/maru-besu-sidecar/docker-compose.yml`:
  - OpenTelemetry Collector (`otel/opentelemetry-collector:0.99.0`)
  - Grafana Tempo (`grafana/tempo:2.4.1`) - Distributed tracing
  - Grafana Loki (`grafana/loki:2.9.7`) - Log aggregation
  - Promtail (`grafana/promtail:2.9.7`) - Log shipping
  - Prometheus (`prom/prometheus:v2.52.0`) - Metrics collection
  - Grafana (`grafana/grafana:10.4.2`) - Dashboards (port 3000)

**Tokio Console (Optional):**
- Feature-gated: `tokio-console` feature flag on sidecar
  - Packages: `console-subscriber` 0.5.0, `tokio-metrics` 0.4.6

**Heap Profiling (Optional):**
- Feature-gated: `dhat-heap` feature flag on sidecar
  - Package: `dhat` 0.3
  - Config: `--dhat-output-path` CLI argument

## CI/CD & Deployment

**Container Registry:**
- GHCR: `ghcr.io/phylaxsystems/credible-sdk/`
  - Images: `sidecar`, `sidecar-debug`, `assertion-da`, `assertion-da-dev`, `shadow-driver`, `state-worker`, `verifier-service`
  - Workflow: `.github/workflows/docker-ghcr-push-containers.yaml`
  - Versioning: Cargo.toml version or git SHA fallback

**Kubernetes (GKE):**
- Helm deployments for sidecar + state-worker
  - Chart repo: `phylaxsystems/credible-sidecar-helm-chart`
  - Config repo: `phylaxsystems/credible-layer-env-config`
  - Values files: `values-{environment}.yaml`
  - Environments: `linea-internal`, `shadow-layer`, `benchmark`
  - Auth: GCP service account key (`GCP_SA_KEY` secret)
  - Workflow: `.github/workflows/helm-deploy.yaml`

**Railway:**
- Verifier service deployment
  - Workflow: `.github/workflows/docker-ghcr-push-containers.yaml` (railway-deploy-verifier-service job)
  - Environment: `platform-internal`
  - Auth: `RAILWAY_API_TOKEN`, `RAILWAY_PROJECT_ID`, `RAILWAY_ENVIRONMENT_ID`, `RAILWAY_VERIFIER_SERVICE_ID`

**GitHub Actions CI:**
- Test & Lint: `.github/workflows/rust-test-lint.yml`
  - Shared workflow: `phylaxsystems/actions/.github/workflows/rust-base.yaml@main`
  - Runner: `big-bois-x86` self-hosted group
  - Features: nextest, sccache, foundry, Docker-in-Docker, submodules
  - Feature sets tested: default+test, optimism+test
- Benchmarks: `.github/workflows/benchmarks.yml`
  - Criterion benchmarks with PR comparison
  - GitHub Pages for benchmark results
- Release: `.github/workflows/release.yml`
  - Tag creation, binary builds (pcl, assertion-da, sidecar), GitHub Release, Homebrew tap update
  - Platforms: macOS ARM64, Linux x86_64, Linux ARM64
  - Homebrew repo: `phylaxsystems/homebrew-pcl`

**Docker Build Strategy:**
- Multi-stage builds with `cargo-chef` for layer caching
  - Stage 1 (chef): Install nightly toolchain + cargo-chef + clang
  - Stage 2 (planner): Prepare recipe
  - Stage 3 (builder): Cook dependencies, then build binary
  - Stage 4 (runtime): Ubuntu 24.04 minimal with just the binary
  - All Dockerfiles: `dockerfile/Dockerfile.*`

## External Blockchain Infrastructure

**Block Builder Integration (gRPC):**
- Bidirectional streaming gRPC between block builder (e.g., Linea Besu sequencer) and sidecar
  - Proto definition: `crates/sidecar/src/transport/grpc/sidecar.proto`
  - Service: `SidecarTransport`
  - RPCs: `StreamEvents` (bidirectional), `SubscribeResults` (server stream), `GetTransactions`, `GetTransaction`
  - Default port: 50051

**Linea Besu Sequencer:**
- Dev setup: `docker/maru-besu-sidecar/docker-compose.yml`
  - Image: `ghcr.io/phylaxsystems/linea-monorepo/linea-besu-package`
  - Ports: 8545 (HTTP RPC), 8546 (WS RPC), 8550 (Engine API)
  - Credible integration via gRPC plugin config

**Maru (Linea Prover):**
- Dev setup: `docker/maru-besu-sidecar/docker-compose.yml`
  - Image: `consensys/maru`
  - Purpose: L2 proving/sequencing

**Docker Daemon:**
- DA server connects to local Docker daemon to run `solc` compiler containers
  - SDK: `bollard` 0.19.4
  - Connection: Docker socket (`/var/run/docker.sock` mounted in containers)
  - Used in: `crates/assertion-da/da-server/src/config.rs` (`Docker::connect_with_local_defaults()`)

**Anvil (Testing):**
- Alloy Anvil node bindings for integration testing
  - Feature: `alloy-node-bindings` / `alloy` `node-bindings` feature
  - Used by: `crates/assertion-executor/`, `crates/int-test-utils/`

## Webhooks & Callbacks

**Incoming:**
- gRPC `StreamEvents` - Block builder sends transaction events to sidecar
- gRPC `SubscribeResults` - Clients subscribe to transaction execution results
- HTTP health endpoint (`/health`) on sidecar health server

**Outgoing:**
- dApp API incident reports (HTTP POST via generated client)
- Aeges event reports (HTTP POST via blocking reqwest client)
- GraphQL queries to sidecar-indexer for assertion events
- DA server HTTP requests for assertion fetch/submit

## Environment Configuration

**Required env vars (by service):**

**Sidecar:**
- `--config-file-path` or individual CLI args (chain_id, spec_id, etc.)
- `RUST_LOG` - Log level
- OTEL vars for tracing (optional)

**DA Server (assertion-da):**
- `DA_PRIVATE_KEY` - Signing key for assertions (required)
- `DA_LISTEN_ADDR` - Server bind address (default: `0.0.0.0:5001`)
- `DA_DB_PATH` - Database path (optional, defaults to platform data dir)
- `DA_CACHE_SIZE` - Sled cache size (default: 1000000)
- `DA_LOG_LEVEL` - Log level (default: info)
- `DA_METRICS_ADDR` - Metrics endpoint (default: `0.0.0.0:9002`)
- `DA_REDIS_URL` - Optional Redis URL (falls back to Sled)

**State Worker:**
- `--ws-url` - WebSocket URL to execution client (required)
- `--mdbx-path` - MDBX database path (required)
- `--state-depth` - Circular buffer depth (required)
- `--file-to-genesis` - Genesis JSON file path (required)
- `--start-block` - Starting block number

**Verifier Service:**
- `VERIFIER_SERVICE_BIND_ADDR` - Bind address (default: `127.0.0.1:8200`)

**Shadow Driver:**
- `--ws-url` - WebSocket URL to chain
- `--sidecar-url` - Sidecar gRPC URL
- `--starting-block` - Starting block number
- `--mdbx-path` - MDBX path

**PCL CLI:**
- Config file at `~/.pcl/config.json` (managed via `pcl config` / `pcl auth` commands)
- `DAPP_ENV` / `DAPP_OPENAPI_URL` - For API client regeneration only

**Secrets location:**
- CI: GitHub Actions secrets (GCP_SA_KEY, RAILWAY_API_TOKEN, SSH_PRIVATE_KEY, GH_TOKEN)
- Production: Kubernetes secrets / Railway environment variables
- Development: Docker-compose env vars (dev keys hardcoded in `etc/docker-compose-dev.yaml`)

## Integration Patterns

**Event-Driven Architecture (Sidecar):**
- Block builder -> gRPC StreamEvents -> Transport -> EventSequencing -> CoreEngine -> TransactionObserver
- Channels: `flume::unbounded()` between components
- Components run on dedicated OS threads with `AtomicBool` shutdown coordination
- Location: `crates/sidecar/src/main.rs` (orchestration), `crates/sidecar/src/engine/`, `crates/sidecar/src/event_sequencing/`

**State Source Abstraction:**
- `Source` trait with multiple implementations: `MdbxSource`, `EthRpcSource`
- Configured via `state.sources[]` array in sidecar config
- Location: `crates/sidecar/src/cache/sources/`

**Code Generation:**
- dApp API client: OpenAPI spec -> `progenitor` -> Rust client at `crates/dapp-api-client/src/generated/client.rs`
- gRPC transport: protobuf -> `tonic-prost-build` -> generated Rust types/services (in `OUT_DIR`)

**Health Check Pattern:**
- All services expose `/health` HTTP endpoint
- DA reachability monitor periodically checks DA server availability
- GraphQL event source has `health_check()` before starting indexer

---

*Integration audit: 2026-03-25*
