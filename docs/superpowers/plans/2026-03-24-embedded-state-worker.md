# Embedded State Worker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Embed the state worker inside the sidecar as a supervised OS thread, gate MDBX mutation on commit head, and replace the circular-buffered MDBX integration with a latest-state-only shared runtime plus `EthRpcSource` fallback.

**Architecture:** The refactor has three main slices: simplify `crates/mdbx` to a latest-state store, expose `crates/state-worker` as a reusable staged-diff runtime that traces heads continuously but only flushes on commit targets, and wire `crates/sidecar` to host a shared MDBX runtime plus supervisor/status handles used by the engine and `MdbxSource`. The engine stays non-blocking by publishing commit targets asynchronously, while cache reads continue to fall back to `EthRpcSource` whenever local MDBX is unhealthy or behind.

**Tech Stack:** Rust, Tokio, OS threads, flume, parking_lot, Alloy providers, MDBX (`reth-libmdbx`), Criterion.

---

### Task 1: Rewrite MDBX Around Latest-State Semantics

**Files:**
- Modify: `crates/mdbx/src/lib.rs`
- Modify: `crates/mdbx/src/common/tables.rs`
- Modify: `crates/mdbx/src/common/types.rs`
- Modify: `crates/mdbx/src/db.rs`
- Modify: `crates/mdbx/src/reader.rs`
- Modify: `crates/mdbx/src/writer.rs`
- Modify: `crates/mdbx/src/tests.rs`
- Modify: `crates/state-checker/src/main.rs`
- Modify: `crates/state-checker/src/state_root.rs`
- Modify: `scripts/geth_snapshot/src/main.rs`

- [ ] **Step 1: Write the failing MDBX latest-state tests**

```rust
#[test]
fn test_latest_state_only_keeps_current_head_readable() {
    let tmp = tempfile::tempdir().unwrap();
    let writer = StateWriter::new(tmp.path()).unwrap();

    writer.commit_block(&simple_update(100, addr(0xAA), 1000, 1)).unwrap();
    writer.commit_block(&simple_update(101, addr(0xAA), 1100, 2)).unwrap();

    let reader = StateReader::new(tmp.path()).unwrap();
    assert!(reader.is_block_available(101).unwrap());
    assert!(!reader.is_block_available(100).unwrap());
    assert_eq!(reader.latest_block_number().unwrap(), Some(101));
}

#[test]
fn test_get_available_block_range_is_removed_from_latest_state_api() {
    // Replace old range assertions with `latest_block_number` / `is_block_available`.
}
```

- [ ] **Step 2: Run the targeted MDBX tests to verify they fail for the right reason**

Run: `cargo test -p mdbx test_latest_state_only_keeps_current_head_readable -- --exact`

Expected: FAIL because `StateWriter::new` and `StateReader::new` still require `CircularBufferConfig` and the current writer still keeps historical namespaces readable.

- [ ] **Step 3: Implement the latest-state-only MDBX core**

```rust
pub trait Reader {
    type Error: std::error::Error;

    fn latest_block_number(&self) -> Result<Option<u64>, Self::Error>;
    fn is_block_available(&self, block_number: u64) -> Result<bool, Self::Error>;
    fn get_account(&self, address_hash: AddressHash, block_number: u64)
        -> Result<Option<AccountInfo>, Self::Error>;
}

impl StateReader {
    pub fn new(path: impl AsRef<Path>) -> StateResult<Self> { /* no buffer config */ }
}

impl StateWriter {
    pub fn new(path: impl AsRef<Path>) -> StateResult<Self> { /* no buffer config */ }
}
```

Implementation notes:
- Remove `CircularBufferConfig` from public constructors and sidecar-facing docs.
- Replace namespace-rotation tables and diff-retention logic with single latest-state tables plus durable metadata (`latest_block`, `block_hash`, `state_root`).
- Keep block-numbered read methods for compatibility, but make only the current durable head readable; older block numbers should return unavailable / not found.
- Delete or rewrite tests in `crates/mdbx/src/tests.rs` that assert circular-buffer ranges, namespace rotation, or historical replay.
- Update `crates/state-checker` and `scripts/geth_snapshot` to use the new constructors and latest-state assumptions.

- [ ] **Step 4: Run the package tests that exercise the rewritten API**

Run: `cargo test -p mdbx`

Expected: PASS with the old circular-buffer tests removed or rewritten around latest-state semantics.

- [ ] **Step 5: Commit the MDBX rewrite**

```bash
git add crates/mdbx/src/lib.rs crates/mdbx/src/common/tables.rs crates/mdbx/src/common/types.rs crates/mdbx/src/db.rs crates/mdbx/src/reader.rs crates/mdbx/src/writer.rs crates/mdbx/src/tests.rs crates/state-checker/src/main.rs crates/state-checker/src/state_root.rs scripts/geth_snapshot/src/main.rs
git commit -m "refactor(mdbx): switch to latest-state semantics"
```

### Task 2: Extract A Reusable Embedded State-Worker Runtime

**Files:**
- Create: `crates/state-worker/src/lib.rs`
- Create: `crates/state-worker/src/embedded.rs`
- Modify: `crates/state-worker/src/worker.rs`
- Modify: `crates/state-worker/src/main.rs`
- Modify: `crates/state-worker/Cargo.toml`
- Modify: `crates/state-worker/src/integration_tests/setup.rs`
- Modify: `crates/state-worker/src/integration_tests/tests.rs`

- [ ] **Step 1: Write failing tests for staged tracing and commit-gated flushing**

```rust
#[tokio::test]
async fn test_worker_stages_heads_without_flushing_before_commit_target() {
    let harness = EmbeddedWorkerHarness::new().await;

    harness.push_new_head(101).await;
    harness.wait_for_staged(101).await;

    assert_eq!(harness.status().mdbx_synced_through, Some(100));
}

#[tokio::test]
async fn test_worker_flushes_only_up_to_commit_target() {
    let harness = EmbeddedWorkerHarness::new().await;

    harness.push_new_head(101).await;
    harness.push_new_head(102).await;
    harness.publish_commit_target(101);
    harness.wait_for_flushed(101).await;

    assert_eq!(harness.status().mdbx_synced_through, Some(101));
    assert_eq!(harness.status().highest_staged_block, Some(102));
}
```

- [ ] **Step 2: Run the targeted state-worker tests to verify they fail**

Run: `cargo test -p state-worker test_worker_stages_heads_without_flushing_before_commit_target -- --exact`

Expected: FAIL because the current worker writes directly to MDBX while following `newHeads` and has no staging / commit-target control path.

- [ ] **Step 3: Implement the reusable embedded runtime**

```rust
pub struct WorkerStatusSnapshot {
    pub latest_head_seen: Option<u64>,
    pub highest_staged_block: Option<u64>,
    pub mdbx_synced_through: Option<u64>,
    pub healthy: bool,
    pub restarting: bool,
}

pub struct EmbeddedStateWorkerHandle {
    pub control: CommitTargetHandle,
    pub status: Arc<WorkerStatus>,
}
```

Implementation notes:
- Move reusable worker modules behind `crates/state-worker/src/lib.rs` so sidecar can depend on the crate as a library.
- Add an `embedded.rs` runtime that keeps a staged in-memory map of `block_number -> BlockStateUpdate`.
- Continue following `newHeads`, but stage traced diffs in memory and only flush staged blocks `<= commit_target`.
- Keep staged memory unbounded by design and drop entries immediately after a successful MDBX write.
- Preserve standalone `state-worker` binary behavior by turning `main.rs` into a thin wrapper that auto-advances commit target to latest head for CLI/tooling use.

- [ ] **Step 4: Run the state-worker package tests**

Run: `cargo test -p state-worker`

Expected: PASS with new staged/flush coverage and the standalone wrapper compiling against the library API.

- [ ] **Step 5: Commit the reusable runtime extraction**

```bash
git add crates/state-worker/src/lib.rs crates/state-worker/src/embedded.rs crates/state-worker/src/worker.rs crates/state-worker/src/main.rs crates/state-worker/Cargo.toml crates/state-worker/src/integration_tests/setup.rs crates/state-worker/src/integration_tests/tests.rs
git commit -m "refactor(state-worker): add embedded staged runtime"
```

### Task 3: Add A Shared Sidecar MDBX Runtime And Worker Supervisor

**Files:**
- Modify: `crates/sidecar/Cargo.toml`
- Modify: `crates/sidecar/src/lib.rs`
- Create: `crates/sidecar/src/state_sync/mod.rs`
- Create: `crates/sidecar/src/state_sync/mdbx_runtime.rs`
- Create: `crates/sidecar/src/state_sync/supervisor.rs`
- Create: `crates/sidecar/src/state_sync/tests.rs`
- Modify: `crates/sidecar/src/main.rs`

- [ ] **Step 1: Write failing supervisor/runtime tests**

```rust
#[test]
fn test_shared_mdbx_runtime_initializes_once() {
    let first = mdbx_runtime::init("/tmp/state.mdbx").unwrap();
    let second = mdbx_runtime::init("/tmp/state.mdbx").unwrap();
    assert!(Arc::ptr_eq(&first, &second));
}

#[test]
fn test_supervisor_restarts_worker_after_panic() {
    let supervisor = TestSupervisor::spawn_with_panicking_worker();
    supervisor.wait_for_restart_count(1);
    assert!(supervisor.status().healthy);
}
```

- [ ] **Step 2: Run the targeted sidecar tests to verify they fail**

Run: `cargo test -p sidecar test_shared_mdbx_runtime_initializes_once -- --exact`

Expected: FAIL because the sidecar currently has no shared MDBX runtime module and no embedded state-worker supervisor.

- [ ] **Step 3: Implement the shared runtime and supervisor**

```rust
static MDBX_RUNTIME: OnceLock<Arc<MdbxRuntime>> = OnceLock::new();

pub struct StateWorkerSupervisor {
    control: EmbeddedStateWorkerHandle,
    status: Arc<WorkerStatus>,
    restart_count: AtomicU64,
}
```

Implementation notes:
- Add `state-worker` as a sidecar dependency and enable `mdbx` writer support in `crates/sidecar/Cargo.toml`.
- Introduce `state_sync::mdbx_runtime` as the single sidecar-owned accessor for the shared reader/writer handles.
- Introduce `state_sync::supervisor` to spawn the embedded worker on a named OS thread, catch panics by thread exit, and restart after backoff.
- Extend `ThreadHandles` / `run_sidecar_once` in `crates/sidecar/src/main.rs` to own and join the supervisor thread independently from engine/event sequencing.

- [ ] **Step 4: Run the sidecar tests that exercise the new runtime layer**

Run: `cargo test -p sidecar test_supervisor_restarts_worker_after_panic -- --exact`

Expected: PASS with the worker restart isolated from overall sidecar shutdown.

- [ ] **Step 5: Commit the sidecar runtime layer**

```bash
git add crates/sidecar/Cargo.toml crates/sidecar/src/lib.rs crates/sidecar/src/state_sync/mod.rs crates/sidecar/src/state_sync/mdbx_runtime.rs crates/sidecar/src/state_sync/supervisor.rs crates/sidecar/src/state_sync/tests.rs crates/sidecar/src/main.rs
git commit -m "feat(sidecar): host embedded state worker supervisor"
```

### Task 4: Publish Commit Targets From The Engine And Replace `MdbxSource` Polling

**Files:**
- Modify: `crates/sidecar/src/engine/mod.rs`
- Modify: `crates/sidecar/src/engine/tests.rs`
- Modify: `crates/sidecar/src/cache/sources/mdbx/mod.rs`
- Modify: `crates/sidecar/src/cache/sources/mod.rs`
- Modify: `crates/sidecar/src/cache/mod.rs`
- Modify: `crates/sidecar/src/main.rs`

- [ ] **Step 1: Write failing engine/source tests**

```rust
#[test]
fn test_process_commit_head_publishes_committed_block_to_state_worker() {
    let sink = RecordingCommitTargetSink::default();
    let mut engine = build_engine_with_commit_sink(sink.clone());

    engine.process_commit_head(&commit_head(101), &mut 0, &mut Instant::now()).unwrap();

    assert_eq!(sink.values(), vec![101]);
}

#[test]
fn test_mdbx_source_uses_status_snapshot_without_background_poller() {
    let status = TestWorkerStatus::new(Some(120), Some(120), Some(100), true);
    let source = MdbxSource::new(test_reader(), status.into_handle());
    assert!(source.is_synced(U256::from(95), U256::from(100)));
}
```

- [ ] **Step 2: Run the targeted sidecar tests to verify they fail**

Run: `cargo test -p sidecar test_process_commit_head_publishes_committed_block_to_state_worker -- --exact`

Expected: FAIL because `CoreEngine` does not currently publish commit targets and `MdbxSource` still depends on its timer-driven range poller.

- [ ] **Step 3: Implement commit-target publication and status-driven sync**

```rust
pub struct CoreEngineConfig {
    pub commit_target_sink: Option<CommitTargetHandle>,
    // existing fields...
}

if let Some(sink) = &self.commit_target_sink {
    sink.publish(commit_head.block_number.saturating_to::<u64>());
}
```

Implementation notes:
- Add a commit-target sink/handle to `CoreEngineConfig` and call it only after commit head is accepted.
- Keep the publish path non-blocking; the engine should not wait on staged tracing or MDBX flush.
- Rewrite `MdbxSource` to consume shared worker status instead of `available_block_range()`.
- Remove the Tokio interval poller, cancellation token, and range-intersection logic from `crates/sidecar/src/cache/sources/mdbx/mod.rs`.
- Make `MdbxSource` use the current `mdbx_synced_through` height for reads and report unsynced when the worker is behind or restarting so `EthRpcSource` remains the fallback.

- [ ] **Step 4: Run the focused engine/cache tests**

Run: `cargo test -p sidecar test_process_commit_head_publishes_committed_block_to_state_worker test_mdbx_source_uses_status_snapshot_without_background_poller -- --exact`

Expected: PASS with no background poller and commit publication occurring only on accepted commit heads.

- [ ] **Step 5: Commit the engine/source wiring**

```bash
git add crates/sidecar/src/engine/mod.rs crates/sidecar/src/engine/tests.rs crates/sidecar/src/cache/sources/mdbx/mod.rs crates/sidecar/src/cache/sources/mod.rs crates/sidecar/src/cache/mod.rs crates/sidecar/src/main.rs
git commit -m "refactor(sidecar): drive mdbx from commit targets"
```

### Task 5: Migrate Sidecar Config And Runtime Entry Points

**Files:**
- Modify: `crates/sidecar/src/args/mod.rs`
- Modify: `crates/sidecar/default_config.json`
- Modify: `crates/sidecar/README.md`
- Modify: `docker/maru-besu-sidecar/config/credible-sidecar/grpc.config.json`
- Modify: `scripts/run-sidecar-host.sh`
- Modify: `crates/state-worker/src/cli.rs`

- [ ] **Step 1: Write failing config-resolution tests**

```rust
#[test]
fn test_state_config_resolves_embedded_worker_settings() {
    let config = Config::from_file("testdata/embedded-state-worker.json").unwrap();
    assert_eq!(config.state.worker.mdbx_path, "/data/state_worker.mdbx");
    assert!(config.state.sources.iter().any(|source| matches!(source, StateSourceConfig::EthRpc { .. })));
}
```

- [ ] **Step 2: Run the targeted config tests to verify they fail**

Run: `cargo test -p sidecar test_state_config_resolves_embedded_worker_settings -- --exact`

Expected: FAIL because `args/mod.rs` still models MDBX as a peer source with legacy `state_worker_*` fields and no embedded worker config block.

- [ ] **Step 3: Implement the config migration**

```json
"state": {
  "worker": {
    "ws_url": "ws://127.0.0.1:8546",
    "mdbx_path": "/data/state_worker.mdbx",
    "file_to_genesis": "/data/genesis.json"
  },
  "sources": [
    { "type": "eth-rpc", "ws_url": "ws://127.0.0.1:8546", "http_url": "http://127.0.0.1:8545" }
  ],
  "minimum_state_diff": 100,
  "sources_sync_timeout_ms": 1000,
  "sources_monitoring_period_ms": 500
}
```

Implementation notes:
- Replace sidecar MDBX depth/replica config with one embedded worker config block.
- Add env var resolution for embedded worker settings (`SIDECAR_STATE_WORKER_WS_URL`, `SIDECAR_STATE_WORKER_MDBX_PATH`, `SIDECAR_STATE_WORKER_FILE_TO_GENESIS`).
- Update default config, README schema/examples, docker config, and `run-sidecar-host.sh` to use the embedded worker model.
- Keep temporary legacy parsing only if required for rollout; if kept, map it into the new embedded config shape in one place.

- [ ] **Step 4: Run the sidecar args tests**

Run: `cargo test -p sidecar args::tests::test_state_config_resolves_embedded_worker_settings`

Expected: PASS with the new config shape documented and resolved correctly.

- [ ] **Step 5: Commit the config migration**

```bash
git add crates/sidecar/src/args/mod.rs crates/sidecar/default_config.json crates/sidecar/README.md docker/maru-besu-sidecar/config/credible-sidecar/grpc.config.json scripts/run-sidecar-host.sh crates/state-worker/src/cli.rs
git commit -m "refactor(sidecar): migrate to embedded worker config"
```

### Task 6: Add End-To-End Fallback Coverage And Run Verification

**Files:**
- Modify: `crates/sidecar/src/engine/tests.rs`
- Modify: `crates/sidecar/src/utils/test_drivers.rs`
- Modify: `crates/sidecar/src/state_sync/tests.rs`

- [ ] **Step 1: Write failing end-to-end fallback and latency-safety tests**

```rust
#[tokio::test]
async fn test_eth_rpc_fallback_when_worker_is_restarting() {
    let harness = SidecarHarness::with_restarting_worker().await;
    assert_eq!(harness.fetch_account(addr(0xAA)).await.unwrap().served_by, SourceName::EthRpcSource);
}

#[tokio::test]
async fn test_commit_head_processing_does_not_wait_for_mdbx_flush() {
    let harness = SidecarHarness::with_slow_flush().await;
    let started = Instant::now();
    harness.send_commit_head(101).await.unwrap();
    assert!(started.elapsed() < Duration::from_millis(50));
}
```

- [ ] **Step 2: Run the targeted sidecar tests to verify they fail**

Run: `cargo test -p sidecar test_eth_rpc_fallback_when_worker_is_restarting test_commit_head_processing_does_not_wait_for_mdbx_flush -- --exact`

Expected: FAIL until the harness can model worker restart/lag and the engine no longer couples commit head processing to flush latency.

- [ ] **Step 3: Implement any missing harness/test support and stabilize the integration**

Implementation notes:
- Extend `crates/sidecar/src/utils/test_drivers.rs` or `state_sync/tests.rs` helpers so tests can force worker lag, restart, and panics deterministically.
- Make sure restart rebuild behavior uses `mdbx_synced_through + 1` and rehydrates staged state before local MDBX becomes eligible again.
- Keep `EthRpcSource` configured in the test harness so fallback is exercised through real source ordering rather than mocks only.

- [ ] **Step 4: Run the verification suite and latency benchmark**

Run: `cargo test -p mdbx`

Expected: PASS

Run: `cargo test -p state-worker`

Expected: PASS

Run: `cargo test -p sidecar`

Expected: PASS

Run: `forge build --root testdata/mock-protocol`

Expected: PASS

Run: `SIDECAR_BENCH_TRANSPORT=mock RUST_LOG=error cargo bench -p sidecar --features bench-utils --bench avg_block`

Expected: Benchmark completes successfully and the post-change result is not materially worse than the pre-change baseline you recorded before Task 1.

- [ ] **Step 5: Commit the end-to-end coverage and final verification fixes**

```bash
git add crates/sidecar/src/engine/tests.rs crates/sidecar/src/utils/test_drivers.rs crates/sidecar/src/state_sync/tests.rs
git commit -m "test(sidecar): cover embedded worker fallback and restart"
```
