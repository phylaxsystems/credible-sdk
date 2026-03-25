# Testing Patterns

**Analysis Date:** 2026-03-25

## Test Framework

**Runner:**
- `cargo nextest` (primary, used in CI and Makefile)
- `cargo test` (fallback, used in `scripts/test-no-full.sh`)

**Assertion Library:**
- Standard `assert!`, `assert_eq!`, `assert_ne!` macros
- Pattern matching with `assert!(matches!(...))` for error variant checks
- `anyhow::Result` used as return type in integration tests for `?` propagation

**Run Commands:**
```bash
# Run all tests (requires Docker, Foundry, builds contracts first)
make test

# Run default EVM tests (excludes mdbx, state-worker)
make test-default

# Run optimism-featured tests
make test-optimism

# Run state-worker tests (single-threaded, 2 threads)
make test-state-worker

# Run tests without Docker/integration tests
make test-no-full

# Run specific crate tests
cargo nextest run --package sidecar --no-default-features --features test

# Run benchmarks
cargo bench -p sidecar --features bench-utils --benches
cargo bench --manifest-path crates/assertion-executor/Cargo.toml --features test --benches

# Build contracts required by tests
make build-contracts

# Lint (effectively a test gate)
make lint

# Format check
make format
```

## Test File Organization

**Location:**
- Unit tests: co-located inside source files in `#[cfg(test)] mod tests { ... }` blocks
- Dedicated test files: `tests.rs` in the same directory as `mod.rs` (e.g., `crates/sidecar/src/engine/tests.rs`)
- Integration tests: `tests/` directory at crate root (e.g., `crates/dapp-api-client/tests/auth_tests.rs`, `crates/pcl/core/tests/da_store_int_tests.rs`)
- Integration test sub-modules: `crates/state-worker/src/integration_tests/` (inside `src/` but gated with `#[cfg(test)]`)

**Naming:**
- Test modules: `tests.rs` or `tests` submodule
- Test utility files: `test_utils.rs`, `test_util.rs`, `test_drivers.rs`
- Integration test files: descriptive names like `auth_tests.rs`, `da_store_int_tests.rs`
- Test helper common modules: `tests/common/mod.rs`

**Structure:**
```
crates/sidecar/
  src/
    engine/
      mod.rs                    # Module with #[cfg(test)] mod tests;
      tests.rs                  # Large test file (~4315 lines)
    utils/
      test_util.rs              # Test initialization (tracing setup)
      test_drivers.rs           # Test transport drivers
      test-macros/              # Proc macro crate for #[engine_test]
        src/lib.rs
  benches/
    worst_case_compute.rs       # Criterion benchmarks
    avg_block.rs

crates/assertion-executor/
  src/
    test_utils.rs               # Shared test utilities (behind feature flag)
    db/overlay/test_utils.rs    # MockDb implementation
  benches/
    worst-case-op.rs

crates/dapp-api-client/
  tests/
    auth_tests.rs               # HTTP mock-based integration tests
    common/mod.rs               # Shared test server setup

crates/pcl/core/
  tests/
    da_store_int_tests.rs       # Docker-dependent integration tests
    common/
      da_store_harness.rs       # Test harness/setup

crates/state-worker/
  src/
    integration_tests/
      mod.rs
      setup.rs                  # TestInstance setup
      mdbx_fixture.rs           # MDBX temp directory fixture
      tests.rs                  # Actual integration tests

crates/mdbx/
  src/
    tests.rs                    # ~4681 lines of unit/integration tests
```

## Test Types Present

| Type | Location | Framework | Count (approx) |
|------|----------|-----------|-----------------|
| Unit tests | `#[cfg(test)] mod tests` in ~91 source files | `#[test]` / `#[tokio::test]` | ~887 `#[test]` + ~245 `#[tokio::test]` |
| Integration tests | `crates/*/tests/`, `integration_tests/` dirs | `#[tokio::test]`, Docker, testcontainers | ~30-50 |
| Benchmarks | `crates/*/benches/*.rs` | Criterion | 11 benchmark files |
| Fuzz tests | `fuzz/fuzz_targets/*.rs` | `libfuzzer-sys` | 3 fuzz targets |

## Test Infrastructure

**Test initialization (tracing):**
`crates/sidecar/src/utils/test_util.rs` uses `#[ctor::ctor]` to set up tracing before any tests run:
```rust
#[ctor::ctor]
fn init_tests() {
    // Reads TEST_TRACE env var to enable debug/trace logging in tests
    // Filters out noisy crates: sled, h2, hyper, tonic::transport
}
```
Enable test tracing with: `TEST_TRACE=debug cargo test ...`

**`#[engine_test]` proc macro:**
`crates/sidecar/src/utils/test-macros/src/lib.rs` generates per-transport test variants:
```rust
#[engine_test(all)]
async fn test_something(mut instance: LocalInstance) {
    instance.new_block().unwrap();
}
// Generates: mock_test_something() and grpc_test_something()
```
- Each variant creates a `LocalInstance` with the respective transport driver
- Tests are wrapped with a 10-second timeout
- Runs with `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]`

**Test drivers (`crates/sidecar/src/utils/test_drivers.rs`):**
- `LocalInstanceMockDriver` -- uses `MockTransport` (no network, direct channel)
- `LocalInstanceGrpcDriver` -- uses real gRPC transport on localhost
- Both implement `TestTransport` trait

**Solidity test artifacts:**
Tests depend on compiled Solidity contracts from `testdata/mock-protocol/`:
- Built with `forge build --root testdata/mock-protocol`
- Loaded via `assertion_executor::test_utils::bytecode("Contract.sol:ContractName")`
- Artifacts read from `testdata/mock-protocol/out/{FileName}/{ContractName}.json`
- **Must be pre-built before running tests** (handled by `make build-contracts`)

**Feature flags controlling test scope:**

| Feature | Purpose | Used By |
|---------|---------|---------|
| `test` | Enables test utility modules and test-only dependencies | `assertion-executor`, `sidecar` |
| `full-test` | Enables Docker-dependent integration tests | `da-server`, `pcl/core`, `assertion-executor` |
| `bench-utils` | Enables benchmark utility code | `sidecar` |
| `optimism` | Runs tests with Optimism EVM variant | `sidecar`, `assertion-executor` |

## Test Structure

**Unit test pattern (inline):**
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_specific_behavior() {
        let component = Component::new_test();
        let result = component.do_thing();
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_async_behavior() {
        // ...
    }
}
```

**Engine test pattern (using proc macro):**
```rust
#[engine_test(all)]
async fn test_transaction_processing(mut instance: LocalInstance) {
    instance.new_block().unwrap();
    let result = instance.send_counter_call().await.unwrap();
    assert!(result.is_valid);
}
```

**Integration test pattern (with test harness):**
```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_state_worker_processes_changes() -> Result<()> {
    let instance = TestInstance::new_mdbx().await.map_err(anyhow::Error::msg)?;
    // ... test logic using instance ...
    Ok(())
}
```

**Docker-gated integration test pattern:**
```rust
#![cfg(all(feature = "full-test", target_arch = "x86_64"))]

#[tokio::test]
#[cfg(feature = "full-test")]
async fn test_da_store_works() {
    let test_setup = TestSetup::new();
    let mut test_runner = test_setup.build().await.unwrap();
    let res = test_runner.run().await;
    assert!(res.is_ok());
}
```

## Mocking

**MockDb (`crates/assertion-executor/src/db/overlay/test_utils.rs`):**
A hand-rolled mock database implementing `Database`, `DatabaseRef`, and `DatabaseCommit`:
```rust
#[derive(Debug, Default, Clone)]
pub struct MockDb {
    accounts: HashMap<Address, AccountInfo>,
    contracts: HashMap<B256, Bytecode>,
    storage: HashMap<Address, HashMap<U256, U256>>,
    block_hashes: HashMap<u64, B256>,
    basic_calls: Arc<Mutex<u64>>,
    // ...
}
```
- Provides `insert_account()`, `insert_storage()`, `insert_block_hash()` for setup
- Provides `get_basic_calls()`, etc. for verifying call counts

**HTTP mocking (`httpmock`):**
Used in `crates/dapp-api-client/tests/`:
```rust
let server = try_start_mock_server();
let mock = server.mock(|when, then| {
    when.method(GET).path("/api/v1/projects");
    then.status(200)
        .header("content-type", "application/json")
        .json_body(json!([...]));
});
mock.assert();
```

**DualProtocolMockServer (`crates/int-test-utils/src/node_protocol_mock_server.rs`):**
Provides mock HTTP and WebSocket servers for simulating blockchain node protocols. Used by `state-worker` integration tests.

**MockTransport (`crates/sidecar/src/transport/mock.rs`):**
A mock implementation of the `Transport` trait for testing the engine without network I/O.

**Ephemeral stores:**
- `AssertionStore::new_ephemeral()` -- in-memory HashMap-backed store (no sled)
- `OverlayDb::new_test()` -- test overlay using `InMemoryDB`

**What to mock:**
- External database backends (use `MockDb`, ephemeral stores)
- Network protocols (use `MockTransport`, `httpmock`, `DualProtocolMockServer`)
- Blockchain nodes (use Anvil via `alloy_node_bindings::Anvil`)

**What NOT to mock:**
- Core EVM execution logic (tests run real REVM)
- Assertion validation pipeline (tests exercise full execution path)
- Solidity contracts (compiled from `testdata/mock-protocol/`, executed in real EVM)

## Fixtures and Test Data

**Solidity contract artifacts:**
```
testdata/mock-protocol/
  assertions/          # Assertion contract sources
  src/                 # Protocol contract sources
  lib/                 # Dependencies (forge-std, openzeppelin, credible-std)
  out/                 # Compiled artifacts (JSON with bytecode)
  foundry.toml         # Foundry configuration
```

**Loading artifacts in tests:**
```rust
const ARTIFACT_ROOT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../testdata/mock-protocol/out"
);

pub fn bytecode(input: &str) -> Bytes {
    // input format: "FileName.sol:ContractName"
    let value = read_artifact(input);
    hex::decode(value["bytecode"]["object"].as_str().unwrap()).into()
}
```

**Test setup structs:**
- `CounterValidationSetup` in `crates/assertion-executor/src/test_utils.rs`
- `TestSetup` / `TestRunner` in `crates/pcl/core/tests/common/da_store_harness.rs`
- `TestInstance` in `crates/state-worker/src/integration_tests/setup.rs`
- `LocalInstance` in `crates/sidecar/src/utils/instance.rs`

**Tempfile usage:**
- `tempfile::TempDir` for ephemeral MDBX databases in tests
- `MdbxTestDir` wrapper in state-worker for managed cleanup

## Coverage

**Requirements:** No coverage threshold enforced.

**Well-tested areas:**
- Core engine (`crates/sidecar/src/engine/tests.rs` -- 4315 lines)
- Event sequencing (`crates/sidecar/src/event_sequencing/tests.rs` -- 5161 lines)
- MDBX storage (`crates/mdbx/src/tests.rs` -- 4681 lines)
- Assertion executor precompiles (multiple test modules in `crates/assertion-executor/src/inspectors/precompiles/legacy/`)
- Event metadata (`crates/sidecar/src/event_sequencing/event_metadata/tests.rs` -- 1689 lines)
- Dapp API client auth (`crates/dapp-api-client/tests/auth_tests.rs`)
- PCL config/submission (`crates/pcl/core/src/config.rs`, `assertion_submission.rs`)

**Coverage gaps:**
- Shadow-driver (`crates/shadow-driver/`) -- minimal test files
- Verifier-service (`crates/verifier-service/`) -- limited coverage
- EIP system calls (`crates/eip-system-calls/`) -- no dedicated test files

## CI Testing

**Main CI workflow (`.github/workflows/rust-test-lint.yml`):**
- Triggers on: push to `main`, PRs to `main` (when crates/Cargo files change)
- Runner: `big-bois-x86` (custom runner group)
- Uses shared workflow: `phylaxsystems/actions/.github/workflows/rust-base.yaml@main`
- Two feature sets tested:
  - `--no-default-features --features test`
  - `--no-default-features --features test --features optimism`
- Excludes `mdbx` and `state-worker` packages (run separately)
- Runs `cargo deny check advisories`
- Uses `sccache` for build caching

**State worker tests (separate job):**
- Runs with `--test-threads=2` to avoid race conditions in MDBX

**Benchmark CI (`.github/workflows/benchmarks.yml`):**
- Triggers on: PRs touching sidecar/assertion-executor, push to main
- Runs Criterion benchmarks, compares against main baseline
- Posts benchmark results as PR comments
- Uses stable Rust (not nightly)

## Common Test Patterns

**Async testing:**
```rust
#[tokio::test]
async fn test_async_operation() {
    let result = some_async_fn().await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_operations() -> Result<()> {
    // ...
    Ok(())
}
```

**Error testing:**
```rust
assert!(matches!(
    result,
    Err(DaSubmitError::PhoundryError(ref boxed_error))
    if matches!(**boxed_error, PhoundryError::DirectoryNotFound(_))
));
```

**Precompile test helper pattern:**
```rust
pub fn run_precompile_test(artifact: &str) -> TxValidationResult {
    // 1. Create overlay DB and fork
    // 2. Load assertion bytecode from artifact
    // 3. Insert into ephemeral assertion store
    // 4. Build executor
    // 5. Deploy target contract
    // 6. Execute triggering transaction
    // 7. Return validation result
}
```

**Anvil provider setup:**
```rust
pub async fn anvil_provider() -> (RootProvider<Ethereum>, AnvilInstance) {
    let anvil = Anvil::new().spawn();
    let provider = ProviderBuilder::new()
        .connect_ws(WsConnect::new(anvil.ws_endpoint()))
        .await.unwrap();
    provider.anvil_set_auto_mine(false).await.unwrap();
    (provider.root().clone(), anvil)
}
```

**Test cleanup:**
```bash
make test-cleanup
# Removes Docker containers from: redis:5.0, solc images
```

---

*Testing analysis: 2026-03-25*
