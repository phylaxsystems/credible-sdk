# Coding Conventions

**Analysis Date:** 2026-03-25

## Naming Conventions

**Files:**
- Use `snake_case.rs` for all Rust source files
- Module files: `mod.rs` for directory modules (e.g., `crates/sidecar/src/engine/mod.rs`)
- Test files: `tests.rs` co-located inside the module directory (e.g., `crates/sidecar/src/engine/tests.rs`)
- Test utility files: `test_utils.rs` or `test_util.rs` for test helper modules
- Benchmark files: `snake_case.rs` in `benches/` directories

**Types/Structs:**
- `PascalCase` for all types, structs, enums, and traits
- Error enums named `{Component}Error` (e.g., `EngineError`, `AssertionStoreError`, `DaClientError`)
- Config structs named `{Component}Config` (e.g., `CoreEngineConfig`, `ExecutorConfig`, `GrpcTransportConfig`)

**Functions:**
- `snake_case` for all functions and methods
- Constructor pattern: `new()` as the primary constructor, `new_test()` for test-only constructors, `new_ephemeral()` for in-memory variants
- Builder pattern: some configs use `build()` method (e.g., `ExecutorConfig::default().build(assertion_store)`)
- Test helper functions: prefix with purpose like `setup_`, `make_`, `mock_`, `assert_`

**Constants:**
- `SCREAMING_SNAKE_CASE` for all constants (e.g., `COUNTER_ADDRESS`, `HISTORY_BUFFER_LENGTH`, `MAX_ATTEMPTS`)
- String constants for artifact references: `"SimpleCounterAssertion.sol:Counter"` format

**Modules:**
- `snake_case` for all module names
- Public modules declared with `pub mod`
- Test modules gated with `#[cfg(test)]`
- Test utility modules gated with `#[cfg(any(test, feature = "test"))]` for cross-crate sharing

**Crate names:**
- Kebab-case for Cargo package names (e.g., `assertion-executor`, `da-server`, `pcl-core`)
- Workspace path aliases use kebab-case matching the crate name (e.g., `assertion-da-client = { path = "..." }`)

## Code Organization Patterns

**Crate `lib.rs` structure:**
1. Crate-level attributes (`#![allow(...)]`, `#![cfg_attr(...)]`, feature gates)
2. Module declarations (`pub mod ...`, `mod ...`)
3. Re-exports (`pub use ...`)

Example from `crates/assertion-executor/src/lib.rs`:
```rust
#![feature(unsafe_cell_access)]
#![feature(test)]
mod error;
pub use error::{ExecutorError, ForkTxExecutionError, TxExecutionError};

mod executor;
pub use executor::{AssertionExecutor, config::ExecutorConfig};

pub mod constants;
pub mod primitives;
// ...

#[cfg(any(test, feature = "test"))]
pub mod test_utils;
```

**Module organization:**
- Each module directory contains `mod.rs` with submodule declarations and public API
- Private submodules use `mod name;`, public ones use `pub mod name;`
- Test modules declared as `#[cfg(test)] mod tests;` in `mod.rs`

**Feature-gated test utilities:**
- Test utility code uses `#[cfg(any(test, feature = "test"))]` to be available both in-crate tests and for other crates that depend on it
- The `test` feature is used across multiple crates (e.g., `assertion-executor/test`, `sidecar/test`)
- Docker-dependent tests use `full-test` feature: `#[cfg(feature = "full-test")]`

**Crate-level clippy allows (standard pattern):**
Most crates allow these pedantic clippy lints at the crate root:
```rust
#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::unreadable_literal)]
#![allow(clippy::similar_names)]
#![allow(clippy::struct_field_names)]
```

**Test-scoped relaxation of strict clippy rules:**
The sidecar crate relaxes strict clippy rules only in test code:
```rust
#![cfg_attr(
    test,
    allow(
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        clippy::unreachable,
        clippy::unwrap_used
    )
)]
```

## Error Handling Conventions

**Error library:** `thiserror` (workspace version `2.0.17`)

**Error definition pattern:**
```rust
#[derive(thiserror::Error, Debug)]
pub enum ComponentError {
    #[error("Description of error: {0}")]
    VariantName(#[source] SourceError),
    #[error("Simple error message")]
    SimpleVariant,
}
```

**Error propagation:**
- Use `thiserror` for all error types -- `#[error]` for display, `#[source]` for chaining
- Production code: use `?` operator and `Result<T, E>` returns
- Test code: `.unwrap()` and `.expect()` are permitted (relaxed by clippy cfg_attr)
- `anyhow` is used in integration tests and scripts for ad-hoc error handling

**Strict clippy rules enforced at workspace level** (in `Cargo.toml`):
```toml
[workspace.lints.clippy]
too_many_lines = "deny"
too_many_arguments = "deny"
panic = "deny"
unwrap_used = "deny"
expect_used = "deny"
unreachable = "deny"
todo = "deny"
unimplemented = "deny"
indexing_slicing = "deny"
```

This means in production code:
- **Never use `.unwrap()` or `.expect()`** -- always propagate errors with `?` or handle explicitly
- **Never use `panic!()`, `todo!()`, `unimplemented!()`** in non-test code
- **Never use array indexing `arr[i]`** -- use `.get(i)` with proper handling
- **Keep functions short** -- denied by `too_many_lines`
- **Limit function parameters** -- denied by `too_many_arguments`

**Generic error types with database errors:**
The assertion-executor uses generic error types parameterized on database error types:
```rust
pub enum TxExecutionError<DbErr> where DbErr: Debug {
    #[error("Evm error: {0}")]
    TxEvmError(#[source] EVMError<DbErr>),
}
```

**Error recoverability pattern:**
The sidecar classifies errors as recoverable or unrecoverable:
```rust
impl From<&EngineError> for ErrorRecoverability {
    fn from(e: &EngineError) -> Self {
        match e {
            EngineError::DatabaseError => ErrorRecoverability::Unrecoverable,
            EngineError::TransactionError => ErrorRecoverability::Recoverable,
            // ...
        }
    }
}
```

## Common Patterns

| Pattern | Where Used | Example |
|---------|-----------|---------|
| Feature-gated test utilities | `assertion-executor`, `sidecar` | `#[cfg(any(test, feature = "test"))] pub mod test_utils;` |
| `full-test` feature for Docker tests | `da-server`, `pcl/core` | `#[cfg(all(feature = "full-test", target_arch = "x86_64"))]` |
| Ephemeral vs persistent backends | `AssertionStore`, `OverlayDb` | `AssertionStore::new_ephemeral()` for tests, sled-backed for production |
| Builder pattern for configs | `ExecutorConfig`, `CoreEngineConfig` | `ExecutorConfig::default().build(assertion_store)` |
| `async_trait` for async interfaces | Transport, StateProvider | `#[async_trait] pub trait Transport: Send + Sync { ... }` |
| Custom proc macro for tests | `sidecar` engine tests | `#[engine_test(all)]` generates per-transport test variants |
| Workspace lint inheritance | All crates | `[lints] workspace = true` in each `Cargo.toml` |
| `critical!` macro for severe errors | All crates via `credible-utils` | `critical!(alert = true, severity = "critical", ...)` wraps `tracing::error!` |
| `#[instrument]` for tracing spans | Core engine, executor, mdbx | `#[instrument(skip_all, ...)]` on key functions |
| `Arc<DashMap>` for concurrent state | `OverlayDb`, `Sources`, caches | Concurrent hash maps for shared mutable state |
| `#[must_use]` on constructors | Test utilities, public constructors | All public functions returning a value in test_utils |
| Solidity artifact loading | Test utilities | `bytecode("Contract.sol:Contract")` reads from `testdata/mock-protocol/out` |

## Import/Use Conventions

**Import organization (enforced by `rustfmt.toml`):**
```toml
imports_indent = "Block"
imports_layout = "Vertical"
imports_granularity = "Crate"
```

This means imports are:
1. Grouped by crate (one `use` per crate)
2. Each imported item on its own line (vertical layout)
3. Block-indented

**Import order (by convention, not enforced):**
1. `super::*` or local module imports
2. Internal crate imports (`crate::...`)
3. Workspace crate imports (`assertion_executor::...`, `sidecar::...`)
4. External crate imports (`revm::...`, `alloy::...`, `tokio::...`)
5. Standard library (`std::...`)

**Example from `crates/sidecar/src/transaction_observer/mod.rs`:**
```rust
use crate::utils::ErrorRecoverability;
use alloy::primitives::B256;
use dapp_api_client::Client as DappClient;
use futures_util::stream::{FuturesUnordered, StreamExt};
use revm::{
    context::{BlockEnv, TxEnv},
    primitives::{Address, Bytes, FixedBytes},
};
use std::{
    collections::HashSet,
    sync::{Arc, atomic::{AtomicBool, Ordering}},
};
use thiserror::Error;
use tracing::{debug, error, info, instrument, trace, warn};
```

**Path aliases:**
- No `use` path aliases in `Cargo.toml` -- crate names are used directly
- Workspace dependencies are referenced by name: `assertion-executor.workspace = true`

## Documentation Conventions

**Module-level docs:**
- Use `//!` doc comments at the top of `mod.rs` or `lib.rs`
- Include ASCII art diagrams for architectural modules (see `crates/sidecar/src/engine/mod.rs`, `crates/sidecar/src/transport/mod.rs`)
- Use `#![doc = include_str!("../README.md")]` for crate-level docs when a README exists

**Function/struct docs:**
- Use `///` doc comments
- Document `# Panics` section when a function can panic (required by clippy for public functions)
- Document `# Arguments` section for non-obvious parameters
- Generated code (`dapp-api-client/src/generated/`) is excluded from doc requirements

**API documentation (da-server):**
- JSON-RPC methods documented with request/response examples in module-level docs
- See `crates/assertion-da/da-server/src/api/mod.rs` for full example

## Formatting & Style

**rustfmt configuration (`rustfmt.toml`):**
```toml
format_code_in_doc_comments = true
force_multiline_blocks = true
imports_indent = "Block"
imports_layout = "Vertical"
imports_granularity = "Crate"
ignore = ["crates/dapp-api-client/src/generated/**"]
```

**Clippy configuration:**
- Run with `--profile dev -- -D warnings -D clippy::pedantic` (see `Makefile` lint target)
- Workspace-level strict denies in `Cargo.toml` (see Error Handling section above)
- Per-crate `#![allow(...)]` for specific pedantic rules that are too noisy

**Rust toolchain:**
- Nightly channel: `nightly-2026-01-07`
- Components: `rustfmt`, `clippy`, `rust-analyzer`, `rust-src`
- Edition: `2024`
- Uses nightly features: `unsafe_cell_access`, `test`, `allocator_api`

**Pre-commit workflow:**
```bash
cargo fmt && make lint
```

**Dependency management:**
- `cargo-deny` for advisory, license, and ban checks (configured in `deny.toml`)
- `cargo-shear` for detecting unused dependencies (configured in `workspace.metadata.cargo-shear`)
- All dependency versions pinned in workspace `Cargo.toml`
- Generated code excluded from formatting: `crates/dapp-api-client/src/generated/**`

---

*Convention analysis: 2026-03-25*
