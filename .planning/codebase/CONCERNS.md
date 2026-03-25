# Technical Concerns

**Analysis Date:** 2026-03-25

## Technical Debt

| Area | Description | Severity | Evidence |
|------|-------------|----------|----------|
| VersionDb cloning | `VersionDb` rebuilds state by cloning the entire `ForkDb` base state and replaying commits. The author acknowledges unnecessary clones and suggests replacing with interior mutability. | High | `crates/assertion-executor/src/db/version_db.rs:43-45` - FIXME comment: "we have unnecessary clones for state data" |
| MultiForkDb journal hack | `MultiForkDb` needs a hack to copy journal entries for revert correctness on forked state. This is an acknowledged incomplete abstraction. | High | `crates/assertion-executor/src/db/multi_fork_db.rs:52-54` - FIXME: "We need to add copy the first two entries in the journal to each new journal" and `:334` - FIXME: "this is a hack to fill the journal" |
| Hard-coded assertion paths | The build system hard-codes assertion file locations to `assertions/src` in two separate places, duplicating the path assumption. | Medium | `crates/pcl/phoundry/src/build_and_flatten.rs:245-247` and `crates/pcl/phoundry/src/build.rs:41-43` - identical FIXME comments |
| Incomplete preview/apply flow | The `pcl apply` command has ~60 lines of commented-out preview/diff logic and confirmation prompts awaiting backend implementation. Dead code functions `render_preview`, `confirm_apply`, and `preview_has_changes` exist but are unused. | Medium | `crates/pcl/core/src/apply.rs:217-261` (commented-out code), `:532-617` (dead code with `#[allow(dead_code)]`) |
| Separate incident database | The transaction observer opens a separate MDBX database for incident reports rather than sharing a global sidecar MDBX instance, causing resource duplication. | Medium | `crates/sidecar/src/transaction_observer/db.rs:30-31` - TODO: "we should have a global sidecar mdbx" |
| Missing prev tx hash propagation | At least 5 locations in test utilities mark the previous transaction hash as needing proper propagation, suggesting the data flow is incomplete. | Medium | `crates/sidecar/src/utils/instance.rs:965,1168,1212,1388,1399` - repeated FIXME: "Propagate correctly the prev tx hash" |
| Missing DappSubmit tests | The dapp assertion submission module has TODO comments noting tests are missing for core submission functionality. | Medium | `crates/pcl/core/src/assertion_submission.rs:50,440` - TODO comments requesting tests |
| Crate-wide `#[allow(unused)]` | The sidecar crate suppresses all unused-code warnings crate-wide, masking potentially dead code across ~20 modules. | Medium | `crates/sidecar/src/lib.rs:17` - `#![allow(unused)]` |
| PCL project module gated | A project module is commented out and noted as not ready for release. | Low | `crates/pcl/core/src/lib.rs:31` - TODO: "Add project module once tested, polished, added to cli" |
| SourceName manual Display | `SourceName` enum implements `Display` manually instead of deriving it, noted for strum migration. | Low | `crates/sidecar/src/cache/sources/mod.rs:122` - FIXME: "Derive strum" |
| Test macro transport limitation | The `engine_test` macro only generates tests for a single transport, though multi-transport testing is desired. | Low | `crates/sidecar/src/utils/test-macros/src/lib.rs:50` - TODO |

## Security Considerations

**Unsafe code in ActiveOverlay:**
- `ActiveOverlay` wraps a database in `Arc<UnsafeCell<Db>>` and manually implements `Send`/`Sync`. The documentation explicitly warns: "The use of the active overlay may result in undefined behaviour if the `active_db` is holding a refrance [sic] that is not valid anymore. There are no protections for this."
- Files: `crates/assertion-executor/src/db/overlay/active_overlay.rs:42-53`
- Multiple unsafe dereferences throughout the file at lines 103, 130, 167, 179, 202, 318, 348, 373
- Risk: UB if the underlying database reference is invalidated while `ActiveOverlay` is in use.

**Unsafe gas manipulation in EVM:**
- The `reprice_gas` macro uses raw pointer manipulation (`*mut Gas`) to temporarily replace the interpreter's gas with `u64::MAX` to disable gas metering during repriced operations, then restores it.
- Files: `crates/assertion-executor/src/evm/build_evm.rs:201-206`
- Risk: If the operation panics between replacing and restoring gas, the EVM state is corrupted.

**Unsafe `std::env::set_var` usage:**
- `std::env::set_var`/`remove_var` are used in build scripts and tests. These are inherently unsafe in Rust 2024 edition (which this project uses) and are correctly wrapped in `unsafe` blocks.
- Files: `crates/sidecar/build.rs:13-27`, `crates/dapp-api-client/src/config.rs:216-303`, `crates/sidecar/src/args/mod.rs:770-795`
- Tests that mutate env vars use a mutex `ENV_LOCK` for serialization, which is the correct pattern.

**Advisory suppression backlog:**
- `deny.toml` suppresses 10+ security advisories, several of which are recent (RUSTSEC-2026-*) and include:
  - `RUSTSEC-2026-0047`: aws-lc-sys PKCS7_verify signature bypass
  - `RUSTSEC-2026-0046`: aws-lc-sys PKCS7_verify certificate chain bypass
  - `RUSTSEC-2026-0045`: aws-lc-sys AES-CCM timing side-channel
  - `RUSTSEC-2026-0049`: rustls-webpki CRL matching logic
- Files: `deny.toml:90-98`
- Impact: These are transitively pulled in; fixes depend on upstream releases (rustls, testcontainers).

**Panicking in production code paths:**
- `crates/pcl/common/src/utils.rs:30` - `panic!("Failed to find artifact for {}")` in `read_artifact()`, which is called at startup. Multiple `.expect()` calls on the same code path (lines 26, 27, 42, 51, 62, 68).
- `crates/verifier-service/src/rpc.rs:179,181` - `unwrap_or_else(|err| panic!(...))` when opening/parsing artifact files.
- `crates/assertion-verification/src/lib.rs:277,279` - Same pattern.
- `crates/state-checker/src/main.rs:82` - `expect()` on crypto provider install.
- `crates/shadow-driver/src/main.rs:18` - `expect()` on crypto provider install.

**sled alpha dependency:**
- The project uses `sled = "1.0.0-alpha.124"` for persistent storage in assertion-da and assertion-executor. The `deny.toml` also suppresses `RUSTSEC-2018-0017` (tempdir unmaintained in sled) and `RUSTSEC-2023-0018` (old remove_dir_all in tempdir/sled).
- Files: `Cargo.toml:89` (workspace dep), crates: `assertion-da/da-client`, `assertion-da/da-server`, `assertion-executor`, `sidecar`

## Performance Considerations

**VersionDb state rebuild on rollback:**
- Rolling back N transactions requires cloning the entire base `ForkDb` state and replaying up to N-1 commits. Each replay calls `.commit(delta.clone())`, cloning the entire `EvmState` HashMap.
- Files: `crates/assertion-executor/src/db/version_db.rs:46-52`
- Impact: O(N * state_size) for rollbacks. With large EVM states this could be significant during chain reorgs.
- Fix: Replace cloning with copy-on-write or persistent data structures (e.g., `im` crate immutable maps).

**Excessive cloning in overlay DB:**
- The `OverlayDb` and `ActiveOverlay` clone `AccountInfo`, `Bytecode`, and `U256` values on every cache miss and cache population. Some values like bytecode can be large.
- Files: `crates/assertion-executor/src/db/overlay/mod.rs:241,257,294,414,420`, `crates/assertion-executor/src/db/overlay/active_overlay.rs:97,113,138`
- Impact: Memory allocation overhead per EVM database lookup on cache miss.

**Large test files:**
- `crates/sidecar/src/event_sequencing/tests.rs` is 5,161 lines
- `crates/mdbx/src/tests.rs` is 4,681 lines
- `crates/sidecar/src/engine/tests.rs` is 4,315 lines
- These files are slow to compile and difficult to navigate.

**DashMap contention in ActiveOverlay:**
- The overlay cache uses `DashMap` (sharded concurrent hashmap) shared across multiple `ActiveOverlay` instances. Under high contention with many concurrent assertion executions, shard-level locking could become a bottleneck.
- Files: `crates/assertion-executor/src/db/overlay/active_overlay.rs:49`

## Complexity Hotspots

| File/Module | Lines | Complexity Indicator | Risk |
|-------------|-------|---------------------|------|
| `crates/sidecar/src/event_sequencing/tests.rs` | 5,161 | Largest file in codebase; test-only but hard to maintain | Low |
| `crates/mdbx/src/tests.rs` | 4,681 | Very large test file | Low |
| `crates/sidecar/src/engine/tests.rs` | 4,315 | Very large test file with many `panic!` assertions | Low |
| `crates/assertion-executor/src/store/assertion_store.rs` | 2,257 | Complex storage layer with dual backends (in-memory + sled) | Medium |
| `crates/sidecar/src/cache/mod.rs` | 2,177 | State caching with RwLock coordination | Medium |
| `crates/sidecar/src/engine/mod.rs` | 2,081 | Core engine with transaction processing, assertion execution, incident reporting | High |
| `crates/shadow-driver/src/listener.rs` | 1,967 | Complex event listener with no unit tests | High |
| `crates/assertion-executor/src/inspectors/tracer.rs` | 1,899 | Call tracing inspector, core to assertion execution | Medium |
| `crates/sidecar/src/args/mod.rs` | 1,589 | Configuration parsing with 68 `.unwrap()` calls (mostly in tests) | Medium |
| `crates/shadow-driver/src/mdbx_store.rs` | 1,143 | MDBX serialization with no unit tests | High |
| `crates/sidecar/src/utils/instance.rs` | 1,544 | Test helpers with 6 FIXME comments about prev tx hash propagation | Medium |
| `crates/assertion-executor/src/db/overlay/active_overlay.rs` | 1,030 | Unsafe code with UnsafeCell and manual Send/Sync | High |

## Dependency Risks

**sled 1.0.0-alpha.124:**
- Alpha release used in production storage paths (assertion store, DA server).
- Has known suppressed advisories (RUSTSEC-2018-0017, RUSTSEC-2023-0018).
- API may change before 1.0 stable.
- Files: `Cargo.toml:89`

**solar-* crates pinned via `[patch.crates-io]`:**
- Eight `solar-*` crates are patched to a specific git revision (`1f28069`) from `paradigmxyz/solar.git`. Comment says "Temporary due to a requirement in phoundry."
- Files: `Cargo.toml:215-222`
- Risk: Patch overrides affect all crates in the workspace; forgetting to remove when upstream catches up can cause subtle version conflicts.

**evm-glue pinned to git rev:**
- `evm-glue` depends on a specific git commit (`6be3e8c`) from a third-party repo (`Philogy/evm-glue`).
- Files: `Cargo.toml:163`
- Risk: If the repo is deleted or force-pushed, builds break. No crates.io fallback.

**reth dependencies pinned to git tag:**
- `reth-db`, `reth-db-api`, `reth-libmdbx` are pinned to `v1.9.3` tag from `paradigmxyz/reth`.
- Files: `Cargo.toml:164-166`
- Risk: Not published to crates.io; requires git fetch on every clean build.

**Nightly Rust toolchain:**
- The project requires `nightly-2026-01-07` and uses unstable features: `unsafe_cell_access`, `test`, `allocator_api`.
- Files: `rust-toolchain.toml:2`, `crates/assertion-executor/src/lib.rs:1-3`
- Risk: Nightly features can break or change semantics between releases. The pinned date mitigates but prevents receiving compiler fixes.

**Multiple suppressed RUSTSEC advisories:**
- 10+ advisories suppressed in `deny.toml:72-98`, including 5 from 2026 (aws-lc-sys and rustls issues).
- Some depend on upstream PR merges (noted in comments).

## Missing or Weak Areas

**Test coverage gaps:**

| Crate | Lines of Code | Test Coverage |
|-------|---------------|---------------|
| `shadow-driver` | 3,332 | 2 tests (both in `tx_env.rs`); `listener.rs` (1,967 lines) and `mdbx_store.rs` (1,143 lines) have zero unit tests |
| `eip-system-calls` | 171 | 0 tests |
| `credible-utils` | 31 | 0 tests (small utility crate, low risk) |
| `assertion-da/da-client` | ~700 | Tests exist but they are integration tests depending on a running DA server |
| `assertion-da/da-core` | small | 0 tests |
| `pcl/common` | small | Limited tests (1 file) |
| `verifier-service` | ~200 | 7 tests, but relies on `unwrap_or_else(panic!)` in production paths |

**Panicking utility functions in production paths:**
- `crates/pcl/common/src/utils.rs` uses `expect()` and `panic!()` throughout `read_artifact()`, `bytecode()`, `compilation_target()`, and `compiler_version()`. These are called during PCL build/submit flows and will crash the CLI on malformed artifacts instead of returning errors.
- Fix: Convert all `expect()`/`panic!()` to `Result` returns. The comment "We allow panics as this should be invoked at startup time" is not a strong justification for a CLI tool.

**Dead code suppression:**
- `crates/sidecar/src/lib.rs:17` - `#![allow(unused)]` suppresses all unused warnings for the entire sidecar crate. This is the largest crate in the project (~15,000+ lines). Dead code could accumulate unnoticed.
- `crates/pcl/core/src/assertion_submission.rs:53` - `#[allow(dead_code)]` on the `Project` struct.
- `crates/sidecar/src/cache/sources/mdbx/mod.rs:1` - `#![cfg_attr(not(test), allow(dead_code))]`

**Broad clippy lint suppressions:**
- Multiple crates suppress important clippy lints at the crate level:
  - `crates/int-test-utils/src/lib.rs:1-7` - suppresses 7 clippy lints including `collapsible_if` and `unused_async`
  - `crates/dapp-api-client/src/lib.rs:11` - `#[allow(clippy::pedantic)]` on generated module
  - `crates/sidecar/src/transport/grpc/mod.rs:82` - `#[allow(clippy::too_many_lines)]` on specific function
  - `crates/pcl/core/src/lib.rs:1-4` - suppresses 4 clippy lints
- The workspace `Cargo.toml` has strict deny-level clippy lints (`unwrap_used`, `expect_used`, `panic`, `todo`, `indexing_slicing`), but per-crate `#[allow]` attributes override these protections.

**TODO/FIXME/HACK inventory (non-test code):**
- 12 TODO comments across production code
- 10 FIXME comments across production code
- Key items:
  - `crates/pcl/core/src/apply.rs:217` - ENG-2129: Preview endpoint incomplete
  - `crates/sidecar/src/engine/mod.rs:887` - "TODO: make this error" (currently silently returns)
  - `crates/assertion-executor/src/executor/mod.rs:809` - "Why do we need to set the balance to max?"
  - `crates/sidecar/src/utils/instance.rs:691` - "commit tx numbers to this map!"

## Scalability Concerns

**VersionDb commit log growth:**
- The `commit_log: Vec<Option<EvmState>>` grows linearly with every committed transaction. There is no compaction or pruning until `checkpoint()` is called, which replaces base_state. Between checkpoints, all historical state deltas are held in memory.
- Files: `crates/assertion-executor/src/db/version_db.rs:30-31`
- Risk: Long-running sessions without checkpoints accumulate unbounded memory usage.

**Single-threaded engine transaction processing:**
- The `CoreEngine` processes transactions sequentially over a channel. Block processing, assertion execution, and incident reporting all happen in this pipeline.
- Files: `crates/sidecar/src/engine/mod.rs`
- Risk: Transaction throughput is bounded by single-thread performance. Assertion execution for each transaction adds latency.

**Incident DB sync-on-write:**
- Every incident report triggers an immediate `fsync` via `self.env.sync(true)`. Under high incident rates this could become an I/O bottleneck.
- Files: `crates/sidecar/src/transaction_observer/db.rs:117`
- Fix: Batch incident writes or use async fsync.

## Documentation Gaps

**Crate-level documentation missing:**
- 20+ `lib.rs` and `mod.rs` files lack `//!` module documentation
- Key undocumented crates: `assertion-executor`, `assertion-da/da-client`, `assertion-da/da-server`, `mdbx`, `pcl/core`, `pcl/common`, `pcl/phoundry`, `credible-utils`
- The `sidecar` crate uses `#![doc = include_str!("../README.md")]` which is good practice

**No architecture documentation:**
- No `ARCHITECTURE.md` or equivalent exists describing how the ~20 crates relate to each other
- The inter-crate dependency graph is complex (sidecar depends on assertion-executor, mdbx, state-worker patterns, etc.)

**Unsafe code documentation:**
- `ActiveOverlay` has a brief comment but no formal safety invariants or SAFETY comments on individual `unsafe` blocks
- The `reprice_gas` macro lacks documentation of its safety requirements
- Files: `crates/assertion-executor/src/db/overlay/active_overlay.rs`, `crates/assertion-executor/src/evm/build_evm.rs:190-208`

---

*Concerns audit: 2026-03-25*
