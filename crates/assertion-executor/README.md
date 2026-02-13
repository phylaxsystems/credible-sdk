# Assertion Executor

This crate gives you all the tools necessary to execute assertions.

## Installation

The easiest way to get started is to add the `assertion-executor` crate with the default features from the command-line using Cargo.

The default feature set executes for optimism.

```sh
cargo add assertion-executor --git ssh://git@github.com/phylaxsystems/credible-sdk.git 
```

Alternatively, you can add the following to your `Cargo.toml` file:

```toml
assertion-executor = { git = "ssh://git@github.com/phylaxsystems/credible-sdk.git", version = "0.1.0" }
```
Features List
* optimism - Supports optimism functionality.
* test - Provides several test utilities and convenience methods. 

## Overview

This repository contains the following primary components:

- [`assertion-executor`]: This component is used for executing assertions against a provided state. 
- [`assertion-store`] - Store for assertions that will return assertions matching triggers collected from transaction execution.
- [`assertion-indexer`] - Indexes state oracle contracts, fetches assertion from da, and extracts the assertion contract details from the bytecode. Once these events are finalized the assertions are moved to the store.
- [`overlay-db`] - Database with a cacheing layer for a `RethDatabase`
- [`fork-db`] -  Database for representing state using an underlying `revm::Database` and a overlay of the state differences.
- [`phevm`] - Provides cheatcodes for assertion execution, using an Inspector.
[`assertion-executor`]: https://github.com/phylaxsystems/credible-sdk/blob/main/crates/assertion-executor/src/executor/mod.rs
[`assertion-store`]: https://github.com/phylaxsystems/credible-sdk/blob/main/crates/assertion-executor/src/store/assertion_store.rs
[`assertion-indexer`]: https://github.com/phylaxsystems/credible-sdk/blob/main/crates/assertion-executor/src/store/indexer.rs
[`overlay-db`]: https://github.com/phylaxsystems/credible-sdk/blob/main/crates/assertion-executor/src/db/overlay/mod.rs
[`fork-db`]: https://github.com/phylaxsystems/credible-sdk/blob/main/crates/assertion-executor/src/db/fork_db.rs
[`phevm`]: https://github.com/phylaxsystems/credible-sdk/blob/main/crates/assertion-executor/src/inspectors/phevm.rs

## Indexer metrics

The assertion indexer emits the following Prometheus metrics:

| Metric | Type | Description |
| --- | --- | --- |
| `assertion_executor_indexer_head_block` | `gauge` | Latest block number indexed by the assertion indexer. During catch-up this advances until it reaches chain head. |
| `assertion_executor_indexer_is_syncing` | `gauge` (`0`/`1`) | `1` only while catching up through a backlog (for example after downtime). `0` during steady-state operation, including normal new-block processing. |
| `assertion_executor_indexer_assertions_seen_total` | `counter` | Total assertion events decoded from state oracle logs. |
| `assertion_executor_indexer_assertions_moved_total` | `counter` | Total assertions moved from pending modifications into the assertion store. |

## Fuzz testing

This repo contains fuzz tests for the executor precompiles/cheatcodes. To run them you need to install `cargo-fuzz`.
With `cargo-fuzz` they can be ran with:
```bash
cargo fuzz run logs_fuzz -- -max_len=200
```
From the top level assertion executor folder.

To view available fuzz tests see the `./fuzz/fuzz_targets` folder.

## Supported Rust Versions (MSRV)
Currently this library requires nightly rust. 
