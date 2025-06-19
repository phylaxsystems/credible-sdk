# Assertion Executor

This crate gives you all the tools necessary to execute assertions.

## Overview

This repository contains the following primary components:

- [`assertion-executor`]: This component is used for executing assertions against a provided state. 
- [`assertion-store`] - Store for assertions that will return assertions matching triggers collected from transaction execution.
- [`assertion-indexer`] - Indexes state oracle contracts, fetches assertion from da, and extracts the assertion contract details from the bytecode. Once these events are finalized the assertions are moved to the store.
- [`overlay-db`] - Database with a cacheing layer for a `RethDatabase`
- [`fork-db`] -  Database for representing state using an underlying `revm::Database` and a overlay of the state differences.
- [`phevm`] - Provides cheatcodes for assertion execution, using an Inspector.

[`assertion-executor`]: https://github.com/phylaxsystems/assertion-executor/blob/main/src/executor/mod.rs
[`assertion-store`]: https://github.com/phylaxsystems/assertion-executor/blob/main/src/store/assertion_store.rs
[`assertion-indexer`]: https://github.com/phylaxsystems/assertion-executor/blob/main/src/store/indexer.rs
[`overlay-db`]: https://github.com/phylaxsystems/assertion-executor/blob/main/src/db/overlay/mod.rs
[`fork-db`]: https://github.com/phylaxsystems/assertion-executor/blob/main/src/db/fork_db.rs
[`phevm`]: https://github.com/phylaxsystems/assertion-executor/blob/main/src/inspectors/phevm.rs

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