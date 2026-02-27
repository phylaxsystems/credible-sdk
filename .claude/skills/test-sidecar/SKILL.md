---
name: test-sidecar
description: fmt→lint→test ∀ task completion
---

Run in order ∀ task done:
① `cargo fmt`
② `make lint`
③ `forge build --root testdata/mock-protocol && FOUNDRY_PROFILE=assertions forge build --root testdata/mock-protocol`
④ `cargo nextest run --workspace --exclude mdbx --exclude state-worker --no-tests=warn --no-default-features --features test`

Specific test: `cargo nextest run -E 'test($ARGUMENTS)' --no-default-features --features test`

`test` feature flag required ∀ test runs.
`#[engine_test(all)]` → generates `mock` ∧ `grpc` variants. Use ∀ sidecar instantiation.
Debug: prefix w∕ `TEST_TRACE=debug`.
State worker → separate ∧ single-threaded: `cargo nextest run --package mdbx --package state-worker --test-threads=2`. Only if touching state-worker ∨ read component.
