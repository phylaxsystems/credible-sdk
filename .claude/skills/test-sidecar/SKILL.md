---
name: test-sidecar
description: Run formatting, linting, and tests for sidecar features after completing a task.
---

# Post-task validation
Run these steps in order after every task is complete.
- `cargo fmt`
- `make lint`
- `forge build --root testdata/mock-protocol && FOUNDRY_PROFILE=assertions forge build --root testdata/mock-protocol`
- `cargo nextest run --workspace --exclude mdbx --exclude state-worker --no-tests=warn --no-default-features --features test`

To run a specific test: `cargo nextest run -E 'test($ARGUMENTS)' --no-default-features --features test`

## Notes

- The `test` feature flag is required for all test runs.
- Tests use `#[engine_test(all)]` macro which generates `mock` and `grpc` transport variants. Use `engine_test` whenever you need a to instantiate a sidecar,
- For test debug output prefix test commands with `TEST_TRACE=debug`.
- State worker tests run separately and single-threaded: `cargo nextest run --package mdbx --package state-worker --test-threads=2`. Only run these if touching state-worker or read component in sidecar.