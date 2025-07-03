#!/bin/bash
# Script to run tests without full tests (Docker tests and integration tests)
# This provides a convenient way to skip Docker tests and integration tests

echo "Running tests without full tests (Docker and integration tests)..."
cargo test --no-default-features "$@"