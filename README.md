# `credible-sdk` monorepo

The credible sdk contains building blocks for running transactions against phylax assertions. Inside, you'll find crates for running assertion EVM code inside our revm extension, types used within said extension, cheatcodes for assertion specific functionality, the DA server for storing assertion code, and our CLI utility for testing assertions and submitting them to credible layer networks.

## Directory structure

1. **Core PCL Crates**

   - `pcl-core`: Core functionality including assertion DA, submission logic, auth, and project management
   - `pcl-cli`: Main CLI binary that provides the `pcl` command
   - `pcl-common`: Shared utilities and types used across PCL crates
   - `pcl-phoundry`: Foundry integration for building and testing Solidity assertions

2. **Data Availability Layer**

   - `da-server`: HTTP server that stores assertion bytecode and metadata
   - `da-client`: Client library for interacting with DA server
   - `da-core`: Core types and traits shared between DA components

3. **Testing Infrastructure**
   - `int-test-utils`: Utilities for integration testing including contract deployment helpers

### READMEs

For more detailed info on how to run, build and test the `credible-sdk` see:

- [Assertion DA](crates/assertion-da/README.md) - Documentation for the Data Availability server component
- [`pcl`](crates/pcl/README.md) - Documentation for phylax command line utilities
- [Assertion Executor](crates/assertion-executor/README.md) - Documentation for assertion execution
- [Integration Test Utils](crates/int-test-utils/README.md) - Documentation for integration testing utilities

## Phoundry Subtree Workflow

We vendor Foundry as a subtree at `vendor/phoundry`. This keeps local changes
inside the monorepo and makes it easy to pull upstream updates from
`foundry-rs/foundry`.

### One-time setup

```bash
git remote add foundry-upstream https://github.com/foundry-rs/foundry.git
```

### Update the subtree from upstream

```bash
git fetch foundry-upstream master
git subtree pull --prefix vendor/phoundry foundry-upstream master --squash
```

If there are conflicts, resolve them in `vendor/phoundry` and commit as usual in
`credible-sdk`.

`git subtree pull` merges upstream changes into the subtree while preserving
your local edits. No manual rebase is needed.

### Working locally

- Edit code under `vendor/phoundry` directly.
- Commit changes in `credible-sdk` (no separate repo needed).
- PCL picks up the subtree via path dependencies in `crates/pcl/phoundry`.

### Update checklist

1. `git status` (ensure a clean working tree)
2. `git fetch foundry-upstream master`
3. `git subtree pull --prefix vendor/phoundry foundry-upstream master --squash`
4. Resolve conflicts (if any), then commit in `credible-sdk`
5. Run `cargo check -p pcl-phoundry` or the usual test suite

### Notes

- We use `--squash` so the subtree comes in as a single commit.
- Upstream uses `master`, so pull from `foundry-upstream master`.

## Running and building the `assertion-da-server`

The assertion-da-server is a data availability layer that stores assertion bytecode and metadata, making them accessible via JSON-RPC. It requires a private key for signing assertions and can be run using Docker or Cargo:

```bash
# Run with Docker (requires DA_PRIVATE_KEY env var)
make compose

# Run with Docker in development mode
make compose-dev

# Run directly with cargo
cargo run --release --bin assertion-da -- --private-key <PRIVATE_KEY>
```

To build from source, use `make docker-build` for the production image or `cargo build --release --bin assertion-da` to build the binary directly. The server exposes a JSON-RPC API on port 5001 by default and supports methods like `da_submit_solidity_assertion` for storing compiled assertions and `da_get_assertion` for retrieval.

## Running and building PCL

PCL (Phylax Command Line) is the main CLI tool for working with assertions - it handles building, testing, storing, and submitting assertions to the Credible Layer network. Before submitting assertions, authenticate using `pcl auth login` which opens a browser for wallet-based authentication.

```bash
# Install PCL
cargo install --path crates/pcl-cli

# Or run directly without installing
cargo run --release --bin pcl -- <command>

# Build the binary
cargo build --release --bin pcl
```

Common workflows include:

```bash
pcl auth login                                     # Authenticate with wallet
pcl build                                          # Compile Solidity assertions
pcl test                                           # Run assertion tests
pcl store AssertionName                            # Upload to DA layer (no constructor args)
pcl store AssertionName 0x123... 100              # Upload to DA layer (with constructor args)
pcl submit AssertionName                          # Submit single assertion (no args)
pcl submit AssertionName 0x123... 100             # Submit single assertion (with args)
pcl submit -a "AssertionName1(0x123...,100)" -a "AssertionName2(arg1,arg2)"  # Submit multiple assertions
```

The CLI automatically detects assertion projects by looking for an `assertions/` directory or `foundry.toml` file.

## License

This repository is distributed under the Business Source License 1.1 (BUSL-1.1). The `LICENSE` file documents the licensor, scope, additional use grants, change date (currently 2029-06-18), and the MIT fallback license; maintainers will finalize any remaining placeholders as part of the release process.
