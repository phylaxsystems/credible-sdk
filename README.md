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
- [Assertion DA](docs/assertion-da.md) - Documentation for the Data Availability server component
- [`pcl`](docs/pcl.md) - Documentation for phylax command line utilities
- [Assertion Executor](docs/assertion-executor.md) - Documentation for assertion execution
- [Integration Test Utils](docs/int-test-utils.md) - Documentation for integration testing utilities

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
pcl store --assertion-contract AssertionName       # Upload to DA layer
pcl submit -p "Project Name" -a "AssertionName"    # Submit to dApp
```

The CLI automatically detects assertion projects by looking for an `assertions/` directory or `foundry.toml` file.