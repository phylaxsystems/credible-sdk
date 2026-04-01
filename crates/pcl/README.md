# Phylax Credible Layer (PCL) CLI

<!-- [![Tests, Linting, Format](https://github.com/phylaxsystems/credible-sdk/actions/workflows/rust-base.yml/badge.svg)](https://github.com/phylaxsystems/credible-sdk/actions/workflows/rust-base.yml) -->

The Phylax Credible CLI (PCL) is a command-line interface for interacting with the Credible Layer. It allows developers to authenticate, build, test, and submit assertions to the Credible Layer Platform.

## Table of Contents

- [Phylax Credible Layer (PCL) CLI](#phylax-credible-layer-pcl-cli)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
    - [Prerequisites](#prerequisites)
    - [Build from Source](#build-from-source)
  - [Usage Guide](#usage-guide)
    - [Authentication](#authentication)
    - [Configuration](#configuration)
   - [Testing](#testing)
     - [Verify](#verify)
     - [Apply](#apply)
     - [Download](#download)
   - [Examples](#examples)
    - [Complete Workflow](#complete-workflow)
  - [Troubleshooting](#troubleshooting)
    - [Authentication Issues](#authentication-issues)
  - [Contributing](#contributing)
    - [Development Setup](#development-setup)

## Installation

### Homebrew (Recommended)

`pcl` can be installed for both macos and linux (arm64/x86) via Homebrew:

1. Add the `phylaxsystems/pcl` tap to your brew taps
2. Install the `phylax` formula

```bash
brew tap phylaxsystems/pcl
brew install phylax
```

Note: We have named the formula as `phylax`, but the binary is called `pcl`. This is because there is a binary already on Homebrew named `pcl`.

### Install from source

1. Install [Rust](https://www.rust-lang.org/tools/install)
2. Install `pcl` with `cargo install`:

```bash

// Build from a specific release
cargo +nightly install --git https://github.com/phylaxsystems/credible-sdk --locked --tag 0.0.7 pcl

// Build from the latest commit on `main`
cargo +nightly install --git https://github.com/phylaxsystems/credible-sdk --locked pcl
```

## Usage Guide

### Authentication

Before using most commands, you need to authenticate:

```bash
pcl auth [OPTIONS] <COMMAND>

Commands:
  login   Login to PCL
  logout  Logout from PCL
  status  Check current authentication status

Options:
  -u, --auth-url <AUTH_URL>  Base URL for authentication service [env: PCL_AUTH_URL=] [default: https://app.phylax.systems]
  -h, --help                 Print help
```

When logging in:

1. A URL and authentication code will be displayed
2. Visit the URL in your browser
3. Approve the authentication (via wallet, email, or OAuth)
4. CLI will automatically detect successful authentication

### Configuration

Manage your PCL configuration:

```bash
pcl config [COMMAND]

Commands:
  show    Display the current configuration
  delete  Delete the current configuration
```

Configuration is stored in `$XDG_CONFIG_HOME/pcl/config.toml` (defaults to `~/.config/pcl/config.toml`) and includes:

- Authentication tokens and identity

### Building

Build your assertion contracts:

```bash
pcl build [OPTIONS]

Options:
      --root <ROOT>  Root directory of the project
  -h, --help         Print help
```

### Testing

Run tests using Phorge (a Forge-compatible development environment). It's a minimal fork of forge to support out assertion execution cheatcodes, so `pcl test` behaves identically to `forge test`.

```bash
pcl test -h
Run tests using Phorge

Usage: pcl test [OPTIONS] [PATH]

Options:
  -h, --help  Print help (see more with '--help')

Display options:
  -v, --verbosity...                Verbosity level of the log messages.
  -q, --quiet                       Do not print log messages
      --json                        Format log messages as JSON
      --color <COLOR>               The color of the log messages [possible values: auto, always, never]
  -s, --suppress-successful-traces  Suppress successful test traces and show only traces for failures [env: FORGE_SUPPRESS_SUCCESSFUL_TRACES=]
      --junit                       Output test results as JUnit XML report
  -l, --list                        List tests instead of running them
      --show-progress               Show test execution progress
      --summary                     Print test summary table
      --detailed                    Print detailed test summary table

... // rest of the `forge test` help output
```

### Verify

Verify assertions locally before deployment. This compiles each assertion and runs it through the assertion verification pipeline to check that deployment succeeds and triggers are correctly registered.

```bash
pcl verify [OPTIONS] [ASSERTION]

Arguments:
  [ASSERTION]  Assertion to verify (contract name or file:contract).
               Verifies all assertions from credible.toml when omitted.

Options:
      --root <ROOT>      Project root directory [default: .]
  -c, --config <CONFIG>  Path to credible.toml, relative to root or absolute [default: assertions/credible.toml]
      --args <ARGS>      Constructor arguments for the assertion
      --json             Emit machine-readable JSON output
  -h, --help             Print help
```

By default, `pcl verify` reads `credible.toml` from the same location as `pcl apply` (`assertions/credible.toml`). Use `-c` to override.

```bash
# Verify all assertions from credible.toml
pcl verify

# Verify a single assertion by name
pcl verify MyAssertion

# Verify with constructor arguments
pcl verify MyAssertion --args 0x1234567890abcdef1234567890abcdef12345678

# Custom config path (same as apply)
pcl verify -c path/to/credible.toml
```

### Apply

Preview and apply declarative deployment changes from `credible.toml`. This command builds assertions, then creates a release on the platform.

```bash
pcl apply [OPTIONS]

Options:
      --root <ROOT>      Project root directory [default: .]
  -c, --config <CONFIG>  Path to credible.toml, relative to root or absolute [default: assertions/credible.toml]
      --json             Emit machine-readable output
      --yes              Apply without interactive confirmation
  -u, --api-url <URL>    Base URL for the platform API [env: PCL_API_URL=] [default: https://app.phylax.systems]
  -h, --help             Print help
```

By default, `pcl apply` looks for `credible.toml` inside the `assertions/` directory of your project:

```
my-project/
├── foundry.toml
├── src/                         # protocol contracts
├── assertions/
│   ├── credible.toml            # <- default location
│   ├── src/                     # assertion contracts
│   └── test/
```

Use `-c` to override the config path:

```bash
# Default: reads ./assertions/credible.toml
pcl apply --root ./my-project

# Legacy layout: credible.toml at project root
pcl apply --root ./my-project -c credible.toml

# Custom path
pcl apply --root ./my-project -c path/to/credible.toml
```

### Download

Download assertion source code for a protocol as `.sol` files. Provide either a project UUID or a protocol manager address to identify the protocol.

```bash
pcl download [OPTIONS]

Options:
      --project-id <UUID>    Project UUID to download assertions from
      --manager <ADDRESS>    Protocol manager address to look up the project
  -o, --output-dir <PATH>    Output directory for .sol files (default: <project_name>-assertions/)
      --json                 Emit machine-readable JSON output
  -u, --api-url <URL>        Base URL for the platform API [env: PCL_API_URL=] [default: https://app.phylax.systems]
  -h, --help                 Print help
```

Exactly one of `--project-id` or `--manager` is required.

Source code is resolved by preferring the release-submitted source, falling back to the DA-layer artifact when the release source is unavailable. Files are named `{contract_name}_{assertion_id_prefix}.sol`.

```bash
# Download by project UUID
pcl download --project-id 550e8400-e29b-41d4-a716-446655440000

# Download by protocol manager address
pcl download --manager 0x1234567890abcdef1234567890abcdef12345678

# Custom output directory
pcl download --project-id <UUID> --output-dir ./my-assertions

# JSON output
pcl download --project-id <UUID> --json
```

## Examples

### Complete Workflow

```bash
# Login
pcl auth login

# Verify status
pcl auth status

# Build and test assertions
pcl build
pcl test

# Verify assertions pass the verification pipeline
pcl verify

# Deploy assertions via credible.toml
pcl apply --root ./my-project

# Auto-approve without interactive confirmation
pcl apply --root ./my-project --yes

# Download assertion source code
pcl download --project-id <UUID>
pcl download --manager 0x1234...

# Logout when done
pcl auth logout
```

## Troubleshooting

### Authentication Issues

- **Error: Not authenticated**: Run `pcl auth login` to authenticate
- **Error: Authentication expired**: Run `pcl auth login` to refresh your authentication
- **Browser doesn't open**: Manually visit the URL displayed in the terminal

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Setup

```bash
# Install dependencies
cargo build

# Run tests
make test

# Check formatting
make format

# Run linter
make lint
```
