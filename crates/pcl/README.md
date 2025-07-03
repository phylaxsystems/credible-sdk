# Phylax Credible Layer (PCL) CLI

<!-- [![Tests, Linting, Format](https://github.com/phylaxsystems/credible-sdk/actions/workflows/rust-base.yml/badge.svg)](https://github.com/phylaxsystems/credible-sdk/actions/workflows/rust-base.yml) -->

The Phylax Credible CLI (PCL) is a command-line interface for interacting with the Credible Layer. It allows developers to authenticate, build, test, and submit assertions to the Credible Layer dApp.

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
    - [Assertion Submission](#assertion-submission)
      - [Store Assertions in Data Availability Layer](#store-assertions-in-data-availability-layer)
      - [Submit Assertions to dApps](#submit-assertions-to-dapps)
  - [Examples](#examples)
    - [Complete Authentication Flow](#complete-authentication-flow)
    - [Development Workflow](#development-workflow)
  - [Troubleshooting](#troubleshooting)
    - [Authentication Issues](#authentication-issues)
    - [Submission Issues](#submission-issues)
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
  login   Login to PCL using your wallet
  logout  Logout from PCL
  status  Check current authentication status

Options:
  -u, --auth-url <AUTH_URL>  Base URL for authentication service [env: PCL_AUTH_URL=] [default: https://dapp.phylax.systems]
  -h, --help                 Print help
```

When logging in:

1. A URL and authentication code will be displayed
2. Visit the URL in your browser
3. Connect your wallet and approve the authentication
4. CLI will automatically detect successful authentication

### Configuration

Manage your PCL configuration:

```bash
pcl config [COMMAND]

Commands:
  show    Display the current configuration
  delete  Delete the current configuration
```

Configuration is stored in `~/.pcl/config.toml` and includes:

- Authentication token
- Pending assertions for submission
- Project settings

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

### Assertion Submission

#### Store Assertions in Data Availability Layer

```bash
pcl store [OPTIONS] <ASSERTION_CONTRACT> [CONSTRUCTOR_ARGS]...

Arguments:
  <ASSERTION_CONTRACT>   Name of the assertion contract to build and flatten
  [CONSTRUCTOR_ARGS]...  Constructor arguments for the assertion contract
                         Format: <ARG0> <ARG1> <ARG2>

Options:
  -u, --da-url <DA_URL>  URL of the assertion-DA server [env: PCL_DA_URL=] [default: https://demo-21-assertion-da.phylax.systems]
      --root <ROOT>      Root directory of the project
  -h, --help             Print help (see a summary with '-h')
```

#### Submit Assertions to dApps

```bash
pcl submit [OPTIONS]

Options:
  -u, --api-url <API_URL>                   Base URL for the Credible Layer dApp API [env: PCL_API_URL=] [default: https://dapp.phylax.systems/api/v1]
  -p, --project-name <PROJECT_NAME>         Optional project name to skip interactive selection
  -a, --assertion-keys <ASSERTION_KEYS>     Optional list of assertion name and constructor args to skip interactive selection
                                            Format: assertion_name OR 'assertion_name(constructor_arg0,constructor_arg1)'
  -h, --help                                Print help
```

## Examples

### Complete Authentication Flow

```bash
# Login
pcl auth login

# Verify status
pcl auth status

# Store assertion
pcl store my_assertion

# Submit to dApp
pcl submit -a my_assertion -p my_project

# Logout when done
pcl auth logout
```

### Development Workflow

```bash
# Run tests
pcl test

# Store and submit assertion
pcl store my_assertion
pcl submit -a my_assertion -p my_project
```

## Troubleshooting

### Authentication Issues

- **Error: Not authenticated**: Run `pcl auth login` to authenticate
- **Error: Authentication expired**: Run `pcl auth login` to refresh your authentication
- **Browser doesn't open**: Manually visit the URL displayed in the terminal

### Submission Issues

- **Error: Failed to submit**: Ensure you're authenticated and have network connectivity
- **Error: Project not found**: Create a project in the Credible Layer dApp first
- **Error: Assertion not found**: Ensure the assertion name is correct and exists in your project

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
