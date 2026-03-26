# PCL

This file covers `crates/pcl/cli`, `crates/pcl/common`, `crates/pcl/core`, and `crates/pcl/phoundry`.

## PCL schema

```text
developer / operator
  |
  v
pcl CLI
  |
  +--> auth   -> app auth endpoints -> store tokens in CliConfig
  |
  +--> build  -> pcl-phoundry -> Foundry compile
  |
  +--> test   -> pcl-phoundry -> phorge / Foundry tests
  |
  +--> apply  -> credible.toml + build_and_flatten -> app releases endpoint
```

## What it is

The PCL stack is the user and operator interface to the wider system.

It is split into four crates:

- `pcl/cli`: top-level binary and command routing,
- `pcl/core`: app auth, apply workflow, and config persistence,
- `pcl/common`: shared assertion helpers and argument types,
- `pcl/phoundry`: wrappers around Foundry and phoundry build and test flows.

PCL does not replace `sidecar`. It serves a different boundary:

- `sidecar` validates transactions at runtime,
- PCL prepares and publishes assertion artifacts ahead of runtime.

## CLI command model

`pcl/cli` reads `CliConfig`, dispatches the selected subcommand, and writes config back to disk after successful command execution.

The current top-level commands are:

- `test`
- `apply`
- `auth`
- `config`
- `build`

The CLI supports JSON output in error paths, but command implementations are still mostly interactive terminal flows.

## Local config model

`pcl/core` defines `CliConfig`.

The key persisted state is:

- `auth: Option<UserAuth>` — tokens, user ID, optional wallet address, optional email

Users authenticate once and keep tokens locally. The `apply` command then uses these tokens when submitting releases to the app backend.

## Auth flow

`pcl core::auth` implements a device-style login flow against the app backend.

The login path is:

1. request a CLI auth code from the app,
2. open a browser to the verification page with the returned session id,
3. poll the auth-status endpoint until the session is verified,
4. persist access token, refresh token, optional wallet address, expiration, and optional user metadata in `CliConfig`.

The auth flow supports wallet-based, email-based, and OAuth-based authentication. Wallet address is optional — the CLI works without one.

`logout` and `status` operate only on local config state.

## Build and test behavior

`pcl-phoundry` wraps Foundry and phoundry operations instead of reimplementing compiler behavior.

The main roles are:

- `build`: compile contracts from the expected assertion source tree,
- `build_and_flatten`: compile, find the produced artifact, collect ABI and bytecode details, and flatten Solidity source,
- `compile`: enforce source-directory assumptions and invoke Foundry project compilation,
- `phorge_test`: run Foundry and phorge tests in a blocking task.

This means PCL correctness for local artifact generation depends heavily on Foundry project layout conventions and on Foundry and phoundry being installed correctly.

## Apply flow

The `apply` command consumes declarative project configuration, currently centered around `credible.toml`.

It assembles a release payload by:

- loading the declared contracts, environments, and assertions,
- building and flattening referenced assertion contracts,
- selecting or resolving the target project,
- submitting the resulting release payload to the app backend.

The code shows that preview-oriented behavior is still incomplete or intentionally deferred; the main implemented path is direct release submission.

## Common assertion path handling

`pcl/common` provides small but important normalization helpers.

One example is `Assertion::get_paths()`, which resolves assertion source candidates by trying both:

- `<contract>.a.sol`
- `<contract>.sol`

That fallback is part of current CLI behavior. Agents modifying assertion discovery should preserve it unless they are deliberately changing supported naming conventions.
