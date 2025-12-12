# Repository Guidelines

## Project Structure & Module Organization
- `crates/` hosts the Rust workspace (pcl-core, pcl-cli, da-server, da-client, assertion-executor, int-test-utils, etc.); inspect each crate README for crate-specific flows.
- `lib/`, `examples/`, and `phorge/` contain reusable libs, Solidity assertion samples, and Foundry scaffolding referenced by the CLI.
- Support directories: `docker/` & `dockerfile/` store container specs, `etc/` has compose stacks, `scripts/` wraps repeatable workflows, `testdata/` and `fuzz/` hold fixtures and fuzz harnesses.

## Build, Test, and Development Commands
- `make build` (cargo release build) or `cargo build --workspace --locked` when iterating locally.
- `cargo run --release --bin pcl -- <subcommand>` to exercise the CLI; `cargo install --path crates/pcl-cli` installs a reusable binary.
- `make compose` (or `make compose-dev`) brings up the assertion DA stack; provide `DA_PRIVATE_KEY` in your env.
- `make docker-build` builds the production image, while `docker compose -f etc/docker-compose-dev.yaml up` mirrors the dev cluster used in integration tests.

## Coding Style & Naming Conventions
- Rust code adheres to the repo `rustfmt.toml`; run `make format` before submitting. Foundry contracts in `testdata/mock-protocol` follow standard Solidity style (4 spaces, PascalCase contracts).
- Lint with `make lint` (nightly clippy, `-D warnings -D clippy::pedantic`). Favor explicit `snake_case` module names and `UpperCamelCase` types; CLI commands stay kebab-case to match `pcl` UX.

## Testing Guidelines
- Primary suites: `make test-optimism` and `make test-default` (both call `cargo nextest run` with the proper feature flags after `forge build`). Use `make test-no-full` for a lighter CI-style sweep.
- Targeted runs: `cargo nextest run -p <crate>` or `cargo test -p pcl-core <module>::<case>`. Keep test files near the code (`mod tests` inline or `tests/` sibling). Name Solidity tests with the assertion they validate for clarity.

## Commit & Pull Request Guidelines
- Prefer conventional commits (`feat(scope): summary (#123)`); scope usually matches the crate (`assex`, `da-server`, `pcl-cli`). Keep subject ≤72 chars and describe rationale in the body when behavior changes.
- PRs should summarize intent, list user-facing effects, link issues, and paste key command output (e.g., `make test-default`). Include screenshots for CLI/UI surface changes and note any new env vars.

## Security & Configuration Notes
- Review `SECURITY.md` before reporting vulnerabilities. Never commit keys; use `.env` overrides and `pcl auth login` for wallet auth. Respect the pinned `rust-toolchain.toml` so builds stay reproducible.
