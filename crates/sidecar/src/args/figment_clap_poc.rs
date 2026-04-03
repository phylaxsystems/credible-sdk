//! # PoC 4 — figment + clap (hybrid approach)
//!
//! Combines the best of both worlds:
//! - **clap** handles CLI parsing + env var reading (flat `SIDECAR_*` naming, `--help`)
//! - **figment** handles the layered merge (no manual per-field resolve logic)
//!
//! ## How it works
//!
//! 1. Clap parses CLI args and env vars into `Option<T>` fields.
//!    - CLI > ENV precedence is handled natively by clap's `ValueSource`.
//!    - `env = "SIDECAR_..."` gives us flat naming with zero ambiguity.
//!    - `--help` auto-shows env var names.
//!
//! 2. The CLI struct implements `Serialize` with `skip_serializing_if = "Option::is_none"`.
//!    Only values actually provided (via CLI or ENV) are serialized.
//!
//! 3. Figment merges layers: `Json::file` (lowest) → `Serialized(cli)` (highest).
//!    Deep merge means individual nested fields override without clobbering siblings.
//!
//! ## What this eliminates vs the current code
//!
//! - **~180 lines of resolve boilerplate** — no `required_or_env`, `optional_or_env`,
//!   `parse_env`, `resolve_chain`, `resolve_credible_required`, etc.
//! - **Intermediate structs** — no `CredibleRequired`, `CredibleOptional`, `CredibleTtls`,
//!   `CrediblePrune` bags.
//! - **Dual struct pattern partially** — `*File` structs are gone; figment deserializes
//!   the JSON directly into the resolved type, with CLI overrides merged on top.
//!
//! ## What this eliminates vs PoC 3 (clap + manual merge)
//!
//! - **No `resolve_field()` per field** — figment handles the merge declaratively.
//! - **No `Sourced<T>` wrapper** — resolved config uses plain types.
//! - **No `merge()` function** — the entire merge is `figment.merge(Serialized(...))`.
//! - **Adding a new field = add to CLI struct + resolved struct, zero merge code.**
//!
//! ## Advantages over PoC 2 (figment alone)
//!
//! - **Flat env naming works** — clap handles env vars, no separator ambiguity.
//! - **`--help` auto-generated** with env var names.
//! - **Active maintenance** — clap is actively maintained.
//! - **No figment env parsing** — we never use `Env::prefixed()`, so the underscore
//!   ambiguity and `#[serde(flatten)]` issues from PoC 2 don't apply.
//!
//! ## Tradeoffs
//!
//! - **Two dependencies** — clap (already in workspace) + figment (new, stalled since
//!   May 2024). Figment is used only for merge, not for env parsing.
//! - **No per-field provenance** — can't tell "from CLI" vs "from ENV" vs "from FILE"
//!   for each individual field (unlike PoC 3's `Sourced<T>`). Figment error messages
//!   do report which *provider* failed.
//! - **Figment maintenance risk** — depends on figment which hasn't had commits since
//!   May 2024. However, we use a tiny surface area (Serialized + Json providers only).

use clap::Parser;
use figment::{
    Figment,
    providers::{
        Format,
        Json,
        Serialized,
    },
};
use serde::{
    Deserialize,
    Serialize,
};

// ---------------------------------------------------------------------------
// 1. CLI args — clap handles CLI flags AND env vars
// ---------------------------------------------------------------------------
// The struct is `Serialize` so figment can consume it as a provider.
// `skip_serializing_if = "Option::is_none"` ensures only provided values
// participate in the merge — unprovided fields fall through to the file layer.

/// Credible layer sidecar (PoC 4 — figment + clap)
#[derive(Debug, Clone, Parser, Serialize)]
#[command(name = "sidecar", about = "Credible layer sidecar (figment + clap PoC)")]
pub struct PocCliArgs {
    /// Chain configuration overrides
    #[command(flatten)]
    pub chain: PocChainCli,

    /// Credible configuration overrides
    #[command(flatten)]
    pub credible: PocCredibleCli,

    /// Path to JSON config file
    #[arg(long, env = "CONFIG_FILE_PATH")]
    #[serde(skip)]
    pub config_file_path: Option<String>,
}

#[derive(Debug, Clone, Default, clap::Args, Serialize)]
pub struct PocChainCli {
    /// Chain ID
    #[arg(long, env = "SIDECAR_CHAIN_ID")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain_id: Option<u64>,

    /// EVM specification (e.g. CANCUN)
    #[arg(long, env = "SIDECAR_CHAIN_SPEC_ID")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec_id: Option<String>,
}

#[derive(Debug, Clone, Default, clap::Args, Serialize)]
pub struct PocCredibleCli {
    /// Gas limit for assertion execution
    #[arg(long, env = "SIDECAR_ASSERTION_GAS_LIMIT")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assertion_gas_limit: Option<u64>,

    /// HTTP URL of the assertion DA
    #[arg(long, env = "SIDECAR_ASSERTION_DA_RPC_URL")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assertion_da_rpc_url: Option<String>,

    /// URL of the event source for assertion events
    #[arg(long, env = "SIDECAR_EVENT_SOURCE_URL")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_source_url: Option<String>,

    /// Contract address of the state oracle
    #[arg(long, env = "SIDECAR_STATE_ORACLE")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_oracle: Option<String>,

    /// Block number of the state oracle deployment
    #[arg(long, env = "SIDECAR_STATE_ORACLE_DEPLOYMENT_BLOCK")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_oracle_deployment_block: Option<u64>,
}

// ---------------------------------------------------------------------------
// 2. Resolved config — the final types that the application uses
// ---------------------------------------------------------------------------
// No `Option<T>` for required fields — figment will error if a required field
// is missing from all providers.

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PocConfig {
    pub chain: PocChainConfig,
    pub credible: PocCredibleConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PocChainConfig {
    pub spec_id: String,
    pub chain_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PocCredibleConfig {
    pub assertion_gas_limit: u64,
    pub assertion_da_rpc_url: String,
    pub event_source_url: String,
    pub state_oracle: String,
    pub state_oracle_deployment_block: u64,
}

// ---------------------------------------------------------------------------
// 3. The load function — this is the entire merge logic
// ---------------------------------------------------------------------------

/// Build a `PocConfig` by merging file config with CLI/ENV overrides.
///
/// Precedence (highest wins):
///   1. JSON config file      (lowest)
///   2. CLI + ENV from clap   (highest — clap handles CLI > ENV internally)
///
/// With the hand-rolled code this takes ~180 lines of helpers + intermediate structs.
/// With figment + clap it's 3 lines.
pub fn load(
    config_file_path: Option<&str>,
    cli: &PocCliArgs,
) -> Result<PocConfig, figment::Error> {
    let mut figment = Figment::new();

    if let Some(path) = config_file_path {
        figment = figment.merge(Json::file(path));
    }

    // CLI+ENV overrides — clap already resolved CLI > ENV, and
    // skip_serializing_if ensures only provided values participate.
    figment = figment.merge(Serialized::defaults(cli));

    figment.extract()
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::{
        io::Write,
        sync::Mutex,
    };

    /// Global lock — env vars are process-global so tests must not race.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// All env var keys this PoC uses, for cleanup.
    const ENV_KEYS: &[&str] = &[
        "SIDECAR_CHAIN_ID",
        "SIDECAR_CHAIN_SPEC_ID",
        "SIDECAR_ASSERTION_GAS_LIMIT",
        "SIDECAR_ASSERTION_DA_RPC_URL",
        "SIDECAR_EVENT_SOURCE_URL",
        "SIDECAR_STATE_ORACLE",
        "SIDECAR_STATE_ORACLE_DEPLOYMENT_BLOCK",
        "CONFIG_FILE_PATH",
    ];

    fn clear_env() {
        for key in ENV_KEYS {
            // SAFETY: test-only env var access; mutex prevents cross-test races
            unsafe { std::env::remove_var(key) };
        }
    }

    fn write_config_file(json: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(json.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    /// Parse CLI args without touching real process args.
    fn parse_cli(args: &[&str]) -> PocCliArgs {
        PocCliArgs::try_parse_from(args).unwrap()
    }

    const BASE_JSON: &str = r#"{
        "chain": {
            "spec_id": "CANCUN",
            "chain_id": 1337
        },
        "credible": {
            "assertion_gas_limit": 3000000,
            "assertion_da_rpc_url": "http://127.0.0.1:5001",
            "event_source_url": "http://127.0.0.1:4350/graphql",
            "state_oracle": "0x6dD3f12ce435f69DCeDA7e31605C02Bb5422597b",
            "state_oracle_deployment_block": 0
        }
    }"#;

    // -----------------------------------------------------------------------
    // Test 1: FILE only — all values from JSON, no CLI or ENV
    // -----------------------------------------------------------------------
    #[test]
    fn test_file_only() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();

        let f = write_config_file(BASE_JSON);
        let cli = parse_cli(&["sidecar"]);

        let cfg = load(Some(f.path().to_str().unwrap()), &cli)
            .expect("should load from file");

        assert_eq!(cfg.chain.spec_id, "CANCUN");
        assert_eq!(cfg.chain.chain_id, 1337);
        assert_eq!(cfg.credible.assertion_gas_limit, 3_000_000);
        assert_eq!(cfg.credible.assertion_da_rpc_url, "http://127.0.0.1:5001");
        assert_eq!(
            cfg.credible.event_source_url,
            "http://127.0.0.1:4350/graphql"
        );
        assert_eq!(
            cfg.credible.state_oracle,
            "0x6dD3f12ce435f69DCeDA7e31605C02Bb5422597b"
        );
        assert_eq!(cfg.credible.state_oracle_deployment_block, 0);
    }

    // -----------------------------------------------------------------------
    // Test 2: ENV overrides FILE — env var wins over file value
    // -----------------------------------------------------------------------
    #[test]
    fn test_env_overrides_file() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();

        let f = write_config_file(BASE_JSON);

        // Set env vars — clap will pick these up via `env = "..."`.
        // SAFETY: test-only env var access; mutex prevents races
        unsafe {
            std::env::set_var("SIDECAR_CHAIN_ID", "42161");
            std::env::set_var("SIDECAR_ASSERTION_GAS_LIMIT", "9999999");
        }

        // Parse CLI with no flags — clap reads from env.
        let cli = parse_cli(&["sidecar"]);

        let cfg = load(Some(f.path().to_str().unwrap()), &cli)
            .expect("should load with env overrides");

        clear_env();

        // ENV overrides file for these fields:
        assert_eq!(cfg.chain.chain_id, 42161, "env should override file");
        assert_eq!(
            cfg.credible.assertion_gas_limit, 9_999_999,
            "env should override file"
        );

        // File values preserved where env was not set:
        assert_eq!(cfg.chain.spec_id, "CANCUN", "file value preserved");
        assert_eq!(
            cfg.credible.assertion_da_rpc_url, "http://127.0.0.1:5001",
            "file value preserved"
        );
        assert_eq!(
            cfg.credible.event_source_url,
            "http://127.0.0.1:4350/graphql",
            "file value preserved"
        );
    }

    // -----------------------------------------------------------------------
    // Test 3: CLI overrides ENV and FILE
    // -----------------------------------------------------------------------
    #[test]
    fn test_cli_overrides_env_and_file() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();

        let f = write_config_file(BASE_JSON);

        // Set env var for chain_id to 42161.
        // SAFETY: test-only env var access
        unsafe { std::env::set_var("SIDECAR_CHAIN_ID", "42161"); }

        // CLI passes --chain-id 1 — should beat env 42161 and file 1337.
        let cli = parse_cli(&[
            "sidecar",
            "--chain-id",
            "1",
            "--assertion-da-rpc-url",
            "http://cli-da:9999",
        ]);

        let cfg = load(Some(f.path().to_str().unwrap()), &cli)
            .expect("should load with CLI overrides");

        clear_env();

        // CLI wins over ENV (42161) and FILE (1337):
        assert_eq!(cfg.chain.chain_id, 1, "CLI should override env and file");
        // CLI wins over FILE:
        assert_eq!(
            cfg.credible.assertion_da_rpc_url, "http://cli-da:9999",
            "CLI should override file"
        );
        // FILE fallback for unset fields:
        assert_eq!(cfg.chain.spec_id, "CANCUN", "file fallback");
        assert_eq!(
            cfg.credible.assertion_gas_limit, 3_000_000,
            "file fallback"
        );
    }

    // -----------------------------------------------------------------------
    // Test 4: Missing required field produces a clear error
    // -----------------------------------------------------------------------
    #[test]
    fn test_missing_required_field_errors() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();

        // Partial file config — missing several required fields.
        let partial_json = r#"{
            "chain": { "chain_id": 1337 }
        }"#;
        let f = write_config_file(partial_json);

        let cli = parse_cli(&["sidecar"]);
        let result = load(Some(f.path().to_str().unwrap()), &cli);

        assert!(result.is_err(), "should fail with missing required fields");
        let err = result.unwrap_err().to_string();
        println!("Error message: {err}");
        // Figment's error should mention the missing field.
        assert!(
            err.contains("missing") || err.contains("credible"),
            "error should mention what's missing: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // Test 5: CLI-only — no file at all, everything from CLI/ENV
    // -----------------------------------------------------------------------
    #[test]
    fn test_cli_only_no_file() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();

        let cli = parse_cli(&[
            "sidecar",
            "--chain-id",
            "10",
            "--spec-id",
            "SHANGHAI",
            "--assertion-gas-limit",
            "5000000",
            "--assertion-da-rpc-url",
            "http://da:5001",
            "--event-source-url",
            "http://events:4350/graphql",
            "--state-oracle",
            "0x0000000000000000000000000000000000001234",
            "--state-oracle-deployment-block",
            "42",
        ]);

        // No file — all values from CLI.
        let cfg = load(None, &cli).expect("should work without file");

        assert_eq!(cfg.chain.chain_id, 10);
        assert_eq!(cfg.chain.spec_id, "SHANGHAI");
        assert_eq!(cfg.credible.assertion_gas_limit, 5_000_000);
        assert_eq!(cfg.credible.assertion_da_rpc_url, "http://da:5001");
        assert_eq!(
            cfg.credible.event_source_url,
            "http://events:4350/graphql"
        );
        assert_eq!(
            cfg.credible.state_oracle,
            "0x0000000000000000000000000000000000001234"
        );
        assert_eq!(cfg.credible.state_oracle_deployment_block, 42);
    }

    // -----------------------------------------------------------------------
    // Test 6: Flat env naming — no separator ambiguity
    // -----------------------------------------------------------------------
    // This test demonstrates that figment's env separator problem does NOT
    // apply here because we use clap for env var reading, not figment.
    // `SIDECAR_ASSERTION_DA_RPC_URL` maps correctly to the nested
    // `credible.assertion_da_rpc_url` field through clap, not through
    // figment's `Env::prefixed().split("_")`.
    #[test]
    fn test_flat_env_naming_no_ambiguity() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();

        let f = write_config_file(BASE_JSON);

        // This env var has underscores that would confuse figment's separator:
        //   SIDECAR_ASSERTION_DA_RPC_URL -> assertion.da.rpc.url (wrong!)
        // But with clap, it maps directly to the `assertion_da_rpc_url` field.
        // SAFETY: test-only env var access
        unsafe {
            std::env::set_var(
                "SIDECAR_ASSERTION_DA_RPC_URL",
                "http://env-da:7777",
            );
        }

        let cli = parse_cli(&["sidecar"]);
        let cfg = load(Some(f.path().to_str().unwrap()), &cli)
            .expect("flat env naming should work");

        clear_env();

        // The env var correctly overrides the nested field:
        assert_eq!(
            cfg.credible.assertion_da_rpc_url, "http://env-da:7777",
            "flat env var should override nested file field without ambiguity"
        );
        // Other nested fields preserved:
        assert_eq!(cfg.credible.assertion_gas_limit, 3_000_000);
        assert_eq!(cfg.chain.chain_id, 1337);
    }

    // -----------------------------------------------------------------------
    // Test 7: load() convenience function with all three layers
    // -----------------------------------------------------------------------
    #[test]
    fn test_load_three_layers() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();

        let f = write_config_file(BASE_JSON);

        // ENV: override spec_id and event_source_url
        // SAFETY: test-only env var access
        unsafe {
            std::env::set_var("SIDECAR_CHAIN_SPEC_ID", "SHANGHAI");
            std::env::set_var(
                "SIDECAR_EVENT_SOURCE_URL",
                "http://env-events:1234",
            );
        }

        // CLI: override chain_id
        let cli = parse_cli(&["sidecar", "--chain-id", "10"]);

        let cfg = load(Some(f.path().to_str().unwrap()), &cli)
            .expect("three-layer merge should work");

        clear_env();

        // CLI wins:
        assert_eq!(cfg.chain.chain_id, 10);
        // ENV wins over file:
        assert_eq!(cfg.chain.spec_id, "SHANGHAI");
        assert_eq!(
            cfg.credible.event_source_url,
            "http://env-events:1234"
        );
        // FILE fallback:
        assert_eq!(cfg.credible.assertion_gas_limit, 3_000_000);
        assert_eq!(cfg.credible.assertion_da_rpc_url, "http://127.0.0.1:5001");
        assert_eq!(
            cfg.credible.state_oracle,
            "0x6dD3f12ce435f69DCeDA7e31605C02Bb5422597b"
        );
    }
}
