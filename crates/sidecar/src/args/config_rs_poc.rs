//! PoC 1: config-rs layering spike
//!
//! Demonstrates using the `config` crate to replace the hand-rolled
//! config system with automatic layering:
//!   CLI > ENV > FILE (> Defaults)
//!
//! ## Key findings
//!
//! 1. **Layering works well** for the simple case: `Config::builder()` with
//!    `File`, `Environment`, and `set_override` gives us the correct
//!    precedence with almost no boilerplate.
//!
//! 2. **The separator problem is a dealbreaker for flat env vars.**
//!    Our env vars use `_` as a word separator (`SIDECAR_ASSERTION_DA_RPC_URL`),
//!    but `config-rs` needs a separator character to map env vars to nested
//!    struct fields. If we use `separator("_")`, then
//!    `SIDECAR_ASSERTION_DA_RPC_URL` gets parsed as:
//!    `sidecar.assertion.da.rpc.url` instead of the intended
//!    `credible.assertion_da_rpc_url`.
//!
//!    Using `separator("__")` (double underscore) would fix this, but that
//!    requires renaming ALL existing env vars from `SIDECAR_CHAIN_ID` to
//!    `SIDECAR__CHAIN__CHAIN_ID` — a breaking change for all deployments.
//!
//! 3. **Workaround options** (all ugly):
//!    - Use `separator("__")` and rename all env vars (breaking change)
//!    - Skip the `Environment` source and manually wire env vars (defeats the purpose)
//!    - Use a flat struct with no nesting (loses JSON structure)

use config::{Config, ConfigError, Environment, File, FileFormat};
use serde::Deserialize;

/// Subset of ChainConfig for the PoC
#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
pub struct PocChainConfig {
    pub spec_id: String,
    pub chain_id: u64,
}

/// Subset of CredibleConfig for the PoC — only the fields we care about
/// to demonstrate the layering pattern.
#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
pub struct PocCredibleConfig {
    pub assertion_gas_limit: u64,
    pub assertion_da_rpc_url: String,
    pub event_source_url: String,
    /// String instead of Address for PoC simplicity
    pub state_oracle: String,
    pub state_oracle_deployment_block: u64,
}

/// Unified config struct that mirrors the JSON file structure.
#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
pub struct PocConfig {
    pub chain: PocChainConfig,
    pub credible: PocCredibleConfig,
}

/// Simulated CLI overrides — in reality these would come from clap args.
/// Only `Some` values are applied as overrides.
#[derive(Debug, Default)]
pub struct CliOverrides {
    pub chain_id: Option<u64>,
    pub spec_id: Option<String>,
    pub assertion_gas_limit: Option<u64>,
    pub assertion_da_rpc_url: Option<String>,
    pub event_source_url: Option<String>,
    pub state_oracle: Option<String>,
    pub state_oracle_deployment_block: Option<u64>,
}

/// Load configuration with layering: CLI > ENV > FILE
///
/// `json_source` is the raw JSON string (from file or embedded default).
/// `cli` contains optional CLI overrides (highest priority).
pub fn load(json_source: &str, cli: CliOverrides) -> Result<PocConfig, ConfigError> {
    let mut builder = Config::builder()
        // Layer 1 (lowest priority): JSON file
        .add_source(File::from_str(json_source, FileFormat::Json))
        // Layer 2 (medium priority): Environment variables
        //
        // NOTE: This uses "__" as separator so that env vars map to nested
        // fields correctly. E.g. SIDECAR__CHAIN__CHAIN_ID -> chain.chain_id
        //
        // This is NOT compatible with the existing flat env vars like
        // SIDECAR_CHAIN_ID — see the separator problem documented above.
        .add_source(
            Environment::with_prefix("SIDECAR")
                .separator("__")
                .try_parsing(true),
        );

    // Layer 3 (highest priority): CLI overrides
    if let Some(v) = cli.chain_id {
        builder = builder.set_override("chain.chain_id", v as i64)?;
    }
    if let Some(v) = cli.spec_id {
        builder = builder.set_override("chain.spec_id", v)?;
    }
    if let Some(v) = cli.assertion_gas_limit {
        builder = builder.set_override("credible.assertion_gas_limit", v as i64)?;
    }
    if let Some(v) = cli.assertion_da_rpc_url {
        builder = builder.set_override("credible.assertion_da_rpc_url", v)?;
    }
    if let Some(v) = cli.event_source_url {
        builder = builder.set_override("credible.event_source_url", v)?;
    }
    if let Some(v) = cli.state_oracle {
        builder = builder.set_override("credible.state_oracle", v)?;
    }
    if let Some(v) = cli.state_oracle_deployment_block {
        builder = builder.set_override("credible.state_oracle_deployment_block", v as i64)?;
    }

    builder.build()?.try_deserialize()
}

/// Demonstrates the separator problem: using `separator("_")` with the
/// existing flat env var naming scheme causes mis-parsing.
///
/// When SIDECAR_CREDIBLE_ASSERTION_DA_RPC_URL is set and separator is "_",
/// config-rs interprets it as the nested path:
///   sidecar -> credible -> assertion -> da -> rpc -> url
/// instead of the intended:
///   credible -> assertion_da_rpc_url
///
/// This function uses `separator("_")` to show the failure mode.
pub fn load_with_underscore_separator(json_source: &str) -> Result<PocConfig, ConfigError> {
    Config::builder()
        .add_source(File::from_str(json_source, FileFormat::Json))
        .add_source(
            Environment::with_prefix("SIDECAR")
                .separator("_")
                .try_parsing(true),
        )
        .build()?
        .try_deserialize()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Lock for env-var tests to prevent races between parallel tests.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    const TEST_JSON: &str = r#"{
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

    // ---------------------------------------------------------------
    // Test 1: FILE only — values come from JSON
    // ---------------------------------------------------------------
    #[test]
    fn test_file_only() {
        let _lock = ENV_LOCK.lock().unwrap();

        let cfg = load(TEST_JSON, CliOverrides::default()).unwrap();

        assert_eq!(cfg.chain.spec_id, "CANCUN");
        assert_eq!(cfg.chain.chain_id, 1337);
        assert_eq!(cfg.credible.assertion_gas_limit, 3000000);
        assert_eq!(
            cfg.credible.assertion_da_rpc_url,
            "http://127.0.0.1:5001"
        );
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

    // ---------------------------------------------------------------
    // Test 2: ENV overrides FILE
    // ---------------------------------------------------------------
    #[test]
    fn test_env_overrides_file() {
        let _lock = ENV_LOCK.lock().unwrap();

        // Set env vars using the double-underscore separator convention
        // SIDECAR__CHAIN__CHAIN_ID -> chain.chain_id
        unsafe {
            std::env::set_var("SIDECAR__CHAIN__CHAIN_ID", "42");
            std::env::set_var(
                "SIDECAR__CREDIBLE__ASSERTION_GAS_LIMIT",
                "9999999",
            );
        }

        let cfg = load(TEST_JSON, CliOverrides::default()).unwrap();

        // ENV wins over FILE
        assert_eq!(cfg.chain.chain_id, 42);
        assert_eq!(cfg.credible.assertion_gas_limit, 9999999);

        // FILE values still present for fields without env overrides
        assert_eq!(cfg.chain.spec_id, "CANCUN");
        assert_eq!(
            cfg.credible.assertion_da_rpc_url,
            "http://127.0.0.1:5001"
        );

        // Cleanup
        unsafe {
            std::env::remove_var("SIDECAR__CHAIN__CHAIN_ID");
            std::env::remove_var("SIDECAR__CREDIBLE__ASSERTION_GAS_LIMIT");
        }
    }

    // ---------------------------------------------------------------
    // Test 3: CLI overrides ENV and FILE
    // ---------------------------------------------------------------
    #[test]
    fn test_cli_overrides_env_and_file() {
        let _lock = ENV_LOCK.lock().unwrap();

        // Set env var that would override FILE
        unsafe {
            std::env::set_var("SIDECAR__CHAIN__CHAIN_ID", "42");
        }

        let cli = CliOverrides {
            chain_id: Some(100), // CLI should beat ENV (42) and FILE (1337)
            assertion_da_rpc_url: Some("http://cli-override:9999".to_string()),
            ..Default::default()
        };

        let cfg = load(TEST_JSON, cli).unwrap();

        // CLI wins over both ENV and FILE
        assert_eq!(cfg.chain.chain_id, 100);
        assert_eq!(
            cfg.credible.assertion_da_rpc_url,
            "http://cli-override:9999"
        );

        // ENV still wins over FILE for fields without CLI override
        // (no env set for spec_id, so FILE value remains)
        assert_eq!(cfg.chain.spec_id, "CANCUN");

        // FILE value for fields with no CLI or ENV override
        assert_eq!(cfg.credible.assertion_gas_limit, 3000000);

        // Cleanup
        unsafe {
            std::env::remove_var("SIDECAR__CHAIN__CHAIN_ID");
        }
    }

    // ---------------------------------------------------------------
    // Test 4: Separator problem demonstration
    // ---------------------------------------------------------------
    // This test demonstrates why `separator("_")` doesn't work with
    // our existing flat env var naming scheme.
    //
    // With separator("_"), the env var SIDECAR_CREDIBLE_ASSERTION_DA_RPC_URL
    // gets mapped to the key path: credible.assertion.da.rpc.url
    // instead of: credible.assertion_da_rpc_url
    //
    // This means deserialization fails because there's no nested
    // struct matching that deep path.
    #[test]
    fn test_separator_problem_underscore_misparsing() {
        let _lock = ENV_LOCK.lock().unwrap();

        // Set an env var using the EXISTING flat naming convention
        unsafe {
            std::env::set_var(
                "SIDECAR_CREDIBLE_ASSERTION_DA_RPC_URL",
                "http://overridden:9999",
            );
        }

        // With separator("_"), this will NOT work as intended.
        // The env var gets split on every "_" after the prefix, producing:
        //   credible -> assertion -> da -> rpc -> url
        // which doesn't match our struct (credible.assertion_da_rpc_url).
        let result = load_with_underscore_separator(TEST_JSON);

        // The config-rs builder may succeed at building but deserialize will
        // fail because the value ends up at the wrong path. Or it may
        // silently ignore it. Either way the override doesn't land:
        match result {
            Ok(cfg) => {
                // If it parses, the override was NOT applied — the value
                // still comes from the file, proving the separator breaks
                // flat env var names.
                assert_eq!(
                    cfg.credible.assertion_da_rpc_url,
                    "http://127.0.0.1:5001",
                    "separator('_') caused the env var to be misparsed — \
                     the override was silently lost!"
                );
            }
            Err(e) => {
                // A deserialization error also proves the point — the env var
                // was mapped to a non-existent nested path.
                eprintln!(
                    "Expected: separator('_') caused deserialization failure: {e}"
                );
            }
        }

        // Cleanup
        unsafe {
            std::env::remove_var("SIDECAR_CREDIBLE_ASSERTION_DA_RPC_URL");
        }
    }

    // ---------------------------------------------------------------
    // Test 5: Double-underscore env vars work correctly
    // ---------------------------------------------------------------
    // This proves that the "__" separator convention DOES work — but it
    // requires renaming all env vars (breaking change).
    #[test]
    fn test_double_underscore_env_vars_work() {
        let _lock = ENV_LOCK.lock().unwrap();

        unsafe {
            std::env::set_var(
                "SIDECAR__CREDIBLE__ASSERTION_DA_RPC_URL",
                "http://overridden:9999",
            );
        }

        let cfg = load(TEST_JSON, CliOverrides::default()).unwrap();

        // With separator("__"), the env var correctly maps to
        // credible.assertion_da_rpc_url
        assert_eq!(
            cfg.credible.assertion_da_rpc_url,
            "http://overridden:9999"
        );

        unsafe {
            std::env::remove_var("SIDECAR__CREDIBLE__ASSERTION_DA_RPC_URL");
        }
    }
}
