//! Shared types and parsing for `credible.toml` deployment configuration files.

use alloy_primitives::Address;
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::{
        BTreeMap,
        HashMap,
        HashSet,
    },
    path::{
        Path,
        PathBuf,
    },
};
use thiserror::Error;
use uuid::Uuid;

const DEFAULT_ASSERTIONS_DIR: &str = "assertions/src";

/// Errors from reading or validating `credible.toml`.
#[derive(Error, Debug)]
pub enum CredibleConfigError {
    #[error("{message}: {source}")]
    Io {
        message: String,
        #[source]
        source: std::io::Error,
    },

    #[error("Failed to parse credible.toml: {0}")]
    Toml(#[source] toml::de::Error),

    #[error("Invalid credible.toml: {0}")]
    Invalid(String),
}

/// Root structure of a `credible.toml` file.
#[derive(Debug, Deserialize)]
pub struct CredibleConfig {
    pub environment: String,
    #[serde(default)]
    pub project_id: Option<Uuid>,
    #[serde(default)]
    pub assertions_dir: Option<PathBuf>,
    pub contracts: BTreeMap<String, ContractConfig>,
}

impl CredibleConfig {
    /// Reads and validates a `credible.toml` file at the given path.
    ///
    /// Performs structural validation (addresses, duplicates, extensions, etc.)
    /// after parsing. Collects all errors before reporting.
    pub fn from_path(path: &Path, root: &Path) -> Result<Self, CredibleConfigError> {
        let contents = std::fs::read_to_string(path).map_err(|e| {
            CredibleConfigError::Io {
                message: format!("credible.toml not found at {}", path.display()),
                source: e,
            }
        })?;
        let credible: Self = toml::from_str(&contents).map_err(CredibleConfigError::Toml)?;
        credible.validate(root)?;
        Ok(credible)
    }

    /// Returns the configured assertions directory, or the default `"assertions/src"`.
    pub fn assertions_dir(&self) -> &Path {
        self.assertions_dir
            .as_deref()
            .unwrap_or(Path::new(DEFAULT_ASSERTIONS_DIR))
    }

    /// Validates config contents and checks assertion files exist on disk.
    ///
    /// Collects all errors before reporting so the user can fix multiple
    /// problems in one pass.
    fn validate(&self, root: &Path) -> Result<(), CredibleConfigError> {
        let mut errors = Vec::new();
        let assertions_dir = self.assertions_dir();

        if self.environment.is_empty() {
            errors.push("'environment' must not be empty".to_string());
        }

        let mut seen_addresses: HashMap<&str, &str> = HashMap::new();
        for (label, contract) in &self.contracts {
            if contract.address.parse::<Address>().is_err() {
                errors.push(format!(
                    "Contract '{label}': invalid Ethereum address '{}'",
                    contract.address
                ));
            }

            if let Some(existing) = seen_addresses.get(contract.address.as_str()) {
                errors.push(format!(
                    "duplicate contract address {}: used by both '{}' and '{}'",
                    contract.address, existing, label
                ));
            }
            seen_addresses.insert(&contract.address, label);

            let mut seen_assertions: HashSet<(&str, &[String])> = HashSet::new();
            for entry in &contract.assertions {
                if !has_valid_extension(&entry.file) {
                    errors.push(format!(
                        "Contract '{label}': assertion file '{}' \
                         must have .sol or .a.sol extension",
                        entry.file
                    ));
                }

                let file_path = root.join(assertions_dir).join(&entry.file);
                if !file_path.exists() {
                    errors.push(format!(
                        "Contract '{label}': assertion file '{}' not found in {}/",
                        entry.file,
                        assertions_dir.display()
                    ));
                }

                if !seen_assertions.insert((&entry.file, &entry.args)) {
                    errors.push(format!(
                        "Contract '{label}': duplicate assertion '{}'",
                        entry.file
                    ));
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else if errors.len() == 1 {
            Err(CredibleConfigError::Invalid(
                errors.into_iter().next().unwrap_or_default(),
            ))
        } else {
            use std::fmt::Write;
            let items = errors.iter().fold(String::new(), |mut acc, e| {
                let _ = writeln!(acc, "  - {e}");
                acc
            });
            Err(CredibleConfigError::Invalid(format!(
                "multiple validation errors:\n{items}"
            )))
        }
    }
}

/// A contract entry within `credible.toml`.
#[derive(Debug, Deserialize)]
pub struct ContractConfig {
    pub address: String,
    pub name: String,
    pub assertions: Vec<AssertionEntry>,
}

/// An assertion entry within a contract.
#[derive(Debug, Deserialize)]
pub struct AssertionEntry {
    pub file: String,
    #[serde(default, deserialize_with = "deserialize_args")]
    pub args: Vec<String>,
}

fn deserialize_args<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::Array(values) => Ok(values.into_iter().map(value_to_string).collect()),
        Value::Null => Ok(vec![]),
        other => Ok(vec![value_to_string(other)]),
    }
}

fn value_to_string(value: Value) -> String {
    match value {
        Value::String(s) => s,
        other => other.to_string(),
    }
}

/// Extracts the contract name from a file path or qualified `file:contract` name.
///
/// Supports:
/// - `file.sol:ContractName` -> `ContractName`
/// - `ContractName.a.sol` -> `ContractName`
/// - `ContractName.sol` -> `ContractName`
pub fn assertion_contract_name(file: &str) -> Result<String, CredibleConfigError> {
    if let Some((_, contract_name)) = file.rsplit_once(':') {
        return Ok(contract_name.to_string());
    }

    let file_name = Path::new(file)
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            CredibleConfigError::Invalid(format!("Invalid assertion file path: {file}"))
        })?;

    for suffix in [".a.sol", ".sol"] {
        if let Some(contract_name) = file_name.strip_suffix(suffix) {
            return Ok(contract_name.to_string());
        }
    }

    Err(CredibleConfigError::Invalid(format!(
        "Could not infer assertion contract from file {file}"
    )))
}

fn has_valid_extension(file: &str) -> bool {
    file.ends_with(".a.sol")
        || Path::new(file)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("sol"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    const VALID_CONFIG: &str = r#"
        environment = "production"
        [contracts.my_contract]
        address = "0x1234567890abcdef1234567890abcdef12345678"
        name = "MockProtocol"
        [[contracts.my_contract.assertions]]
        file = "NoArgsAssertion.a.sol"
    "#;

    fn setup_valid_project() -> TempDir {
        let tmp = TempDir::new().unwrap();
        let assertions = tmp.path().join("assertions/src");
        fs::create_dir_all(&assertions).unwrap();
        fs::write(assertions.join("NoArgsAssertion.a.sol"), "// sol").unwrap();
        tmp
    }

    // --- Parsing tests ---

    #[test]
    fn infers_assertion_contract_name_from_solidity_path() {
        assert_eq!(
            assertion_contract_name("MockAssertion.a.sol").unwrap(),
            "MockAssertion"
        );
        assert_eq!(
            assertion_contract_name("Other.sol:NamedAssertion").unwrap(),
            "NamedAssertion"
        );
    }

    #[test]
    fn parses_config_with_assertions_dir() {
        let toml_str = r#"
            environment = "staging"
            assertions_dir = "custom/assertions"
            [contracts.c]
            address = "0x0000000000000000000000000000000000000001"
            name = "C"
            [[contracts.c.assertions]]
            file = "Foo.a.sol"
        "#;
        let config: CredibleConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.assertions_dir(), Path::new("custom/assertions"));
    }

    #[test]
    fn defaults_assertions_dir() {
        let config: CredibleConfig = toml::from_str(VALID_CONFIG).unwrap();
        assert_eq!(config.assertions_dir(), Path::new("assertions/src"));
    }

    #[test]
    fn from_path_reads_and_validates() {
        let tmp = setup_valid_project();
        let config_path = tmp.path().join("credible.toml");
        fs::write(&config_path, VALID_CONFIG).unwrap();

        let config = CredibleConfig::from_path(&config_path, tmp.path()).unwrap();
        assert_eq!(config.environment, "production");
        assert_eq!(
            config.contracts.get("my_contract").unwrap().name,
            "MockProtocol"
        );
    }

    #[test]
    fn from_path_returns_error_for_missing_file() {
        let tmp = TempDir::new().unwrap();
        let err =
            CredibleConfig::from_path(&tmp.path().join("missing.toml"), tmp.path()).unwrap_err();
        assert!(matches!(err, CredibleConfigError::Io { .. }));
    }

    // --- Validation tests ---

    #[test]
    fn validate_catches_empty_environment() {
        let tmp = setup_valid_project();
        let toml_str = r#"
            environment = ""
            [contracts.my_contract]
            address = "0x1234567890abcdef1234567890abcdef12345678"
            name = "Mock"
            [[contracts.my_contract.assertions]]
            file = "NoArgsAssertion.a.sol"
        "#;
        let config: CredibleConfig = toml::from_str(toml_str).unwrap();
        let err = config.validate(tmp.path()).unwrap_err();
        assert!(err.to_string().contains("'environment' must not be empty"));
    }

    #[test]
    fn validate_catches_invalid_address() {
        let tmp = setup_valid_project();
        let toml_str = r#"
            environment = "production"
            [contracts.my_contract]
            address = "not-an-address"
            name = "Mock"
            [[contracts.my_contract.assertions]]
            file = "NoArgsAssertion.a.sol"
        "#;
        let config: CredibleConfig = toml::from_str(toml_str).unwrap();
        let err = config.validate(tmp.path()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("invalid Ethereum address"), "got: {msg}");
        assert!(msg.contains("my_contract"), "got: {msg}");
    }

    #[test]
    fn validate_catches_invalid_extension() {
        let tmp = setup_valid_project();
        fs::write(tmp.path().join("assertions/src/Bad.txt"), "// nope").unwrap();
        let toml_str = r#"
            environment = "production"
            [contracts.my_contract]
            address = "0x1234567890abcdef1234567890abcdef12345678"
            name = "Mock"
            [[contracts.my_contract.assertions]]
            file = "Bad.txt"
        "#;
        let config: CredibleConfig = toml::from_str(toml_str).unwrap();
        let err = config.validate(tmp.path()).unwrap_err();
        assert!(err.to_string().contains(".sol or .a.sol extension"));
    }

    #[test]
    fn validate_catches_missing_assertion_file() {
        let tmp = setup_valid_project();
        let toml_str = r#"
            environment = "production"
            [contracts.my_contract]
            address = "0x1234567890abcdef1234567890abcdef12345678"
            name = "Mock"
            [[contracts.my_contract.assertions]]
            file = "Missing.a.sol"
        "#;
        let config: CredibleConfig = toml::from_str(toml_str).unwrap();
        let err = config.validate(tmp.path()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("my_contract"), "missing label in: {msg}");
        assert!(msg.contains("Missing.a.sol"), "missing file in: {msg}");
        assert!(msg.contains("not found"), "missing 'not found' in: {msg}");
    }

    #[test]
    fn validate_catches_duplicate_assertions() {
        let tmp = setup_valid_project();
        let toml_str = r#"
            environment = "production"
            [contracts.my_contract]
            address = "0x1234567890abcdef1234567890abcdef12345678"
            name = "Mock"
            [[contracts.my_contract.assertions]]
            file = "NoArgsAssertion.a.sol"
            [[contracts.my_contract.assertions]]
            file = "NoArgsAssertion.a.sol"
        "#;
        let config: CredibleConfig = toml::from_str(toml_str).unwrap();
        let err = config.validate(tmp.path()).unwrap_err();
        assert!(err.to_string().contains("duplicate assertion"));
    }

    #[test]
    fn validate_allows_same_file_with_different_args() {
        let tmp = setup_valid_project();
        let toml_str = r#"
            environment = "production"
            [contracts.my_contract]
            address = "0x1234567890abcdef1234567890abcdef12345678"
            name = "Mock"
            [[contracts.my_contract.assertions]]
            file = "NoArgsAssertion.a.sol"
            args = ["0x01"]
            [[contracts.my_contract.assertions]]
            file = "NoArgsAssertion.a.sol"
            args = ["0x02"]
        "#;
        let config: CredibleConfig = toml::from_str(toml_str).unwrap();
        config.validate(tmp.path()).unwrap();
    }

    #[test]
    fn validate_catches_duplicate_addresses() {
        let tmp = setup_valid_project();
        let toml_str = r#"
            environment = "production"
            [contracts.ownable]
            address = "0x1234567890abcdef1234567890abcdef12345678"
            name = "Ownable"
            [[contracts.ownable.assertions]]
            file = "NoArgsAssertion.a.sol"

            [contracts.ownable2]
            address = "0x1234567890abcdef1234567890abcdef12345678"
            name = "Ownable2"
            [[contracts.ownable2.assertions]]
            file = "NoArgsAssertion.a.sol"
        "#;
        let config: CredibleConfig = toml::from_str(toml_str).unwrap();
        let err = config.validate(tmp.path()).unwrap_err();
        assert!(err.to_string().contains("duplicate contract address"));
    }

    #[test]
    fn validate_collects_multiple_errors() {
        let tmp = setup_valid_project();
        let toml_str = r#"
            environment = ""
            [contracts.my_contract]
            address = "bad"
            name = "Mock"
            [[contracts.my_contract.assertions]]
            file = "Missing.txt"
        "#;
        let config: CredibleConfig = toml::from_str(toml_str).unwrap();
        let err = config.validate(tmp.path()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("environment"), "missing env error in: {msg}");
        assert!(msg.contains("address"), "missing address error in: {msg}");
        assert!(
            msg.contains("extension"),
            "missing extension error in: {msg}"
        );
    }

    #[test]
    fn toml_rejects_duplicate_contract_keys() {
        let toml_str = r#"
            environment = "production"
            [contracts.ownable]
            address = "0xD1f444eA1D2d9fA567F8fD73b15199F90e630074"
            name = "Ownable"
            [[contracts.ownable.assertions]]
            file = "OwnableAssertion.a.sol"

            [contracts.ownable]
            address = "0xC9734723aAD51626dC9244fed32668ccb280856A"
            name = "Ownable2"
            [[contracts.ownable.assertions]]
            file = "OwnableAssertion.a.sol"
        "#;
        let result = toml::from_str::<CredibleConfig>(toml_str);
        assert!(result.is_err(), "TOML should reject duplicate keys");
    }
}
