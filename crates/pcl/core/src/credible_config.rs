//! Shared types and parsing for `credible.toml` deployment configuration files.

use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::{
        BTreeMap,
        HashMap,
    },
    path::Path,
};
use thiserror::Error;
use uuid::Uuid;

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
pub struct CredibleToml {
    pub environment: String,
    #[serde(default)]
    pub project_id: Option<Uuid>,
    pub contracts: BTreeMap<String, CredibleContract>,
}

impl CredibleToml {
    /// Reads and validates a `credible.toml` file at the given path.
    pub fn from_path(path: &Path) -> Result<Self, CredibleConfigError> {
        let contents = std::fs::read_to_string(path).map_err(|e| {
            CredibleConfigError::Io {
                message: format!("credible.toml not found at {}", path.display()),
                source: e,
            }
        })?;
        let credible: Self = toml::from_str(&contents).map_err(CredibleConfigError::Toml)?;
        credible.validate()?;
        Ok(credible)
    }

    /// Runs all config validations.
    fn validate(&self) -> Result<(), CredibleConfigError> {
        self.validate_unique_addresses()
    }

    /// Ensures no two contracts share the same address.
    pub(crate) fn validate_unique_addresses(&self) -> Result<(), CredibleConfigError> {
        let mut seen: HashMap<&str, &str> = HashMap::new();
        for (key, contract) in &self.contracts {
            if let Some(existing_key) = seen.get(contract.address.as_str()) {
                return Err(CredibleConfigError::Invalid(format!(
                    "duplicate contract address {}: used by both `{}` and `{}`",
                    contract.address, existing_key, key
                )));
            }
            seen.insert(&contract.address, key);
        }
        Ok(())
    }
}

/// A contract entry within `credible.toml`.
#[derive(Debug, Deserialize)]
pub struct CredibleContract {
    pub address: String,
    pub name: String,
    pub assertions: Vec<CredibleAssertion>,
}

/// An assertion entry within a contract.
#[derive(Debug, Deserialize)]
pub struct CredibleAssertion {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    const VALID_CREDIBLE_TOML: &str = r#"
        environment = "production"
        [contracts.my_contract]
        address = "0x1234567890abcdef1234567890abcdef12345678"
        name = "MockProtocol"
        [[contracts.my_contract.assertions]]
        file = "src/NoArgsAssertion.a.sol"
    "#;

    #[test]
    fn infers_assertion_contract_name_from_solidity_path() {
        assert_eq!(
            assertion_contract_name("assertions/src/MockAssertion.a.sol").unwrap(),
            "MockAssertion"
        );
        assert_eq!(
            assertion_contract_name("assertions/src/Other.sol:NamedAssertion").unwrap(),
            "NamedAssertion"
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
            file = "src/OwnableAssertion.a.sol"

            [contracts.ownable]
            address = "0xC9734723aAD51626dC9244fed32668ccb280856A"
            name = "Ownable2"
            [[contracts.ownable.assertions]]
            file = "src/OwnableAssertion.a.sol"
        "#;
        let result = toml::from_str::<CredibleToml>(toml_str);
        assert!(result.is_err(), "TOML should reject duplicate keys");
    }

    #[test]
    fn rejects_duplicate_contract_addresses() {
        let toml_str = r#"
            environment = "production"
            [contracts.ownable]
            address = "0xD1f444eA1D2d9fA567F8fD73b15199F90e630074"
            name = "Ownable"
            [[contracts.ownable.assertions]]
            file = "src/OwnableAssertion.a.sol"

            [contracts.ownable2]
            address = "0xD1f444eA1D2d9fA567F8fD73b15199F90e630074"
            name = "Ownable2"
            [[contracts.ownable2.assertions]]
            file = "src/OwnableAssertion.a.sol"
        "#;
        let credible: CredibleToml = toml::from_str(toml_str).unwrap();
        let err = credible.validate_unique_addresses().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("duplicate contract address"),
            "expected duplicate address error, got: {msg}"
        );
        assert!(msg.contains("0xD1f444eA1D2d9fA567F8fD73b15199F90e630074"));
    }

    #[test]
    fn accepts_distinct_contract_addresses() {
        let toml_str = r#"
            environment = "production"
            [contracts.ownable]
            address = "0xD1f444eA1D2d9fA567F8fD73b15199F90e630074"
            name = "Ownable"
            [[contracts.ownable.assertions]]
            file = "src/OwnableAssertion.a.sol"

            [contracts.ownable2]
            address = "0xC9734723aAD51626dC9244fed32668ccb280856A"
            name = "Ownable2"
            [[contracts.ownable2.assertions]]
            file = "src/OwnableAssertion.a.sol"
        "#;
        let credible: CredibleToml = toml::from_str(toml_str).unwrap();
        credible.validate_unique_addresses().unwrap();
    }

    #[test]
    fn reads_credible_toml_from_path() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();

        let assertions_dir = root.join("assertions");
        fs::create_dir_all(&assertions_dir).unwrap();
        fs::write(assertions_dir.join("credible.toml"), VALID_CREDIBLE_TOML).unwrap();

        let credible = CredibleToml::from_path(&root.join("assertions/credible.toml")).unwrap();
        assert_eq!(credible.environment, "production");
        assert_eq!(
            credible.contracts.get("my_contract").unwrap().name,
            "MockProtocol"
        );
    }

    #[test]
    fn from_path_returns_error_for_missing_file() {
        let tmp = TempDir::new().unwrap();
        let err =
            CredibleToml::from_path(&tmp.path().join("nonexistent/credible.toml")).unwrap_err();
        assert!(matches!(err, CredibleConfigError::Io { .. }));
    }
}
