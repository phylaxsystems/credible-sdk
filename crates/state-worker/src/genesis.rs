//! Helpers for hydrating the worker's view of block 0 from a genesis JSON file.
use alloy::primitives::{
    Address,
    B256,
    Bytes,
    U256,
    keccak256,
};
use anyhow::{
    Context,
    Result,
    anyhow,
};
use mdbx::AccountState;
use revm::primitives::KECCAK_EMPTY;
use serde::Deserialize;
use std::{
    collections::HashMap,
    str::FromStr,
};

/// Parsed representation of the genesis state. The worker consumes this when
/// hydrating block 0 in Redis.
pub struct GenesisState {
    accounts: Vec<AccountState>,
    config: Config,
}

impl GenesisState {
    /// Immutable view of the parsed account commits.
    #[cfg(test)]
    pub fn accounts(&self) -> &[AccountState] {
        &self.accounts
    }

    /// Consume the state and return the owned account commits.
    pub fn into_accounts(self) -> Vec<AccountState> {
        self.accounts
    }

    /// Immutable view of the parsed genesis config.
    pub fn config(&self) -> &Config {
        &self.config
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct GenesisFile {
    config: Config,
    alloc: HashMap<String, GenesisAccount>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Config {
    pub cancun_time: Option<u64>,
    pub prague_time: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct GenesisAccount {
    balance: Option<String>,
    nonce: Option<String>,
    code: Option<String>,
    storage: HashMap<String, String>,
}

/// Parse accounts from a genesis JSON blob.
pub fn parse_from_str(data: &str) -> Result<GenesisState> {
    let genesis: GenesisFile =
        serde_json::from_str(data).context("failed to deserialize genesis JSON")?;
    build_state(genesis)
}

fn build_state(genesis: GenesisFile) -> Result<GenesisState> {
    let mut accounts = Vec::with_capacity(genesis.alloc.len());
    for (address, account) in genesis.alloc {
        let commit = convert_account(&address, account)
            .with_context(|| format!("failed to parse alloc entry for address {address}"))?;
        accounts.push(commit);
    }

    accounts.sort_by(|a, b| a.address_hash.cmp(&b.address_hash));
    Ok(GenesisState {
        accounts,
        config: genesis.config,
    })
}

fn convert_account(address: &str, account: GenesisAccount) -> Result<AccountState> {
    let address = parse_address(address)?;
    let balance = parse_u256(account.balance.as_deref())?;
    let nonce = parse_u64(account.nonce.as_deref())
        .with_context(|| format!("failed to parse nonce for address {address}"))?;
    let (code, code_hash) = parse_code(account.code.as_deref())?;
    let storage = parse_storage(account.storage)?;

    Ok(AccountState {
        address_hash: address.into(),
        balance,
        nonce,
        code_hash,
        code,
        storage,
        deleted: false,
    })
}

fn parse_address(value: &str) -> Result<Address> {
    let formatted = if value.starts_with("0x") || value.starts_with("0X") {
        value.to_string()
    } else {
        format!("0x{value}")
    };
    Address::from_str(&formatted).map_err(|err| anyhow!("failed to parse address {value}: {err}"))
}

fn parse_u256(value: Option<&str>) -> Result<U256> {
    let value = value.unwrap_or("0x0").trim();
    U256::from_str(value).map_err(|err| anyhow!("failed to parse numeric value: {err}"))
}

fn parse_u64(value: Option<&str>) -> Result<u64> {
    let value = value.unwrap_or("0x0").trim();
    if let Some(hex_str) = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
    {
        u64::from_str_radix(hex_str, 16)
            .map_err(|err| anyhow!("failed to parse u64 value {value}: {err}"))
    } else {
        u64::from_str(value).map_err(|err| anyhow!("failed to parse u64 value {value}: {err}"))
    }
}

fn parse_code(code: Option<&str>) -> Result<(Option<Bytes>, B256)> {
    let Some(code) = code else {
        return Ok((None, KECCAK_EMPTY));
    };

    let bytes = decode_hex_bytes(code)?;
    if bytes.is_empty() {
        return Ok((None, KECCAK_EMPTY));
    }

    let hash = keccak256(&bytes);
    Ok((Some(bytes), hash))
}

fn parse_storage(storage: HashMap<String, String>) -> Result<HashMap<B256, U256>> {
    let mut entries = HashMap::new();
    for (slot, value) in storage {
        let slot = parse_u256(Some(&slot))
            .with_context(|| format!("failed to parse storage slot key {slot}"))?;
        let value = parse_u256(Some(&value))
            .with_context(|| format!("failed to parse storage slot value {value}"))?;
        let slot_hash = keccak256(slot.to_be_bytes::<32>());
        entries.insert(slot_hash, value);
    }

    Ok(entries)
}

fn decode_hex_bytes(value: &str) -> Result<Bytes> {
    let normalized = if let Some(hex_str) = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
    {
        if hex_str.len() % 2 != 0 {
            format!("0x0{hex_str}")
        } else {
            value.to_string()
        }
    } else {
        value.to_string()
    };

    Bytes::from_str(&normalized).map_err(|err| anyhow!("failed to decode hex value {value}: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{
        Context,
        Result,
    };

    #[test]
    fn test_parse_valid_genesis() -> Result<()> {
        let genesis_json = r#"{
            "alloc": {
                "00000000000000000000000000000000000000f0": {
                    "balance": "0x100",
                    "nonce": "0x5",
                    "code": "0x6080604052",
                    "storage": {
                        "0x0": "0x123",
                        "0x1": "0x456"
                    }
                }
            }
        }"#;

        let state = parse_from_str(genesis_json).context("should parse valid genesis")?;
        assert_eq!(state.accounts().len(), 1);

        let account = state.accounts().first().context("missing account")?;
        assert_eq!(
            account.address_hash,
            Address::from_str("00000000000000000000000000000000000000f0")
                .context("failed to parse account address")?
                .into()
        );
        assert_eq!(account.balance, U256::from(0x100));
        assert_eq!(account.nonce, 5);
        assert_eq!(account.storage.len(), 2);
        Ok(())
    }

    #[test]
    fn test_parse_genesis_with_missing_fields() -> Result<()> {
        let genesis_json = r#"{
            "alloc": {
                "0000000000000000000000000000000000000002": {
                    "balance": "0x200"
                }
            }
        }"#;

        let state =
            parse_from_str(genesis_json).context("should parse with missing optional fields")?;
        assert_eq!(state.accounts().len(), 1);

        let account = state.accounts().first().context("missing account")?;
        assert_eq!(account.balance, U256::from(0x200));
        assert_eq!(account.nonce, 0);
        assert_eq!(account.code, None);
        assert_eq!(account.code_hash, KECCAK_EMPTY);
        assert_eq!(account.storage.len(), 0);
        Ok(())
    }

    #[test]
    fn test_parse_empty_genesis() -> Result<()> {
        let genesis_json = r#"{"alloc": {}}"#;
        let state = parse_from_str(genesis_json).context("should parse empty genesis")?;
        assert_eq!(state.accounts().len(), 0);
        Ok(())
    }

    #[test]
    fn test_parse_malformed_json() {
        let malformed = r#"{"alloc": {"invalid json"#;
        assert!(
            parse_from_str(malformed).is_err(),
            "should fail on malformed JSON"
        );
    }

    #[test]
    fn test_parse_invalid_address() {
        let genesis_json = r#"{
            "alloc": {
                "not_a_valid_address": {
                    "balance": "0x100"
                }
            }
        }"#;

        assert!(
            parse_from_str(genesis_json).is_err(),
            "should fail on invalid address"
        );
    }

    #[test]
    fn test_parse_invalid_balance() {
        let genesis_json = r#"{
            "alloc": {
                "0000000000000000000000000000000000000001": {
                    "balance": "not_a_number"
                }
            }
        }"#;

        assert!(
            parse_from_str(genesis_json).is_err(),
            "should fail on invalid balance"
        );
    }

    #[test]
    fn test_parse_invalid_nonce() {
        let genesis_json = r#"{
            "alloc": {
                "0000000000000000000000000000000000000001": {
                    "balance": "0x100",
                    "nonce": "not_a_number"
                }
            }
        }"#;

        assert!(
            parse_from_str(genesis_json).is_err(),
            "should fail on invalid nonce"
        );
    }

    #[test]
    fn test_parse_invalid_hex_code() {
        let genesis_json = r#"{
            "alloc": {
                "0000000000000000000000000000000000000001": {
                    "balance": "0x100",
                    "code": "0xZZZZ"
                }
            }
        }"#;

        assert!(
            parse_from_str(genesis_json).is_err(),
            "should fail on invalid hex in code"
        );
    }

    #[test]
    fn test_parse_invalid_storage_slot() {
        let genesis_json = r#"{
            "alloc": {
                "0000000000000000000000000000000000000001": {
                    "balance": "0x100",
                    "storage": {
                        "not_hex": "0x123"
                    }
                }
            }
        }"#;

        assert!(
            parse_from_str(genesis_json).is_err(),
            "should fail on invalid storage slot key"
        );
    }

    #[test]
    fn test_parse_invalid_storage_value() {
        let genesis_json = r#"{
            "alloc": {
                "0000000000000000000000000000000000000001": {
                    "balance": "0x100",
                    "storage": {
                        "0x0": "not_hex"
                    }
                }
            }
        }"#;

        assert!(
            parse_from_str(genesis_json).is_err(),
            "should fail on invalid storage value"
        );
    }

    #[test]
    fn test_parse_oversized_address() {
        // Address longer than 20 bytes
        let genesis_json = r#"{
            "alloc": {
                "0000000000000000000000000000000000000000000000": {
                    "balance": "0x100"
                }
            }
        }"#;

        assert!(
            parse_from_str(genesis_json).is_err(),
            "should fail on oversized address"
        );
    }

    #[test]
    fn test_parse_oversized_storage_value() {
        // Value longer than 32 bytes (64 hex chars)
        let oversized_value = format!("0x{}", "1".repeat(65));
        let genesis_json = format!(
            r#"{{
                "alloc": {{
                    "0000000000000000000000000000000000000001": {{
                        "balance": "0x100",
                        "storage": {{
                            "0x0": "{oversized_value}"
                        }}
                    }}
                }}
            }}"#
        );

        assert!(
            parse_from_str(&genesis_json).is_err(),
            "should fail on oversized storage value"
        );
    }

    #[test]
    fn test_parse_hex_with_uppercase() -> Result<()> {
        let genesis_json = r#"{
            "alloc": {
                "0000000000000000000000000000000000000001": {
                    "balance": "0xABCDEF",
                    "nonce": "0xA"
                }
            }
        }"#;

        let state = parse_from_str(genesis_json).context("should parse uppercase hex")?;
        assert_eq!(state.accounts().len(), 1);
        let account = state.accounts().first().context("missing account")?;
        assert_eq!(account.balance, U256::from(0x00AB_CDEF));
        assert_eq!(account.nonce, 10);
        Ok(())
    }

    #[test]
    fn test_parse_hex_without_prefix() -> Result<()> {
        let genesis_json = r#"{
            "alloc": {
                "0000000000000000000000000000000000000001": {
                    "balance": "256",
                    "nonce": "10"
                }
            }
        }"#;

        let state = parse_from_str(genesis_json).context("should parse decimal values")?;
        assert_eq!(state.accounts().len(), 1);
        let account = state.accounts().first().context("missing account")?;
        assert_eq!(account.balance, U256::from(256));
        assert_eq!(account.nonce, 10);
        Ok(())
    }

    #[test]
    fn test_parse_empty_code() -> Result<()> {
        let genesis_json = r#"{
            "alloc": {
                "0000000000000000000000000000000000000001": {
                    "balance": "0x100",
                    "code": "0x"
                }
            }
        }"#;

        let state = parse_from_str(genesis_json).context("should handle empty code")?;
        assert_eq!(state.accounts().len(), 1);
        let account = state.accounts().first().context("missing account")?;
        assert_eq!(account.code, None);
        assert_eq!(account.code_hash, KECCAK_EMPTY);
        Ok(())
    }

    #[test]
    fn test_parse_multiple_accounts() -> Result<()> {
        let genesis_json = r#"{
            "alloc": {
                "0000000000000000000000000000000000000001": {
                    "balance": "0x100"
                },
                "0000000000000000000000000000000000000002": {
                    "balance": "0x200"
                },
                "0000000000000000000000000000000000000003": {
                    "balance": "0x300"
                }
            }
        }"#;

        let state = parse_from_str(genesis_json).context("should parse multiple accounts")?;
        assert_eq!(state.accounts().len(), 3);

        // Verify accounts are sorted by address
        for window in state.accounts().windows(2) {
            let first = window.first().context("missing first account")?;
            let second = window.get(1).context("missing second account")?;
            assert!(
                first.address_hash < second.address_hash,
                "accounts should be sorted by address"
            );
        }
        Ok(())
    }
}
