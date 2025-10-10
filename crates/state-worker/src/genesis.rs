//! Helpers for hydrating the worker's view of block 0 from a genesis JSON file.
use crate::{
    genesis_data,
    state::AccountCommit,
};
use alloy::primitives::{
    Address,
    B256,
    U256,
    keccak256,
};
use anyhow::{
    Context,
    Result,
    anyhow,
};
use revm::primitives::KECCAK_EMPTY;
use serde::Deserialize;
use std::{
    borrow::Cow,
    collections::HashMap,
    str::FromStr,
};

/// Parsed representation of the genesis state. The worker consumes this when
/// hydrating block 0 in Redis.
pub struct GenesisState {
    accounts: Vec<AccountCommit>,
}

impl GenesisState {
    /// Immutable view of the parsed account commits.
    #[cfg(test)]
    pub fn accounts(&self) -> &[AccountCommit] {
        &self.accounts
    }

    /// Consume the state and return the owned account commits.
    pub fn into_accounts(self) -> Vec<AccountCommit> {
        self.accounts
    }
}

#[derive(Debug, Deserialize)]
struct GenesisFile {
    #[serde(default)]
    alloc: HashMap<String, GenesisAccount>,
}

#[derive(Debug, Default, Deserialize)]
struct GenesisAccount {
    #[serde(default)]
    balance: Option<String>,
    #[serde(default)]
    nonce: Option<String>,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    storage: HashMap<String, String>,
}

/// Parse accounts from a genesis JSON blob.
pub fn parse_from_str(data: &str) -> Result<GenesisState> {
    let genesis: GenesisFile =
        serde_json::from_str(data).context("failed to deserialize genesis JSON")?;
    build_state(genesis)
}

/// Parse accounts from an embedded JSON value.
pub fn parse_from_value(value: &serde_json::Value) -> Result<GenesisState> {
    let data = serde_json::to_string(value).context("failed to serialize genesis JSON value")?;
    parse_from_str(&data)
}

/// Load a genesis state for a known chain id embedded in the binary.
pub fn load_embedded(chain_id: u64) -> Result<GenesisState> {
    let value = genesis_data::for_chain_id(chain_id)
        .ok_or_else(|| anyhow!("no embedded genesis for chain id {chain_id}"))?;
    parse_from_value(value)
}

fn build_state(genesis: GenesisFile) -> Result<GenesisState> {
    let mut accounts = Vec::with_capacity(genesis.alloc.len());
    for (address, account) in genesis.alloc {
        let commit = convert_account(&address, account)
            .with_context(|| format!("failed to parse alloc entry for address {address}"))?;
        accounts.push(commit);
    }

    accounts.sort_by(|a, b| a.address.cmp(&b.address));
    Ok(GenesisState { accounts })
}

fn convert_account(address: &str, account: GenesisAccount) -> Result<AccountCommit> {
    let address = parse_address(address)?;
    let balance = parse_u256(account.balance.as_deref())?;
    let nonce = parse_u64(account.nonce.as_deref())
        .with_context(|| format!("failed to parse nonce for address {address}"))?;
    let (code, code_hash) = parse_code(account.code.as_deref())?;
    let storage = parse_storage(account.storage)?;

    Ok(AccountCommit {
        address,
        balance,
        nonce,
        code_hash,
        code,
        storage,
        deleted: false,
    })
}

fn parse_address(value: &str) -> Result<Address> {
    let bytes = decode_hex_bytes(value)?;
    if bytes.len() > Address::len_bytes() {
        return Err(anyhow!(
            "address {value} exceeds {} bytes",
            Address::len_bytes()
        ));
    }
    Ok(Address::from_str(value).unwrap())
}

fn parse_u256(value: Option<&str>) -> Result<U256> {
    let value = value.unwrap_or("0x0").trim();
    U256::from_str(value).map_err(|err| anyhow!("failed to parse numeric value: {err}"))
}

fn parse_u64(value: Option<&str>) -> Result<u64> {
    let value = value.unwrap_or("0x0").trim();
    Ok(u64::from_str(value).unwrap())
}

fn parse_code(code: Option<&str>) -> Result<(Option<Vec<u8>>, B256)> {
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

fn parse_storage(storage: HashMap<String, String>) -> Result<Vec<(B256, B256)>> {
    let mut entries = Vec::with_capacity(storage.len());
    for (slot, value) in storage {
        let slot = parse_b256(&slot)
            .with_context(|| format!("failed to parse storage slot key {slot}"))?;
        let value = parse_b256(&value)
            .with_context(|| format!("failed to parse storage slot value {value}"))?;
        entries.push((slot, value));
    }

    entries.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(entries)
}

fn parse_b256(value: &str) -> Result<B256> {
    let bytes = decode_hex_bytes(value)?;
    if bytes.len() > B256::len_bytes() {
        return Err(anyhow!("value {value} exceeds {} bytes", B256::len_bytes()));
    }
    Ok(B256::left_padding_from(&bytes))
}

fn decode_hex_bytes(value: &str) -> Result<Vec<u8>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    let without_prefix = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);
    if without_prefix.is_empty() {
        return Ok(Vec::new());
    }

    let normalized: Cow<'_, str> = if without_prefix.len() % 2 == 0 {
        Cow::Borrowed(without_prefix)
    } else {
        let mut owned = String::with_capacity(without_prefix.len() + 1);
        owned.push('0');
        owned.push_str(without_prefix);
        Cow::Owned(owned)
    };

    hex::decode(normalized.as_ref())
        .map_err(|err| anyhow!("failed to decode hex value {value}: {err}"))
}
