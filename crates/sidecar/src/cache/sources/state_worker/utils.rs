use super::error::StateWorkerCacheError;
use assertion_executor::primitives::{
    B256,
    U256,
};
use revm::primitives::StorageKey;
use std::str::FromStr;

/// Parses a decimal `u64` stored in state-worker.
pub(super) fn parse_u64(
    value: &str,
    key: &str,
    field: &'static str,
) -> Result<u64, StateWorkerCacheError> {
    value.trim().parse::<u64>().map_err(|source| {
        StateWorkerCacheError::InvalidInteger {
            key: key.to_string(),
            field,
            source,
        }
    })
}

/// Parses a decimal or hex-encoded `U256` stored in state-worker.
pub(super) fn parse_u256(
    value: &str,
    key: &str,
    field: &'static str,
) -> Result<U256, StateWorkerCacheError> {
    let trimmed = value.trim();
    if let Some(hex) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        let bytes = decode_hex(hex, field)?;
        if bytes.len() > 32 {
            return Err(StateWorkerCacheError::HexLength { kind: field });
        }
        Ok(U256::from_be_slice(&bytes))
    } else {
        U256::from_str(trimmed).map_err(|source| {
            StateWorkerCacheError::InvalidU256 {
                key: key.to_string(),
                field,
                source,
            }
        })
    }
}

/// Parses a 32-byte hash stored in state-worker.
pub(super) fn parse_b256(value: &str, kind: &'static str) -> Result<B256, StateWorkerCacheError> {
    let trimmed = value.trim();
    let hex = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);
    let bytes = decode_hex(hex, kind)?;
    if bytes.len() != 32 {
        return Err(StateWorkerCacheError::HexLength { kind });
    }
    Ok(B256::from_slice(&bytes))
}

/// Decodes a hex string, returning a detailed error on failure.
pub(super) fn decode_hex(
    value: &str,
    kind: &'static str,
) -> Result<Vec<u8>, StateWorkerCacheError> {
    let trimmed = value.trim();
    let hex = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);
    hex::decode(hex).map_err(|source| StateWorkerCacheError::InvalidHex { kind, source })
}

/// Formats bytes as a 0x-prefixed lower-case hex string.
pub(super) fn encode_hex(data: &[u8]) -> String {
    format!("0x{}", hex::encode(data))
}

/// Converts raw bytes to a lower-case hex string without a prefix.
pub(super) fn to_hex_lower(data: &[u8]) -> String {
    hex::encode(data)
}

/// Serializes a storage key into the format stored in state-worker.
pub(super) fn encode_storage_key(slot: StorageKey) -> String {
    let bytes = slot.to_be_bytes::<32>();
    encode_hex(&bytes)
}

/// Serializes a `U256` into the format stored in state-worker.
pub(super) fn encode_u256_hex(value: U256) -> String {
    let bytes = value.to_be_bytes::<32>();
    encode_hex(&bytes)
}
