//! Shared helpers for transport decoding/parsing across HTTP and gRPC.

use crate::transport::common::HttpDecoderError::*;
use revm::{
    context::TxEnv,
    primitives::{
        Address,
        Bytes,
        TxKind,
        U256,
    },
};
use std::str::FromStr;

#[derive(thiserror::Error, Debug, Clone)]
pub enum HttpDecoderError {
    #[error("Request not in proper schema")]
    SchemaError,
    #[error("Invalid address format: {0}")]
    InvalidAddress(String),
    #[error("Invalid hash format: {0}")]
    InvalidHash(String),
    #[error("Invalid hex value: {0}")]
    InvalidHex(String),
    #[error("Missing transaction parameters")]
    MissingParams,
    #[error("No transactions found in request")]
    NoTransactions,
}

/// Parse `transact_to` into a `TxKind`.
/// Treats `None`, empty string, or "0x" as a contract creation.
pub fn parse_tx_kind_opt(transact_to: Option<&str>) -> Result<TxKind, HttpDecoderError> {
    match transact_to {
        None | Some("" | "0x") => Ok(TxKind::Create),
        Some(addr_str) => {
            let addr =
                Address::from_str(addr_str).map_err(|_| InvalidAddress(addr_str.to_string()))?;
            Ok(TxKind::Call(addr))
        }
    }
}

/// Parse hex data that may be empty, 0x-prefixed, or raw hex.
pub fn parse_hex_data(data: &str) -> Result<Bytes, HttpDecoderError> {
    if data.is_empty() {
        return Ok(Bytes::new());
    }
    let hex = strip_0x(data);
    assertion_executor::primitives::hex::decode(hex)
        .map(Bytes::from)
        .map_err(|_| InvalidHex(data.to_string()))
}

/// Build a TxEnv from string fields shared by HTTP and gRPC shapes.
pub fn to_tx_env_from_fields(
    caller: &str,
    gas_limit: u64,
    gas_price: &str,
    transact_to: Option<&str>,
    value: &str,
    data: &str,
    nonce: u64,
    chain_id: u64,
) -> Result<TxEnv, HttpDecoderError> {
    let caller = Address::from_str(caller).map_err(|_| InvalidAddress(caller.to_string()))?;

    let gas_price: u128 = gas_price
        .parse()
        .map_err(|_| InvalidHex(gas_price.to_string()))?;

    let kind = parse_tx_kind_opt(transact_to)?;

    // Expect decimal value strings for consistency with existing schema/comments
    let value = U256::from_str(value).map_err(|_| InvalidHex(value.to_string()))?;

    let data = parse_hex_data(data)?;

    Ok(TxEnv {
        caller,
        gas_limit,
        gas_price,
        kind,
        value,
        data,
        nonce,
        chain_id: Some(chain_id),
        ..Default::default()
    })
}

#[inline]
pub fn strip_0x(s: &str) -> &str {
    s.strip_prefix("0x").unwrap_or(s)
}
