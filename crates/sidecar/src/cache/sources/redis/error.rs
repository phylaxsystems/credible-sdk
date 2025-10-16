//! redis error types

use crate::cache::sources::SourceError;
use assertion_executor::primitives::B256;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RedisCacheError {
    #[error("redis backend error: {0}")]
    Backend(#[source] redis::RedisError),
    #[error("missing field '{field}' for key '{key}'")]
    MissingField { key: String, field: &'static str },
    #[error("invalid integer for '{key}.{field}': {source}")]
    InvalidInteger {
        key: String,
        field: &'static str,
        source: std::num::ParseIntError,
    },
    #[error("invalid u256 for '{key}.{field}': {source}")]
    InvalidU256 {
        key: String,
        field: &'static str,
        source: alloy::primitives::ruint::ParseError,
    },
    #[error("invalid hex for '{kind}': {source}")]
    InvalidHex {
        kind: &'static str,
        source: hex::FromHexError,
    },
    #[error("hex value for '{kind}' must be at most 32 bytes")]
    HexLength { kind: &'static str },
    #[error("block hash not found for block {0}")]
    BlockHashNotFound(u64),
    #[error("bytecode not found for hash {0}")]
    CodeNotFound(B256),
    #[error("cache miss for '{kind}'")]
    CacheMiss { kind: &'static str },
    #[error("{0}")]
    Other(String),
}

impl From<RedisCacheError> for SourceError {
    fn from(value: RedisCacheError) -> Self {
        match value {
            RedisCacheError::Backend(err) => SourceError::Request(Box::new(err)),
            RedisCacheError::BlockHashNotFound(_) => SourceError::BlockNotFound,
            RedisCacheError::CodeNotFound(_) => SourceError::CodeByHashNotFound,
            RedisCacheError::CacheMiss { .. } => SourceError::CacheMiss,
            other => SourceError::Other(other.to_string()),
        }
    }
}
