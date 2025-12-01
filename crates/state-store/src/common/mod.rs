//! Common types, configuration, and utilities shared between reader and writer.

pub mod error;

use alloy::primitives::{
    Address,
    B256,
    U256,
    keccak256,
};
use error::{
    StateError,
    StateResult,
};
use std::{
    collections::HashMap,
    sync::Arc,
};

/// Type for defining the keccak256(address)
#[derive(
    Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct AddressHash(B256);

impl AddressHash {
    pub fn new<T: AsRef<[u8]>>(bytes: T) -> Self {
        Self(keccak256(bytes))
    }
}

impl From<B256> for AddressHash {
    fn from(hash: B256) -> Self {
        Self(hash)
    }
}

impl From<Address> for AddressHash {
    fn from(address: Address) -> Self {
        Self(keccak256(address))
    }
}

impl AsRef<[u8]> for AddressHash {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

/// Redis key prefixes and separators
pub mod keys {
    /// Separator between namespace components
    pub const SEPARATOR: &str = ":";

    /// Key suffix for block number in namespace
    pub const BLOCK: &str = "block";

    /// Key prefix for account data
    pub const ACCOUNT: &str = "account";

    /// Key prefix for storage data
    pub const STORAGE: &str = "storage";

    /// Key prefix for contract code
    pub const CODE: &str = "code";

    /// Key prefix for state diffs
    pub const DIFF: &str = "diff";

    /// Key prefix for block hashes (shared across namespaces)
    pub const BLOCK_HASH: &str = "block_hash";

    /// Key prefix for state roots (shared across namespaces)
    pub const STATE_ROOT: &str = "state_root";

    /// Key for storing the latest block number globally
    pub const LATEST_BLOCK: &str = "meta:latest_block";

    /// Key for recording the configured namespace rotation length
    pub const STATE_DUMP_INDICES: &str = "state_dump_indices";
}

/// Configuration for the circular buffer of states in Redis.
#[derive(Clone, Debug)]
pub struct CircularBufferConfig {
    /// Number of historical states to maintain
    pub buffer_size: usize,
}

impl CircularBufferConfig {
    pub fn new(buffer_size: usize) -> StateResult<Self> {
        if buffer_size <= 1 {
            return Err(StateError::InvalidBufferSize);
        }
        Ok(Self { buffer_size })
    }
}

impl Default for CircularBufferConfig {
    fn default() -> Self {
        Self { buffer_size: 3 }
    }
}

/// Get the namespace for a given block number using circular buffer logic.
pub fn get_namespace_for_block(
    base_namespace: &str,
    block_number: u64,
    buffer_size: usize,
) -> StateResult<String> {
    let namespace_idx = usize::try_from(block_number)? % buffer_size;
    Ok(format!(
        "{base_namespace}{}{namespace_idx}",
        keys::SEPARATOR
    ))
}

/// Get the key for storing state diffs.
pub fn get_diff_key(base_namespace: &str, block_number: u64) -> String {
    format!(
        "{base_namespace}{}{}{}{block_number}",
        keys::SEPARATOR,
        keys::DIFF,
        keys::SEPARATOR
    )
}

/// Get the key for block number in a namespace.
pub fn get_block_key(namespace: &str) -> String {
    format!("{namespace}{}{}", keys::SEPARATOR, keys::BLOCK)
}

/// Get the key for account data in a namespace.
pub fn get_account_key(namespace: &str, address_hash: &AddressHash) -> String {
    format!(
        "{namespace}{}{}{}{}",
        keys::SEPARATOR,
        keys::ACCOUNT,
        keys::SEPARATOR,
        hex::encode(address_hash)
    )
}

/// Returns the pattern for all accounts in a namespace.
pub fn get_all_accounts_patter(namespace: &str) -> String {
    format!(
        "{namespace}{}{}{}*",
        keys::SEPARATOR,
        keys::ACCOUNT,
        keys::SEPARATOR,
    )
}
/// Get the key for storage data in a namespace.
pub fn get_storage_key(namespace: &str, address_hash: &AddressHash) -> String {
    format!(
        "{namespace}{}{}{}{}",
        keys::SEPARATOR,
        keys::STORAGE,
        keys::SEPARATOR,
        hex::encode(address_hash)
    )
}

/// Get the key for contract code in a namespace.
pub fn get_code_key(namespace: &str, code_hash_hex: &str) -> String {
    format!(
        "{namespace}{}{}{}{}",
        keys::SEPARATOR,
        keys::CODE,
        keys::SEPARATOR,
        code_hash_hex
    )
}

/// Get the key for block hash (shared across namespaces).
pub fn get_block_hash_key(base_namespace: &str, block_number: u64) -> String {
    format!(
        "{base_namespace}{}{}{}{block_number}",
        keys::SEPARATOR,
        keys::BLOCK_HASH,
        keys::SEPARATOR
    )
}

/// Get the key for state root (shared across namespaces).
pub fn get_state_root_key(base_namespace: &str, block_number: u64) -> String {
    format!(
        "{base_namespace}{}{}{}{block_number}",
        keys::SEPARATOR,
        keys::STATE_ROOT,
        keys::SEPARATOR
    )
}

/// Get the key for the latest block metadata.
pub fn get_latest_block_metadata_key(base_namespace: &str) -> String {
    format!(
        "{}{}{}",
        base_namespace,
        keys::SEPARATOR,
        keys::LATEST_BLOCK
    )
}

/// Get the key storing the configured circular buffer length for dumps.
pub fn get_state_dump_indices_key(base_namespace: &str) -> String {
    format!(
        "{}{}{}",
        base_namespace,
        keys::SEPARATOR,
        keys::STATE_DUMP_INDICES
    )
}

/// Read the latest block number from top-level metadata (O(1) operation).
/// Falls back to scanning all namespaces if metadata is not available.
pub fn read_latest_block_number<C>(conn: &mut C, base_namespace: &str) -> StateResult<Option<u64>>
where
    C: redis::ConnectionLike,
{
    let meta_key = get_latest_block_metadata_key(base_namespace);
    let value: Option<String> = redis::cmd("GET").arg(&meta_key).query(conn)?;

    if let Some(v) = value {
        // Metadata exists, use it
        Ok(Some(
            v.parse::<u64>().map_err(|e| StateError::ParseInt(v, e))?,
        ))
    } else {
        Ok(None)
    }
}

/// Update metadata atomically within a pipeline.
pub fn update_metadata_in_pipe(
    pipe: &mut redis::Pipeline,
    base_namespace: &str,
    block_number: u64,
    buffer_size: usize,
) {
    // Update latest block
    let latest_key = get_latest_block_metadata_key(base_namespace);
    pipe.set(&latest_key, block_number.to_string());

    // Record the configured namespace rotation length for external tooling.
    let indices_key = get_state_dump_indices_key(base_namespace);
    pipe.set(&indices_key, buffer_size.to_string());
}

/// Ensure the state dump indices metadata matches the configured buffer size.
pub fn ensure_state_dump_indices<C>(
    conn: &mut C,
    base_namespace: &str,
    buffer_size: usize,
) -> StateResult<()>
where
    C: redis::ConnectionLike,
{
    let key = get_state_dump_indices_key(base_namespace);
    let value: Option<String> = redis::cmd("GET").arg(&key).query(conn)?;

    if let Some(existing) = value {
        let parsed = existing
            .parse::<usize>()
            .map_err(|e| StateError::ParseInt(existing.clone(), e))?;
        if parsed != buffer_size {
            return Err(StateError::StateDumpIndexMismatch {
                existing: parsed,
                requested: buffer_size,
            });
        }
    } else {
        redis::cmd("SET")
            .arg(&key)
            .arg(buffer_size.to_string())
            .query::<()>(&mut *conn)?;
    }

    Ok(())
}

/// Read the current block number stored in a namespace.
pub fn read_namespace_block_number<C>(conn: &mut C, namespace: &str) -> StateResult<Option<u64>>
where
    C: redis::ConnectionLike,
{
    let block_key = get_block_key(namespace);
    let value: Option<String> = redis::cmd("GET").arg(&block_key).query(conn)?;

    value
        .map(|v| v.parse::<u64>().map_err(|e| StateError::ParseInt(v, e)))
        .transpose()
}

/// Account info without storage slots.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AccountInfo {
    pub address_hash: AddressHash,
    pub balance: U256,
    pub nonce: u64,
    pub code_hash: B256,
}

/// Complete account state with all fields including storage.
/// This is the canonical representation used by both reader and writer.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AccountState {
    pub address_hash: AddressHash,
    pub balance: U256,
    pub nonce: u64,
    pub code_hash: B256,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    /// Storage slots keyed by `keccak256(slot)` (hashed slot indices).
    pub storage: HashMap<U256, U256>,
    #[serde(default)]
    pub deleted: bool,
}

/// Complete state update for a block.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BlockStateUpdate {
    pub block_number: u64,
    pub block_hash: B256,
    pub state_root: B256,
    pub accounts: Vec<AccountState>,
}

/// Block metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct BlockMetadata {
    pub block_number: u64,
    pub block_hash: B256,
    pub state_root: B256,
}

/// Base client for Redis operations with connection pooling.
#[derive(Clone, Debug)]
pub struct RedisStateClient {
    client: Arc<redis::Client>,
    pub base_namespace: String,
    pub buffer_config: CircularBufferConfig,
}

impl RedisStateClient {
    /// Create a new Redis client.
    pub fn new(
        redis_url: &str,
        base_namespace: String,
        buffer_config: CircularBufferConfig,
    ) -> StateResult<Self> {
        let client = redis::Client::open(redis_url)?;
        Ok(Self {
            client: Arc::new(client),
            base_namespace,
            buffer_config,
        })
    }

    /// Execute a synchronous Redis operation directly.
    /// This is used for sync contexts (reader).
    pub fn with_connection<T, F>(&self, func: F) -> StateResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut redis::Connection) -> StateResult<T> + Send + 'static,
    {
        let client = self.client.clone();
        let mut conn = client.get_connection()?;
        func(&mut conn)
    }

    /// Get the client for direct access if needed.
    pub fn client(&self) -> &redis::Client {
        &self.client
    }
}

// ============================================================================
// Encoding/decoding helpers
// ============================================================================

/// Helper to render 32-byte words in `0x`-prefixed hex for Redis consumers.
pub fn encode_b256(value: B256) -> String {
    format!("0x{}", hex::encode(value))
}

/// Helper to render arbitrary byte slices for the code cache.
pub fn encode_u256(value: U256) -> String {
    format!("0x{value:064x}")
}

/// Decode a U256 from hex string.
pub fn decode_u256(hex: &str) -> StateResult<U256> {
    let hex = hex.strip_prefix("0x").unwrap_or(hex);
    U256::from_str_radix(hex, 16).map_err(|e| StateError::ParseU256(hex.to_string(), e))
}

/// Decode bytes from hex string.
pub fn encode_bytes(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

/// Decode a B256 from hex string.
pub fn decode_b256(s: &str) -> StateResult<B256> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(s).map_err(|e| StateError::HexDecode(s.to_string(), e))?;
    if bytes.len() != 32 {
        return Err(StateError::InvalidB256Length(bytes.len()));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(B256::from(arr))
}

/// Decode bytes from hex string.
pub fn decode_bytes(s: &str) -> StateResult<Vec<u8>> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    hex::decode(s).map_err(|e| StateError::HexDecode(s.to_string(), e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circular_buffer_config() {
        let config = CircularBufferConfig::new(5).unwrap();
        assert_eq!(config.buffer_size, 5);

        let default_config = CircularBufferConfig::default();
        assert_eq!(default_config.buffer_size, 3);

        let result = CircularBufferConfig::new(0);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StateError::InvalidBufferSize));
    }

    #[test]
    fn test_namespace_calculation() {
        let base = "chain";
        let buffer_size = 3;

        assert_eq!(
            get_namespace_for_block(base, 0, buffer_size).unwrap(),
            "chain:0"
        );
        assert_eq!(
            get_namespace_for_block(base, 1, buffer_size).unwrap(),
            "chain:1"
        );
        assert_eq!(
            get_namespace_for_block(base, 2, buffer_size).unwrap(),
            "chain:2"
        );
        assert_eq!(
            get_namespace_for_block(base, 3, buffer_size).unwrap(),
            "chain:0"
        );
        assert_eq!(
            get_namespace_for_block(base, 4, buffer_size).unwrap(),
            "chain:1"
        );
    }

    #[test]
    fn test_key_generation() {
        let base = "test";
        let namespace = "test:0";
        let address = "1234567890abcdef";
        let code_hash = "fedcba0987654321";

        assert_eq!(get_diff_key(base, 42), "test:diff:42");
        assert_eq!(get_block_key(namespace), "test:0:block");
        assert_eq!(
            get_account_key(namespace, &AddressHash::new(address)),
            "test:0:account:25e4fcd3b1ecd473d5393d9636435394b77da34df77e9474db337f4e980d16d1"
        );
        assert_eq!(
            get_storage_key(namespace, &AddressHash::new(address)),
            "test:0:storage:25e4fcd3b1ecd473d5393d9636435394b77da34df77e9474db337f4e980d16d1"
        );
        assert_eq!(
            get_code_key(namespace, code_hash),
            "test:0:code:fedcba0987654321"
        );
        assert_eq!(get_block_hash_key(base, 100), "test:block_hash:100");
        assert_eq!(get_state_root_key(base, 100), "test:state_root:100");
    }

    #[test]
    fn test_b256_encoding_decoding() {
        let original = B256::from([42u8; 32]);
        let encoded = encode_b256(original);
        assert!(encoded.starts_with("0x"));
        assert_eq!(encoded.len(), 66);

        let decoded = decode_b256(&encoded).unwrap();
        assert_eq!(original, decoded);

        let no_prefix = encoded.strip_prefix("0x").unwrap();
        let decoded2 = decode_b256(no_prefix).unwrap();
        assert_eq!(original, decoded2);
    }

    #[test]
    fn test_bytes_encoding_decoding() {
        let original = vec![1, 2, 3, 4, 5];
        let encoded = encode_bytes(&original);
        assert!(encoded.starts_with("0x"));

        let decoded = decode_bytes(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_decode_b256_invalid() {
        let result = decode_b256("0x1234");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            StateError::InvalidB256Length(_)
        ));

        let result = decode_b256("0xGGGG");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StateError::HexDecode(_, _)));
    }

    #[test]
    fn test_metadata_key_generation() {
        let base = "chain";

        assert_eq!(
            get_latest_block_metadata_key(base),
            "chain:meta:latest_block"
        );
    }
}
