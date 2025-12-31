//! System contract updates for EIP-2935 and EIP-4788.
//!
//! These EIPs require state modifications at the start of each block,
//! before any user transactions are processed. This module computes
//! those state changes and returns them as `AccountState` records.
//!
//! ## EIP-2935: Historical Block Hashes in State (Prague)
//! Stores the parent block hash in a ring buffer at a system contract.
//! - Contract: `0x0000F90827F1C53a10cb7A02335B175320002935`
//! - Ring buffer size: 8191 slots
//!
//! ## EIP-4788: Beacon Block Root in the EVM (Cancun)
//! Stores beacon chain block roots for trust-minimized consensus layer access.
//! - Contract: `0x000F3df6D732807Ef1319fB7B8bB8522d0Beac02`
//! - Dual ring buffer: timestamps + roots, 8191 slots each

use alloy::{
    eips::{
        eip2935::{
            HISTORY_SERVE_WINDOW,
            HISTORY_STORAGE_ADDRESS,
            HISTORY_STORAGE_CODE,
        },
        eip4788::{
            BEACON_ROOTS_ADDRESS,
            BEACON_ROOTS_CODE,
        },
    },
    primitives::{
        B256,
        Bytes,
        U256,
        keccak256,
    },
};
use anyhow::{
    Result,
    bail,
};
use state_store::{
    AccountState,
    AddressHash,
    Reader,
    redis::StateReader,
};
use std::collections::HashMap;

/// The length of the ring buffer for storing beacon roots.
pub const HISTORY_BUFFER_LENGTH: u64 = 8191;

/// Configuration for system calls at block start
#[derive(Debug, Clone)]
pub struct SystemCallConfig {
    /// Current block number
    pub block_number: u64,
    /// Current block timestamp
    pub timestamp: u64,
    /// Parent block hash (for EIP-2935)
    pub parent_block_hash: Option<B256>,
    /// Parent beacon block root (for EIP-4788)
    pub parent_beacon_block_root: Option<B256>,
}

/// Computes system call state changes for a block.
///
/// Returns a list of `AccountState` records representing the state
/// modifications from EIP-2935 and EIP-4788 system calls.
pub fn compute_system_call_states(
    config: &SystemCallConfig,
    reader: Option<&StateReader>,
) -> Result<Vec<AccountState>> {
    let mut states = Vec::new();

    // EIP-4788: Beacon roots (Cancun+)
    if let Some(state) = compute_eip4788_state(config, reader)? {
        states.push(state);
    }

    // EIP-2935: Historical block hashes (Prague+)
    if let Some(state) = compute_eip2935_state(config, reader)? {
        states.push(state);
    }

    Ok(states)
}

/// Compute EIP-2935 state changes (historical block hashes)
fn compute_eip2935_state(
    config: &SystemCallConfig,
    reader: Option<&StateReader>,
) -> Result<Option<AccountState>> {
    // Skip genesis block
    if config.block_number == 0 {
        return Ok(None);
    }

    let Some(parent_hash) = config.parent_block_hash else {
        bail!(
            "missing parent block hash for EIP-2935 at block {}",
            config.block_number
        )
    };

    let address_hash = AddressHash::from(keccak256(HISTORY_STORAGE_ADDRESS));

    // Storage slot = block_number % HISTORY_SERVE_WINDOW
    let slot = U256::from(config.block_number % u64::try_from(HISTORY_SERVE_WINDOW)?);
    let value = U256::from_be_bytes(parent_hash.0);

    let mut storage = HashMap::new();
    storage.insert(keccak256(slot.to_be_bytes::<32>()), value);

    // Try to fetch existing account state from the previous block
    let (balance, nonce, code_hash, code) =
        fetch_existing_or_default(reader, &address_hash, config.block_number, || {
            (
                U256::ZERO,
                1,
                keccak256(&HISTORY_STORAGE_CODE),
                Some(HISTORY_STORAGE_CODE.clone()),
            )
        });

    Ok(Some(AccountState {
        address_hash,
        balance,
        nonce,
        code_hash,
        code,
        storage,
        deleted: false,
    }))
}

/// Compute EIP-4788 state changes (beacon block roots)
fn compute_eip4788_state(
    config: &SystemCallConfig,
    reader: Option<&StateReader>,
) -> Result<Option<AccountState>> {
    // Skip genesis block
    if config.block_number == 0 {
        if let Some(root) = config.parent_beacon_block_root
            && !root.is_zero()
        {
            bail!("genesis block cannot have non-zero parent beacon root");
        }
        return Ok(None);
    }

    let Some(beacon_root) = config.parent_beacon_block_root else {
        bail!(
            "missing parent beacon block root for EIP-4788 at block {}",
            config.block_number
        )
    };

    let address_hash = AddressHash::from(keccak256(BEACON_ROOTS_ADDRESS));

    // Dual ring buffer layout:
    // - Slot `timestamp % HISTORY_BUFFER_LENGTH`: timestamp
    // - Slot `timestamp % HISTORY_BUFFER_LENGTH + HISTORY_BUFFER_LENGTH`: beacon root
    let timestamp_index = config.timestamp % HISTORY_BUFFER_LENGTH;

    // Compute raw EVM slots (U256)
    let timestamp_slot = U256::from(timestamp_index);
    let root_slot = U256::from(timestamp_index + HISTORY_BUFFER_LENGTH);

    // Hash slots to get trie keys (B256)
    let timestamp_slot_key = keccak256(timestamp_slot.to_be_bytes::<32>());
    let root_slot_key = keccak256(root_slot.to_be_bytes::<32>());

    let mut storage = HashMap::new();
    storage.insert(timestamp_slot_key, U256::from(config.timestamp));
    storage.insert(root_slot_key, U256::from_be_bytes(beacon_root.0));

    // Try to fetch existing account state from the previous block
    let (balance, nonce, code_hash, code) =
        fetch_existing_or_default(reader, &address_hash, config.block_number, || {
            (
                U256::ZERO,
                1,
                keccak256(&BEACON_ROOTS_CODE),
                Some(BEACON_ROOTS_CODE.clone()),
            )
        });

    Ok(Some(AccountState {
        address_hash,
        balance,
        nonce,
        code_hash,
        code,
        storage,
        deleted: false,
    }))
}

/// Fetch existing account state from the reader, or return defaults if not present.
///
/// Reads from `block_number - 1` (the previous block's state).
fn fetch_existing_or_default<F>(
    reader: Option<&StateReader>,
    address_hash: &AddressHash,
    block_number: u64,
    defaults: F,
) -> (U256, u64, B256, Option<Bytes>)
where
    F: FnOnce() -> (U256, u64, B256, Option<Bytes>),
{
    let Some(reader) = reader else {
        return defaults();
    };

    // Read from previous block's state
    let prev_block = block_number.saturating_sub(1);

    match reader.get_full_account(*address_hash, prev_block) {
        Ok(Some(existing)) => {
            (
                existing.balance,
                existing.nonce,
                existing.code_hash,
                existing.code,
            )
        }
        Ok(None) | Err(_) => {
            // Account doesn't exist or read failed, use defaults
            defaults()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eip2935_computes_correct_slot() {
        let config = SystemCallConfig {
            block_number: 100,
            timestamp: 1_700_000_000,
            parent_block_hash: Some(B256::repeat_byte(0xab)),
            parent_beacon_block_root: Some(B256::repeat_byte(0xcd)),
        };

        // No reader provided, should use defaults
        let states = compute_system_call_states(&config, None).unwrap();

        let eip2935_state = states
            .iter()
            .find(|s| s.address_hash == AddressHash::from(keccak256(HISTORY_STORAGE_ADDRESS)))
            .expect("EIP-2935 state should exist");

        // Slot = 100 % 8191 = 100
        let raw_slot = U256::from(100);
        let expected_slot_key = keccak256(raw_slot.to_be_bytes::<32>());
        assert!(eip2935_state.storage.contains_key(&expected_slot_key));

        let stored_hash = eip2935_state.storage.get(&expected_slot_key).unwrap();
        assert_eq!(*stored_hash, U256::from_be_bytes(B256::repeat_byte(0xab).0));

        // Verify defaults are used
        assert_eq!(eip2935_state.balance, U256::ZERO);
        assert_eq!(eip2935_state.nonce, 1);
        assert_eq!(eip2935_state.code_hash, keccak256(&HISTORY_STORAGE_CODE));
    }

    #[test]
    fn test_eip4788_computes_dual_slots() {
        let config = SystemCallConfig {
            block_number: 100,
            timestamp: 1_700_000_000,
            parent_block_hash: Some(B256::repeat_byte(0xab)),
            parent_beacon_block_root: Some(B256::repeat_byte(0xcd)),
        };

        // No reader provided, should use defaults
        let states = compute_system_call_states(&config, None).unwrap();

        let eip4788_state = states
            .iter()
            .find(|s| s.address_hash == AddressHash::from(keccak256(BEACON_ROOTS_ADDRESS)))
            .expect("EIP-4788 state should exist");

        let timestamp_index = 1_700_000_000u64 % HISTORY_BUFFER_LENGTH;

        // Hash the raw slots to get storage keys
        let timestamp_slot_key = keccak256(U256::from(timestamp_index).to_be_bytes::<32>());
        let root_slot_key =
            keccak256(U256::from(timestamp_index + HISTORY_BUFFER_LENGTH).to_be_bytes::<32>());

        assert!(eip4788_state.storage.contains_key(&timestamp_slot_key));
        assert!(eip4788_state.storage.contains_key(&root_slot_key));

        assert_eq!(
            *eip4788_state.storage.get(&timestamp_slot_key).unwrap(),
            U256::from(1_700_000_000u64)
        );
        assert_eq!(
            *eip4788_state.storage.get(&root_slot_key).unwrap(),
            U256::from_be_bytes(B256::repeat_byte(0xcd).0)
        );

        // Verify defaults are used
        assert_eq!(eip4788_state.balance, U256::ZERO);
        assert_eq!(eip4788_state.nonce, 1);
        assert_eq!(eip4788_state.code_hash, keccak256(&BEACON_ROOTS_CODE));
    }

    #[test]
    fn test_genesis_block_skipped() {
        let config = SystemCallConfig {
            block_number: 0,
            timestamp: 0,
            parent_block_hash: Some(B256::ZERO),
            parent_beacon_block_root: Some(B256::ZERO),
        };

        let states = compute_system_call_states(&config, None).unwrap();
        assert!(states.is_empty());
    }

    #[test]
    fn test_missing_parent_hash_errors() {
        let config = SystemCallConfig {
            block_number: 100,
            timestamp: 1_700_000_000,
            parent_block_hash: None,
            parent_beacon_block_root: Some(B256::repeat_byte(0xcd)),
        };

        let result = compute_system_call_states(&config, None);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("missing parent block hash")
        );
    }

    #[test]
    fn test_missing_beacon_root_errors() {
        let config = SystemCallConfig {
            block_number: 100,
            timestamp: 1_700_000_000,
            parent_block_hash: Some(B256::repeat_byte(0xab)),
            parent_beacon_block_root: None,
        };

        let result = compute_system_call_states(&config, None);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("missing parent beacon block root")
        );
    }

    #[test]
    fn test_ring_buffer_wraparound() {
        let config = SystemCallConfig {
            block_number: HISTORY_SERVE_WINDOW as u64 + 99, // Wraps to slot 99
            timestamp: 1_700_000_000,
            parent_block_hash: Some(B256::repeat_byte(0xff)),
            parent_beacon_block_root: Some(B256::repeat_byte(0xee)),
        };

        let states = compute_system_call_states(&config, None).unwrap();

        let eip2935_state = states
            .iter()
            .find(|s| s.address_hash == AddressHash::from(keccak256(HISTORY_STORAGE_ADDRESS)))
            .unwrap();

        // (8191 + 99) % 8191 = 99

        let raw_slot = U256::from(99);
        let expected_slot_key = keccak256(raw_slot.to_be_bytes::<32>());
        assert!(eip2935_state.storage.contains_key(&expected_slot_key));
    }
}
