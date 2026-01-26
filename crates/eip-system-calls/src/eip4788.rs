//! EIP-4788: Beacon Block Root in the EVM, from Cancun fork
//! 
//! See also https://eips.ethereum.org/EIPS/eip-4788.

use super::{SystemContract, b256_to_storage};
use alloy_primitives::{Address, B256, Bytes, U256};
pub use alloy_eips::eip4788::{BEACON_ROOTS_ADDRESS, BEACON_ROOTS_CODE};

pub const HISTORY_BUFFER_LENGTH: u64 = 8191;

/// EIP-4788 system contract marker type.
pub struct Eip4788;

impl SystemContract for Eip4788 {
    const ADDRESS: Address = BEACON_ROOTS_ADDRESS;
    const RING_BUFFER_SIZE: u64 = HISTORY_BUFFER_LENGTH;
    
    fn bytecode() -> &'static Bytes {
        &BEACON_ROOTS_CODE
    }
}

impl Eip4788 {
    /// Timestamp slot (first ring buffer).
    #[inline]
    pub fn timestamp_slot(timestamp: u64) -> U256 {
        U256::from(timestamp % Self::RING_BUFFER_SIZE)
    }
    
    /// Root slot (second ring buffer, offset by buffer size).
    #[inline]
    pub fn root_slot(timestamp: u64) -> U256 {
        U256::from((timestamp % Self::RING_BUFFER_SIZE) + Self::RING_BUFFER_SIZE)
    }
    
    /// U256 variants for when you already have U256.
    #[inline]
    pub fn timestamp_slot_u256(timestamp: U256) -> U256 {
        timestamp % U256::from(Self::RING_BUFFER_SIZE)
    }
    
    #[inline]
    pub fn root_slot_u256(timestamp: U256) -> U256 {
        Self::timestamp_slot_u256(timestamp) + U256::from(Self::RING_BUFFER_SIZE)
    }
    
    /// Convert beacon root to storage value.
    #[inline]
    pub fn root_to_value(root: B256) -> U256 {
        b256_to_storage(root)
    }
}