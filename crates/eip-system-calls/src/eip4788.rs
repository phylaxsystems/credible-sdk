//! EIP-4788: Beacon Block Root in the EVM, from Cancun fork
//!
//! Stores beacon chain block roots for trust-minimized consensus layer access.
//! - Contract: `0x000F3df6D732807Ef1319fB7B8bB8522d0Beac02`
//!
//! ## Storage Layout (Dual Ring Buffer)
//!
//! - Slot `timestamp % HISTORY_BUFFER_LENGTH`: stores the timestamp (for verification)
//! - Slot `timestamp % HISTORY_BUFFER_LENGTH + HISTORY_BUFFER_LENGTH`: stores the beacon root
//!
//! See also <https://eips.ethereum.org/EIPS/eip-4788>

use super::{
    SystemContract,
    b256_to_storage,
};
pub use alloy_eips::eip4788::{
    BEACON_ROOTS_ADDRESS,
    BEACON_ROOTS_CODE,
};
use alloy_primitives::{
    Address,
    B256,
    Bytes,
    U256,
};

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
    #[must_use]
    pub fn timestamp_slot(timestamp: u64) -> U256 {
        U256::from(timestamp % Self::RING_BUFFER_SIZE)
    }

    /// Root slot (second ring buffer, offset by buffer size).
    #[inline]
    #[must_use]
    pub fn root_slot(timestamp: u64) -> U256 {
        U256::from((timestamp % Self::RING_BUFFER_SIZE) + Self::RING_BUFFER_SIZE)
    }

    /// U256 variants for when you already have U256.
    #[inline]
    #[must_use]
    pub fn timestamp_slot_u256(timestamp: U256) -> U256 {
        timestamp % U256::from(Self::RING_BUFFER_SIZE)
    }

    #[inline]
    #[must_use]
    pub fn root_slot_u256(timestamp: U256) -> U256 {
        Self::timestamp_slot_u256(timestamp) + U256::from(Self::RING_BUFFER_SIZE)
    }

    /// Convert beacon root to storage value.
    #[inline]
    #[must_use]
    pub fn root_to_value(root: B256) -> U256 {
        b256_to_storage(root)
    }
}
