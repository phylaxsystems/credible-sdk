//! EIP-2935: Historical Block Hashes in State, since Prague fork
//!
//! Stores the last 8191 block hashes in a ring buffer at a system contract,
//! enabling smart contracts to access historical block hashes beyond the
//! 256-block limit of the `BLOCKHASH` opcode.
//!
//! ## Storage Layout
//!
//! - Slot `block_number % 8191`: stores the block hash
//!
//! When block 8192 is processed, it overwrites slot 1 (since `8192 % 8191 = 1`).
//!
//! ## Contract
//!
//! - Address: `0x0000F90827F1C53a10cb7A02335B175320002935`
//! - Ring buffer size: 8191 slots
//!
//! See also <https://eips.ethereum.org/EIPS/eip-2935>.

use super::{
    SystemContract,
    b256_to_storage,
};
pub use alloy_eips::eip2935::{
    HISTORY_SERVE_WINDOW,
    HISTORY_STORAGE_ADDRESS,
    HISTORY_STORAGE_CODE,
};
use alloy_primitives::{
    Address,
    B256,
    Bytes,
    U256,
};

/// EIP-2935 system contract marker type.
pub struct Eip2935;

impl SystemContract for Eip2935 {
    const ADDRESS: Address = HISTORY_STORAGE_ADDRESS;
    const RING_BUFFER_SIZE: u64 = HISTORY_SERVE_WINDOW as u64;

    fn bytecode() -> &'static Bytes {
        &HISTORY_STORAGE_CODE
    }
}

impl Eip2935 {
    /// Calculate the storage slot for a block number.
    #[inline]
    #[must_use]
    pub fn slot(block_number: u64) -> U256 {
        U256::from(block_number % Self::RING_BUFFER_SIZE)
    }

    /// Calculate the storage slot from U256.
    #[inline]
    #[must_use]
    pub fn slot_u256(block_number: U256) -> U256 {
        block_number % U256::from(Self::RING_BUFFER_SIZE)
    }

    /// Convert block hash to storage value.
    #[inline]
    #[must_use]
    pub fn hash_to_value(hash: B256) -> U256 {
        b256_to_storage(hash)
    }
}
