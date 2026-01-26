//! EIP-2935: Historical Block Hashes in State, since Prague fork
//! 
//! See also https://eips.ethereum.org/EIPS/eip-2935.

use super::{SystemContract, b256_to_storage};
use alloy_primitives::{Address, B256, Bytes, U256};
pub use alloy_eips::eip2935::{HISTORY_SERVE_WINDOW, HISTORY_STORAGE_ADDRESS, HISTORY_STORAGE_CODE};

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
    pub fn slot(block_number: u64) -> U256 {
        U256::from(block_number % Self::RING_BUFFER_SIZE)
    }
    
    /// Calculate the storage slot from U256.
    #[inline]
    pub fn slot_u256(block_number: U256) -> U256 {
        block_number % U256::from(Self::RING_BUFFER_SIZE)
    }
    
    /// Convert block hash to storage value.
    #[inline]
    pub fn hash_to_value(hash: B256) -> U256 {
        b256_to_storage(hash)
    }
}