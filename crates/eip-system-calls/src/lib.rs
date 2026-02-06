//! Shared EIP-2935 and EIP-4788 system call logic.
//!
//! This crate provides common constants, slot calculations, and default account
//! information for the EIP system contracts used by both the sidecar and state-worker.

//! Common abstractions for EIP system contracts.

use alloy_primitives::{
    Address,
    B256,
    Bytes,
    U256,
    keccak256,
};

pub mod eip2935;
pub mod eip4788;

/// Trait for system contracts that use ring buffer storage.
pub trait SystemContract {
    /// The contract address.
    const ADDRESS: Address;

    /// The ring buffer size.
    const RING_BUFFER_SIZE: u64;

    /// The contract bytecode.
    fn bytecode() -> &'static Bytes;

    /// Compute the code hash (can be cached via `LazyLock` if needed).
    #[must_use]
    fn code_hash() -> B256 {
        keccak256(Self::bytecode())
    }
}

/// Convert a 32-byte value to storage format.
#[inline]
#[must_use]
pub fn b256_to_storage(value: B256) -> U256 {
    U256::from_be_bytes(value.0)
}
