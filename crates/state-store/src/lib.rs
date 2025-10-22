//! Lightweight blocking Redis client with circular buffer support for multiple blockchain states.
//!
//! # Overview
//!
//! This module provides a Redis-based storage system for maintaining multiple historical blockchain
//! states using a circular buffer pattern.
//!
//! # Architecture
//!
//! ## Circular Buffer Pattern
//!
//! States are distributed across N namespaces (where N = `buffer_size`). Each block is assigned to
//! a namespace using modulo arithmetic: `namespace_idx = block_number % buffer_size`.
//!
//! Example with `buffer_size = 3`:
//! - Block 0 → namespace:0
//! - Block 1 → namespace:1
//! - Block 2 → namespace:2
//! - Block 3 → namespace:0 (overwrites block 0)
//! - Block 4 → namespace:1 (overwrites block 1)
//!
//! ## Cumulative State Maintenance
//!
//! The critical feature of this system is that when a namespace is overwritten, it maintains the
//! cumulative state by applying all intermediate state diffs. This ensures each namespace
//! contains the complete state up to its current block, not just the delta.
//!
//! Example: When block 3 overwrites the namespace:0 (previously containing block 0):
//! 1. Load diffs for blocks 1 and 2 from Redis
//! 2. Apply diff 1 to the namespace (updates from block 0→1)
//! 3. Apply diff 2 to the namespace (updates from block 1→2)
//! 4. Apply diff 3 to the namespace (updates from block 2→3)
//! 5. Result: the namespace:0 now contains a complete state at block 3
//!
//! ## State Diff Storage
//!
//! To enable cumulative state reconstruction, each block's state changes are stored as a separate
//! diff in Redis with key `{base_namespace}:diff:{block_number}`. These diffs are automatically
//! cleaned up after `buffer_size` blocks to prevent unbounded growth.
//!
//! # Redis Schema
//!
//! ## Key Structure Overview
//!
//! The system uses a hierarchical key structure with three main categories:
//!
//! ### 1. Namespace-Scoped Keys (Rotated per Circular Buffer)
//!
//! These keys live within individual namespaces and get overwritten when the circular buffer wraps:
//!
//! For each namespace, the following keys are stored:
//! - `{base_namespace}:{namespace_idx}:block` - Current block number in this namespace
//! - `{base_namespace}:{namespace_idx}:account:{address_hash}` - Hash containing balance, nonce, code hash
//! - `{base_namespace}:{namespace_idx}:storage:{address_hash}` - Hash of storage slots for the account
//! - `{base_namespace}:{namespace_idx}:code:{code_hash}` - Contract bytecode indexed by code hash
//!
//! Shared across namespaces:
//! - `{base_namespace}:meta:latest_block` - The most recent block number written to the system
//! - `{base_namespace}:block_hash:{block_number}` - Block hash for each block (kept for last `buffer_size` blocks)
//! - `{base_namespace}:state_root:{block_number}` - State root for each block (kept for last `buffer_size` blocks)
//! - `{base_namespace}:diff:{block_number}` - Serialized state diff (kept for last `buffer_size` blocks)
//!
//! ## Complete Schema Example
//!
//! For a system with `base_namespace="chain"` and `buffer_size=3` at block 1005:
//!
//! ```text
//! Namespace Keys (Circular Buffer):
//!   chain:0:block = "1005"                      # Block 1005 % 3 = 0
//!   chain:0:account:abc123... = {balance, nonce, code_hash}
//!   chain:0:storage:abc123... = {slot1: value1, slot2: value2}
//!   chain:0:code:def456... = "0x6080..."
//!
//!   chain:1:block = "1003"                      # Block 1003 % 3 = 1
//!   chain:1:account:xyz789... = {...}
//!   ...
//!
//!   chain:2:block = "1004"                      # Block 1004 % 3 = 2
//!   chain:2:account:...
//!   ...
//!
//! Global Metadata:
//!   chain:meta:latest_block = "1005"
//!
//! Recent Block Metadata (last 3 blocks):
//!   chain:block_hash:1003 = "0xaaa..."
//!   chain:block_hash:1004 = "0xbbb..."
//!   chain:block_hash:1005 = "0xccc..."
//!
//!   chain:state_root:1003 = "0x111..."
//!   chain:state_root:1004 = "0x222..."
//!   chain:state_root:1005 = "0x333..."
//!
//! Recent State Diffs (last 3 blocks):
//!   chain:diff:1003 = "{block_number: 1003, accounts: [...]}"
//!   chain:diff:1004 = "{block_number: 1004, accounts: [...]}"
//!   chain:diff:1005 = "{block_number: 1005, accounts: [...]}"
//! ```
//!
//! ## Address Hashing
//!
//! Account and storage keys use `keccak256(address)` to create the `{address_hash}` portion.
//! This provides a consistent 32-byte identifier regardless of the address format.
//!
//! ## Features
//!
//! - `writer` - Enable state writing functionality
//! - `reader` - Enable state reading functionality
//!
//! ## Example
//!
//! ```ignore
//! use state_store::{
//!     CircularBufferConfig,
//!     StateReader,
//!     StateWriter,
//! };
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = CircularBufferConfig::new(3)?;
//!
//!     // Writer (requires "writer" feature)
//!     #[cfg(feature = "writer")]
//!     {
//!         let writer = StateWriter::new(
//!             "redis://localhost:6379",
//!             "blockchain".to_string(),
//!             config.clone(),
//!         )?;
//!     }
//!
//!     // Reader (requires "reader" feature)
//!     #[cfg(feature = "reader")]
//!     {
//!         let reader =
//!             StateReader::new("redis://localhost:6379", "blockchain".to_string(), config)?;
//!     }
//!
//!     Ok(())
//! }
//! ```

#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]

pub mod common;

#[cfg(feature = "writer")]
pub mod writer;

#[cfg(feature = "reader")]
pub mod reader;
#[cfg(test)]
mod tests;

// Re-export common types at the root
pub use common::CircularBufferConfig;

#[cfg(feature = "writer")]
pub use writer::StateWriter;

#[cfg(feature = "reader")]
pub use reader::StateReader;
