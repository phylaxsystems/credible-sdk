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
//! # Data Model
//!
//! For each namespace, the following keys are stored:
//! - `{namespace}:block` - Current block number in this namespace
//! - `{namespace}:account:{address}` - Hash containing balance, nonce, code hash
//! - `{namespace}:storage:{address}` - Hash of storage slots for the account
//! - `{namespace}:code:{code_hash}` - Contract bytecode
//!
//! Shared across namespaces:
//! - `{base_namespace}:block_hash:{block_number}` - Block hash for each block
//! - `{base_namespace}:state_root:{block_number}` - State root for each block
//! - `{base_namespace}:diff:{block_number}` - Serialized state diff (kept for `buffer_size` blocks)
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
