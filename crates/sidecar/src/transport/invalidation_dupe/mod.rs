//! Known-invalidating transaction content-hash filtering.
//!
//! Rejects transactions that are known to invalidate assertions based on their
//! content (ignoring nonce, gas_price, and gas_priority_fee).
//!
//! ## Deduplication Strategy
//!
//! We calculate a content hash of transactions using:
//! - tx_type
//! - caller
//! - gas_limit
//! - kind (Create or Call)
//! - value
//! - data
//! - chain_id
//! - access_list
//! - blob_hashes
//! - max_fee_per_blob_gas
//! - authorization_list
//!
//! Incremented nonces and gas price changes do not affect the hash.
//! For assertion execution, these parameters are hardcoded in the `TxEnv`
//! and we never pass the original account nonce/gas parameters to assex.
//!
//! The engine records a hash only when it observes an assertion invalidation.
//! The transport checks this cache to drop repeats early, reducing repeated
//! assertion execution noise for known-bad transactions.

mod cache;
mod content_hash;

pub use cache::ContentHashCache;
pub use content_hash::tx_content_hash;
