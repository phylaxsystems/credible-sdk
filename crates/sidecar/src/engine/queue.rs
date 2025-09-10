//! # Transaction queue
//!
//! The transaction queue is how transactions get internally gossiped from the transport
//! to the core engine. The queue is an unbounded crossbeam channel that we sequentially
//! take transactions from and execute inside the core engine. The flow from the
//! transport to the core engine looks like this:
//! 1. Received by transport from external driver
//! 2. Processed inside of transport and converted to `TxQueueContents`
//! 3. Sent via the `TransactionQueueSender` to the engine
//! 4. Received by engine and executed.
//!
//! ## Channel contents
//!
//! Channel contents can either be:
//! - New blocks,
//! - New transactions,
//! - Reorg events.
//!
//! New block events contain `BlockEnv`s of the next block the sidecar should build on top of.
//! At least one new block event is needed for the sidecar to accept transactions.
//!
//! New transaction events are transactions that should be executed and included for the
//! current `BlockEnv`.
//!
//! Reorg events are signals from the driver to the engine that it should discard the last
//! executed transaction.

use alloy::primitives::TxHash;
use crossbeam::channel::{
    Receiver,
    Sender,
};
use revm::{
    context::{
        BlockEnv,
        TxEnv,
    },
    primitives::B256,
};
use serde::{
    Deserialize,
    Deserializer,
    Serialize,
    Serializer,
};
use serde_json::Value;

/// Represents a transaction to be processed by the sidecar engine.
///
/// This struct encapsulates both the transaction hash for identification/tracing
/// and the transaction environment containing the actual transaction data needed
/// for EVM execution. The hash enables transaction tracking throughout the
/// execution pipeline, while the `TxEnv` provides the EVM with all necessary
/// transaction context (sender, recipient, value, data, gas, etc.).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueTransaction {
    pub tx_hash: B256,
    pub tx_env: TxEnv,
}

/// `QueueBlockEnv` encapsulates block environment data, the hash of the last transaction in the
/// queue (if any), and the total number of transactions in the queue.
///
/// # Fields
///
/// * `block_env` - A `BlockEnv` struct that contains the flattened block environment details required for the queue.
/// * `last_tx_hash` - An `Option` containing the hash of the last transaction in the queue, or `None` if no transactions exist.
/// * `n_transactions` - A `u64` value indicating the total number of transactions queued.
#[derive(Clone, Debug, Default)]
pub struct QueueBlockEnv {
    pub block_env: BlockEnv,
    pub last_tx_hash: Option<TxHash>,
    pub n_transactions: u64,
}

// Implementing custo (de)serialization for QueueBlockEnv so we can add new fields without breaking
// backwards compatibility. We cannot use `#[serde(flatten)]` because it does not support custom
// fields (https://github.com/serde-rs/serde/issues/1183) and it breaks the custom deserialization
// for `u128`.

impl<'de> Deserialize<'de> for QueueBlockEnv {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;

        // Check if this looks like the new format (has our additional fields)
        if let Value::Object(ref map) = value
            && (map.contains_key("last_tx_hash") || map.contains_key("n_transactions"))
        {
            // New format with additional fields - deserialize them manually
            let last_tx_hash = match map.get("last_tx_hash") {
                Some(Value::Null) | None => None, // Handle explicit null and missing fields
                Some(Value::String(s)) if s.is_empty() => None, // Handle empty string
                Some(v) => {
                    Some(serde_json::from_value(v.clone()).map_err(serde::de::Error::custom)?)
                }
            };

            let n_transactions = match map.get("n_transactions") {
                Some(Value::Null) | None => 0, // Treat null as default value and missing fields as 0
                Some(v) => serde_json::from_value(v.clone()).map_err(serde::de::Error::custom)?,
            };

            // Validate sequencer requirements:
            // N > 0 + must contain last tx hash
            // N = 0, must contain txhash = [empty, "", null] or missing
            match n_transactions {
                0 => {
                    // When n_transactions is 0, last_tx_hash must be None, empty string, null, or missing
                    if last_tx_hash.is_some() {
                        return Err(serde::de::Error::custom(
                            "When n_transactions is 0, last_tx_hash must be null, empty, or missing",
                        ));
                    }
                }
                _ => {
                    // When n_transactions > 0, last_tx_hash must be present and valid
                    if last_tx_hash.is_none() {
                        return Err(serde::de::Error::custom(
                            "When n_transactions > 0, last_tx_hash must be provided and non-empty",
                        ));
                    }
                }
            }
            // Create a value without our custom fields for BlockEnv deserialization
            let mut block_env_map = map.clone();
            block_env_map.remove("last_tx_hash");
            block_env_map.remove("n_transactions");

            let block_env = serde_json::from_value(Value::Object(block_env_map))
                .map_err(serde::de::Error::custom)?;

            return Ok(QueueBlockEnv {
                block_env,
                last_tx_hash,
                n_transactions,
            });
        }

        // Old format - just deserialize as BlockEnv
        let block_env = serde_json::from_value(value).map_err(serde::de::Error::custom)?;

        Ok(QueueBlockEnv {
            block_env,
            last_tx_hash: None,
            n_transactions: 0,
        })
    }
}

impl Serialize for QueueBlockEnv {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;

        // First, serialize the BlockEnv to get all its fields
        let block_env_value =
            serde_json::to_value(&self.block_env).map_err(serde::ser::Error::custom)?;

        // Count the fields we need to serialize
        let mut field_count = 0;
        if let Value::Object(ref map) = block_env_value {
            field_count += map.len();
        }

        // Add our additional fields if they have non-default values
        if self.last_tx_hash.is_some() {
            field_count += 1;
        }
        if self.n_transactions != 0 {
            field_count += 1;
        }

        // Create a map serializer
        let mut map = serializer.serialize_map(Some(field_count))?;

        // Serialize all BlockEnv fields
        if let Value::Object(block_env_map) = block_env_value {
            for (key, value) in block_env_map {
                map.serialize_entry(&key, &value)?;
            }
        }

        // Serialize our additional fields only if they have non-default values
        if self.last_tx_hash.is_some() {
            map.serialize_entry("last_tx_hash", &self.last_tx_hash)?;
        }

        if self.n_transactions != 0 {
            map.serialize_entry("n_transactions", &self.n_transactions)?;
        }

        map.end()
    }
}

/// Contains the two possible types that can be sent in the transaction queue.
/// `Block` is a new block being processed, while `Tx` is a new transaction.
///
/// `Block` should be used to mark the end of the current block and the start
/// of a new one.
///
/// `Tx` should be used to append a new transaction to the current block,
/// along with its transaction hash for identification and tracing.
///
/// `Reorg` should be used to signal to remove the last executed transaction.
/// To verify the transaction is indeed the correct one, we include a tx hash
/// and should only process it as a valid event if the hashes match.
#[derive(Debug)]
pub enum TxQueueContents {
    Block(QueueBlockEnv, tracing::Span),
    Tx(QueueTransaction, tracing::Span),
    Reorg(B256, tracing::Span),
}

/// `crossbeam` sender for the transaction queue. Sends data to tx queue.
pub type TransactionQueueSender = Sender<TxQueueContents>;
/// `crossbeam` receiver for the transaction queue. Receives data from tx queue.
pub type TransactionQueueReceiver = Receiver<TxQueueContents>;
