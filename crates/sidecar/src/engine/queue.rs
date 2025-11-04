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

use crate::execution_ids::{
    BlockExecutionId,
    TxExecutionId,
};
use alloy::primitives::TxHash;
use crossbeam::channel::{
    Receiver,
    Sender,
};
use revm::context::{
    BlockEnv,
    TxEnv,
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
/// This struct encapsulates both the transaction execution identifier for identification/tracing
/// and the transaction environment containing the actual transaction data needed
/// for EVM execution. The hash enables transaction tracking throughout the
/// execution pipeline, while the `TxEnv` provides the EVM with all necessary
/// transaction context (sender, recipient, value, data, gas, etc.).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueTransaction {
    pub tx_execution_id: TxExecutionId,
    pub tx_env: TxEnv,
}

/// `QueueIteration` encapsulates the iteration block environment data and iteration identifier.
///
/// # Fields
///
/// * `block_env` - A `BlockEnv` struct that contains the block environment details required for the queue.
/// * `iteration_id` - A `u64` value indicating the iteration identifier.
#[derive(Clone, Debug, Default)]
pub struct QueueIteration {
    pub block_env: BlockEnv,
    pub iteration_id: u64,
}

impl From<&QueueIteration> for BlockExecutionId {
    fn from(iteration: &QueueIteration) -> Self {
        BlockExecutionId {
            block_number: iteration.block_env.number,
            iteration_id: iteration.iteration_id,
        }
    }
}

/// `QueueCommitHead` encapsulates the information needed to commit an iteration head
///
/// # Fields
///
/// * `iteration_id` (`u64`) - An identifier containing the selected iteration ID to be committed.
/// * `block_number` (`u64`) - The block number of the selected iteration.
/// * `last_tx_hash` (`Option<TxHash>`) - An optional field that stores the hash of the last processed transaction. If there are no transactions processed yet, this value will be `None`.
/// * `n_transactions` (`u64`) - The total number of transactions that have been processed up to this point in the selected iteration.
#[derive(Clone, Debug, Default)]
pub struct QueueCommitHead {
    pub block_number: u64,
    pub iteration_id: u64,
    pub last_tx_hash: Option<TxHash>,
    pub n_transactions: u64,
}

impl From<&QueueCommitHead> for BlockExecutionId {
    fn from(commit_head: &QueueCommitHead) -> Self {
        BlockExecutionId {
            block_number: commit_head.block_number,
            iteration_id: commit_head.iteration_id,
        }
    }
}

// We cannot use `#[serde(flatten)]` because it does not support custom
// fields (https://github.com/serde-rs/serde/issues/1183) and it breaks the custom deserialization
// for `u128`.

impl Serialize for QueueIteration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;

        let block_env_value = serde_json::to_value(&self.block_env).map_err(|e| {
            serde::ser::Error::custom(format!("failed to serialize block_env: {e}"))
        })?;

        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("block_env", &block_env_value)?;
        map.serialize_entry("iteration_id", &self.iteration_id)?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for QueueIteration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;

        let Value::Object(ref map) = value else {
            return Err(serde::de::Error::custom("expected object"));
        };

        // Extract block_env - must be present
        let block_env_value = map
            .get("block_env")
            .ok_or_else(|| serde::de::Error::custom("missing field: block_env"))?;

        let iteration_id = match map.get("iteration_id") {
            Some(Value::Null) | None => 0,
            Some(v) => {
                serde_json::from_value(v.clone())
                    .map_err(|e| serde::de::Error::custom(format!("invalid iteration_id: {e}")))?
            }
        };

        let block_env = serde_json::from_value(block_env_value.clone()).map_err(|e| {
            let msg = e.to_string();

            // Be very specific with field name matching - check for backticks or exact field references
            // Serde typically formats errors as: missing field `fieldname` or invalid value for `fieldname`
            let custom_msg = if msg.contains("missing field `number`")
                || msg.contains("missing field \"number\"")
            {
                "missing field: number"
            } else if msg.contains("missing field `beneficiary`")
                || msg.contains("missing field \"beneficiary\"")
            {
                "missing field: beneficiary"
            } else if msg.contains("missing field `timestamp`")
                || msg.contains("missing field \"timestamp\"")
            {
                "missing field: timestamp"
            } else if msg.contains("missing field `gas_limit`")
                || msg.contains("missing field \"gas_limit\"")
            {
                "missing field: gas_limit"
            } else if msg.contains("missing field `basefee`")
                || msg.contains("missing field \"basefee\"")
            {
                "missing field: basefee"
            } else if msg.contains("missing field `difficulty`")
                || msg.contains("missing field \"difficulty\"")
            {
                "missing field: difficulty"
            } else if msg.contains("`number`") && !msg.contains("missing field") {
                "invalid block number"
            } else if msg.contains("`beneficiary`") && !msg.contains("missing field") {
                "invalid beneficiary address"
            } else if msg.contains("`timestamp`") && !msg.contains("missing field") {
                "invalid timestamp"
            } else if msg.contains("`gas_limit`") && !msg.contains("missing field") {
                "invalid gas_limit"
            } else if msg.contains("`basefee`") && !msg.contains("missing field") {
                "invalid basefee"
            } else if msg.contains("`difficulty`") && !msg.contains("missing field") {
                "invalid difficulty"
            } else if msg.contains("`prevrandao`") {
                "invalid prevrandao"
            } else if msg.contains("blob_excess_gas_and_price") {
                "invalid blob_excess_gas_and_price"
            } else {
                &msg
            };

            serde::de::Error::custom(custom_msg)
        })?;

        Ok(QueueIteration {
            block_env,
            iteration_id,
        })
    }
}

/// Contains the two possible types that can be sent in the transaction queue.
/// `Block` is a new block being processed, while `Tx` is a new transaction.
///
/// `Block` should be used to mark the end of the current block and the start
/// of a new one.
///
/// `Tx` should be used to append a new transaction to the current block,
/// along with its transaction execution id for identification and tracing.
///
/// `Reorg` should be used to signal to remove the last executed transaction.
/// To verify the transaction is indeed the correct one, we include the
/// transaction execution id and should only process it as a valid event if it matches.
#[derive(Debug)]
pub enum TxQueueContents {
    CommitHead(QueueCommitHead, tracing::Span),
    Iteration(QueueIteration, tracing::Span),
    Tx(QueueTransaction, tracing::Span),
    Reorg(TxExecutionId, tracing::Span),
}

/// `crossbeam` sender for the transaction queue. Sends data to tx queue.
pub type TransactionQueueSender = Sender<TxQueueContents>;
/// `crossbeam` receiver for the transaction queue. Receives data from tx queue.
pub type TransactionQueueReceiver = Receiver<TxQueueContents>;
