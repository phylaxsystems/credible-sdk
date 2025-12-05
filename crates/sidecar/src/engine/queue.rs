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
//! - `CommitHead` events,
//! - `NewIteration` events,
//! - New transactions,
//! - Reorg events.
//!
//! `CommitHead` events advance the canonical head to a finalized iteration selected by the driver.
//! `NewIteration` events contain `BlockEnv`s for the next block the sidecar should build on top of.
//! At least one `NewIteration` event is needed for the sidecar to accept transactions.
//!
//! New transaction events are transactions that should be executed and included for the
//! current `BlockEnv`.
//!
//! Reorg events are signals from the driver to the engine that it should discard the last
//! executed transaction.

use crate::execution_ids::TxExecutionId;
use alloy::primitives::{
    TxHash,
    U256,
};
use flume::{
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
    Serialize,
};

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
    pub prev_tx_hash: Option<TxHash>,
}

/// Contains the possible types that can be sent in the transaction queue.
///
/// `CommitHead` advances the canonical chain head.
/// `NewIteration` seeds the engine with block context for building the next block.
///
/// `Tx` should be used to append a new transaction to the current block,
/// along with its transaction execution id for identification and tracing.
///
/// `Reorg` should be used to signal to remove the last executed transaction.
/// To verify the transaction is indeed the correct one, we include the
/// transaction execution id and should only process it as a valid event if it matches.
#[derive(Debug, Clone)]
pub enum TxQueueContents {
    Tx(QueueTransaction, tracing::Span),
    Reorg(TxExecutionId, tracing::Span),
    CommitHead(CommitHead, tracing::Span),
    NewIteration(NewIteration, tracing::Span),
}

impl TxQueueContents {
    pub fn block_number(&self) -> U256 {
        match self {
            TxQueueContents::Tx(v, _) => v.tx_execution_id.block_number,
            TxQueueContents::Reorg(v, _) => v.block_number,
            TxQueueContents::CommitHead(v, _) => v.block_number,
            TxQueueContents::NewIteration(v, _) => v.block_env.number,
        }
    }

    pub fn iteration_id(&self) -> u64 {
        match self {
            TxQueueContents::Tx(v, _) => v.tx_execution_id.iteration_id,
            TxQueueContents::Reorg(v, _) => v.iteration_id,
            TxQueueContents::CommitHead(v, _) => v.selected_iteration_id,
            TxQueueContents::NewIteration(v, _) => v.iteration_id,
        }
    }
}

/// Event used to commit a built iteration as the head block of the chain.
///
/// Includes `last_tx_hash` and `n_transactions` to ensure consistency.
/// `CommitHead` *MUST* select an iteration to apply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitHead {
    /// Block number of the selected iteration.
    pub(crate) block_number: U256,
    /// Identifier of the selected iteration. Selected iteration will be
    /// applied as the head block.
    pub(crate) selected_iteration_id: u64,
    /// Last included tx hash, can be optional if the block is empty.
    pub(crate) last_tx_hash: Option<TxHash>,
    /// Number of txs included in the block.
    pub(crate) n_transactions: u64,
    /// Block hash for EIP-2935 (Prague+)
    /// Required for historical block hash storage
    pub(crate) block_hash: Option<B256>,
    /// Beacon block root for EIP-4788 (Cancun+)
    /// Required for beacon chain root storage
    pub(crate) beacon_block_root: Option<B256>,
    /// Timestamp of the block.
    pub(crate) timestamp: U256,
}

impl CommitHead {
    /// Construct a new commit head event.
    pub fn new(
        block_number: U256,
        selected_iteration_id: u64,
        last_tx_hash: Option<TxHash>,
        n_transactions: u64,
        block_hash: Option<B256>,
        beacon_block_root: Option<B256>,
        timestamp: U256,
    ) -> Self {
        Self {
            block_number,
            selected_iteration_id,
            last_tx_hash,
            n_transactions,
            block_hash,
            beacon_block_root,
            timestamp,
        }
    }

    /// Accessor for the selected iteration identifier.
    pub fn iteration_id(&self) -> u64 {
        self.selected_iteration_id
    }
}

/// Creates a new iteration with a specific block env
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewIteration {
    pub(crate) iteration_id: u64,
    pub(crate) block_env: BlockEnv,
}

impl NewIteration {
    /// Construct a new iteration event.
    pub fn new(iteration_id: u64, block_env: BlockEnv) -> Self {
        Self {
            iteration_id,
            block_env,
        }
    }
}

/// `crossbeam` sender for the transaction queue. Sends data to tx queue.
pub type TransactionQueueSender = Sender<TxQueueContents>;
/// `crossbeam` receiver for the transaction queue. Receives data from tx queue.
pub type TransactionQueueReceiver = Receiver<TxQueueContents>;
