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
//! executed transaction(s).

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

/// Reorg request to remove the specified transactions from an iteration.
///
/// The `tx_execution_id` identifies the LAST (newest) transaction being reorged.
/// The `tx_hashes` list specifies which transactions to remove, in chronological
/// order (oldest first). The depth is implicitly `tx_hashes.len()`.
///
/// For a reorg removing N transactions at index I, transactions from index
/// `(I - N + 1)` to `I` inclusive are removed.
///
/// ## Validation
/// Both the event sequencing layer and engine validate:
/// 1. `tx_hashes` is non-empty
/// 2. `tx_hashes.last() == tx_execution_id.tx_hash`
/// 3. `tx_hashes` match the tail of executed/sent transactions
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReorgRequest {
    /// Identifies the last (newest) transaction being reorged.
    pub tx_execution_id: TxExecutionId,
    /// Transaction hashes being reorged, in chronological order (oldest first).
    /// Must be non-empty. The length determines the reorg depth.
    pub tx_hashes: Vec<TxHash>,
}

impl ReorgRequest {
    /// Returns the depth of the reorg (number of transactions to remove).
    #[inline]
    pub fn depth(&self) -> usize {
        self.tx_hashes.len()
    }
}

/// Contains the possible types that can be sent in the transaction queue.
///
/// `CommitHead` advances the canonical chain head.
/// `NewIteration` seeds the engine with block context for building the next block.
///
/// `Tx` should be used to append a new transaction to the current block,
/// along with its transaction execution id for identification and tracing.
///
/// `Reorg` should be used to signal to remove the last executed transaction(s).
/// To verify the transaction is indeed the correct one, we include the
/// transaction execution id plus the last `depth` tx hashes and should only
/// process it as a valid event if it matches.
#[derive(Debug, Clone)]
pub enum TxQueueContents {
    Tx(QueueTransaction),
    Reorg(ReorgRequest),
    CommitHead(CommitHead),
    NewIteration(NewIteration),
}

impl TxQueueContents {
    pub fn block_number(&self) -> U256 {
        match self {
            TxQueueContents::Tx(v) => v.tx_execution_id.block_number,
            TxQueueContents::Reorg(v) => v.tx_execution_id.block_number,
            TxQueueContents::CommitHead(v) => v.block_number,
            TxQueueContents::NewIteration(v) => v.block_env.number,
        }
    }

    pub fn iteration_id(&self) -> u64 {
        match self {
            TxQueueContents::Tx(v) => v.tx_execution_id.iteration_id,
            TxQueueContents::Reorg(v) => v.tx_execution_id.iteration_id,
            TxQueueContents::CommitHead(v) => v.selected_iteration_id,
            TxQueueContents::NewIteration(v) => v.iteration_id,
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
    /// Current block hash for EIP-2935 (Prague+)
    /// Required for historical block hash storage
    pub(crate) block_hash: B256,
    /// Parent beacon block root for EIP-4788 (Cancun+)
    /// Required for beacon chain root storage
    pub(crate) parent_beacon_block_root: Option<B256>,
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
        block_hash: B256,
        parent_beacon_block_root: Option<B256>,
        timestamp: U256,
    ) -> Self {
        Self {
            block_number,
            selected_iteration_id,
            last_tx_hash,
            n_transactions,
            block_hash,
            parent_beacon_block_root,
            timestamp,
        }
    }

    /// Accessor for the selected iteration identifier.
    pub fn iteration_id(&self) -> u64 {
        self.selected_iteration_id
    }
}

/// Creates a new iteration with a specific block env.
///
/// Contains all data needed to apply system calls - EIP-4788, EIP-2935
/// It should be applied before the transaction execution
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct NewIteration {
    pub(crate) iteration_id: u64,
    pub(crate) block_env: BlockEnv,
    /// Required parent block hash for EIP-2935, stored at current block's slot
    pub(crate) block_hash: B256,
    /// Required parent beacon block root for EIP-4788
    pub(crate) parent_beacon_block_root: Option<B256>,
}

impl NewIteration {
    /// Construct a new iteration event with all system call data.
    pub fn new(
        iteration_id: u64,
        block_env: BlockEnv,
        block_hash: B256,
        parent_beacon_block_root: Option<B256>,
    ) -> Self {
        Self {
            iteration_id,
            block_env,
            block_hash,
            parent_beacon_block_root,
        }
    }
}

/// `crossbeam` sender for the transaction queue. Sends data to tx queue.
pub type TransactionQueueSender = Sender<TxQueueContents>;
/// `crossbeam` receiver for the transaction queue. Receives data from tx queue.
pub type TransactionQueueReceiver = Receiver<TxQueueContents>;
