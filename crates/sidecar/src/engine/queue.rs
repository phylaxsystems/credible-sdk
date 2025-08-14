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

/// Contains the two possible types that can be sent in the transaction queue.
/// `Block` is a new block being processed, while `Tx` is a new transaction.
///
/// `Block` should be used to mark the end of the current block and the start
/// of a new one.
///
/// `Tx` should be used to append a new transaction to the current block,
/// along with its transaction hash for identification and tracing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxQueueContents {
    Block(BlockEnv),
    Tx { tx_hash: B256, tx_env: TxEnv },
}

/// `crossbeam` sender for the transaction queue. Sends data to tx queue.
pub type TransactionQueueSender = Sender<TxQueueContents>;
/// `crossbeam` receiver for the transaction queue. Receives data from tx queue.
pub type TransactionQueueReceiver = Receiver<TxQueueContents>;
