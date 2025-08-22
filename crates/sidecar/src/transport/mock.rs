use crate::{
    engine::queue::{
        TransactionQueueReceiver,
        TransactionQueueSender,
    },
    transactions_state::TransactionsState,
    transport::Transport,
};
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum MockTransportError {
    #[error("Error sending data to core engine via channel")]
    CoreSendError,
}

/// `MockTransport` should be used to instantiate a sidecar for tests.
/// Instead of connecting it to a real driver, we can send transactions
/// via a channel to the mock transport, forwarding them to the core engine.
#[derive(Debug, Clone)]
pub struct MockTransport {
    /// Core engine queue sender.
    tx_sender: TransactionQueueSender,
    /// Transactions sent to this channel will be forwarded
    /// to the core engine queue.
    mock_receiver: TransactionQueueReceiver,
    /// Shared transaction state results.
    _state_results: Arc<TransactionsState>,
}

impl MockTransport {
    /// Create a new mock transport with explicit receiver.
    /// This is for backwards compatibility and testing.
    pub fn with_receiver(
        tx_sender: TransactionQueueSender,
        mock_receiver: TransactionQueueReceiver,
        state_results: Arc<TransactionsState>,
    ) -> Self {
        Self {
            tx_sender,
            mock_receiver,
            _state_results: state_results,
        }
    }
}

impl Transport for MockTransport {
    type Error = MockTransportError;
    type Config = ();

    fn new(
        _config: (),
        tx_sender: TransactionQueueSender,
        state_results: Arc<TransactionsState>,
    ) -> Result<Self, Self::Error> {
        // Create a dummy receiver channel for the trait implementation
        let (_, mock_receiver) = crossbeam::channel::unbounded();
        Ok(Self {
            tx_sender,
            mock_receiver,
            _state_results: state_results,
        })
    }

    async fn run(&self) -> Result<(), MockTransportError> {
        tracing::debug!("MockTransport starting");
        loop {
            // Use tokio::task::yield_now() to make this async-friendly
            tokio::task::yield_now().await;

            match self.mock_receiver.try_recv() {
                Ok(rax) => {
                    tracing::debug!("MockTransport forwarding message to engine");
                    self.tx_sender
                        .send(rax)
                        .map_err(|_| MockTransportError::CoreSendError)?;
                }
                Err(crossbeam::channel::TryRecvError::Empty) => {
                    // No data yet, yield and try again
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    continue;
                }
                Err(crossbeam::channel::TryRecvError::Disconnected) => {
                    // Channel closed, exit gracefully
                    tracing::debug!("MockTransport channel disconnected, stopping");
                    break;
                }
            }
        }
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), MockTransportError> {
        // We dont have anything to cleanup
        Ok(())
    }
}
