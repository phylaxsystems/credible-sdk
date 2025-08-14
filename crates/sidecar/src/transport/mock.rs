use crate::{
    engine::queue::{
        TransactionQueueReceiver,
        TransactionQueueSender,
    },
    transport::Transport,
};

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
}

impl MockTransport {
    /// Create a new mock transport.
    /// - `tx_sender`: The sender to the core engine transaction queue.
    /// - `mock_receiver`: The receiver for transactions sent to the mock transport.
    pub fn new(tx_sender: TransactionQueueSender, mock_receiver: TransactionQueueReceiver) -> Self {
        Self {
            tx_sender,
            mock_receiver,
        }
    }
}

impl Transport for MockTransport {
    type Error = MockTransportError;
    async fn run(&self) -> Result<(), MockTransportError> {
        loop {
            let rax = self.mock_receiver.recv().unwrap();
            self.tx_sender
                .send(rax)
                .map_err(|_| MockTransportError::CoreSendError)?;
        }
    }
    async fn stop(&mut self) -> Result<(), MockTransportError> {
        // We dont have anything to cleanup
        Ok(())
    }
}
