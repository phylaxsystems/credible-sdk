//! gRPC server implementation for sidecar transport

use super::proto::{
    sidecar_service_server::SidecarService,
    *,
};
use crate::{
    engine::queue::{TransactionQueueSender, TxQueueContents},
    transactions_state::TransactionsState,
    transport::{
        decoder::HttpDecoderError,
        grpc::proto::GrpcTransactionDecoder,
    },
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, instrument, trace, warn};

/// gRPC server implementation for the sidecar
#[derive(Debug)]
pub struct GrpcTransportServer {
    /// Signal if the transport has seen a blockenv
    has_blockenv: Arc<AtomicBool>,
    /// Core engine queue sender
    tx_sender: TransactionQueueSender,
    /// Shared transaction results state
    transactions_state: Arc<TransactionsState>,
}

impl GrpcTransportServer {
    pub fn new(
        has_blockenv: Arc<AtomicBool>,
        tx_sender: TransactionQueueSender,
        transactions_state: Arc<TransactionsState>,
    ) -> Self {
        Self {
            has_blockenv,
            tx_sender,
            transactions_state,
        }
    }
}

#[tonic::async_trait]
impl SidecarService for GrpcTransportServer {
    #[instrument(name = "grpc_server::send_transactions", skip(self), level = "debug")]
    async fn send_transactions(
        &self,
        request: Request<SendTransactionsRequest>,
    ) -> Result<Response<SendTransactionsResponse>, Status> {
        let request = request.into_inner();
        debug!("Processing gRPC sendTransactions request with {} transactions", request.transactions.len());

        // Check if we have block environment before processing transactions
        if !self.has_blockenv.load(Ordering::Relaxed) {
            debug!("Rejecting transactions - no block environment available");
            return Err(Status::failed_precondition(
                "Block environment not available. Send block environment first.",
            ));
        }

        // Decode transactions
        let queue_transactions = match GrpcTransactionDecoder::decode_send_transactions(&request) {
            Ok(txs) => txs,
            Err(e) => {
                error!("Failed to decode gRPC transactions: {}", e);
                return Err(Status::invalid_argument(format!("Failed to decode transactions: {}", e)));
            }
        };

        let transaction_count = queue_transactions.len();
        let mut processed_count = 0;
        let mut failed_hashes = Vec::new();

        // Send each decoded transaction to the queue
        for queue_tx in queue_transactions {
            match &queue_tx {
                TxQueueContents::Tx(tx) => {
                    if let Err(e) = self.tx_sender.send(queue_tx) {
                        error!("Failed to send transaction to queue: {}", e);
                        failed_hashes.push(tx.tx_hash.to_string());
                    } else {
                        processed_count += 1;
                    }
                }
                _ => {
                    error!("Unexpected queue content type in transaction processing");
                }
            }
        }

        debug!(
            transaction_count = transaction_count,
            processed_count = processed_count,
            failed_count = failed_hashes.len(),
            "Processed gRPC transaction batch"
        );

        let response = SendTransactionsResponse {
            status: "accepted".to_string(),
            transaction_count: processed_count as u32,
            message: format!("Successfully processed {} transactions", processed_count),
            failed: failed_hashes,
        };

        Ok(Response::new(response))
    }

    #[instrument(name = "grpc_server::send_block_env", skip(self), level = "debug")]
    async fn send_block_env(
        &self,
        request: Request<SendBlockEnvRequest>,
    ) -> Result<Response<SendBlockEnvResponse>, Status> {
        let request = request.into_inner();
        debug!("Processing gRPC sendBlockEnv request for block {}", request.number);

        // Decode block environment
        let block_env_queue = match GrpcTransactionDecoder::decode_block_env(&request) {
            Ok(block_env) => block_env,
            Err(e) => {
                error!("Failed to decode gRPC block environment: {}", e);
                return Err(Status::invalid_argument(format!("Failed to decode block environment: {}", e)));
            }
        };

        // Send BlockEnv to queue
        if let Err(e) = self.tx_sender.send(block_env_queue) {
            error!("Failed to send BlockEnv to queue: {}", e);
            return Err(Status::internal(format!("Failed to queue block environment: {}", e)));
        }

        // Mark that we have a block environment
        self.has_blockenv.store(true, Ordering::Relaxed);

        debug!("Successfully processed BlockEnv for block {}", request.number);

        let response = SendBlockEnvResponse {
            status: "accepted".to_string(),
            block_number: request.number,
            message: format!("Successfully set block environment for block {}", request.number),
        };

        Ok(Response::new(response))
    }

    #[instrument(name = "grpc_server::get_transactions", skip(self), level = "debug")]
    async fn get_transactions(
        &self,
        request: Request<GetTransactionsRequest>,
    ) -> Result<Response<GetTransactionsResponse>, Status> {
        let request = request.into_inner();
        debug!("Processing gRPC getTransactions request for {} hashes", request.hashes.len());

        let mut results = Vec::new();

        for hash in &request.hashes {
            match self.transactions_state.get_transaction_result(hash).await {
                Some(tx_result) => {
                    let result = TransactionResult {
                        hash: hash.clone(),
                        status: tx_result.status,
                        gas_used: tx_result.gas_used,
                        error: tx_result.error,
                    };
                    results.push(result);
                }
                None => {
                    // Transaction not found - could be still processing
                    let result = TransactionResult {
                        hash: hash.clone(),
                        status: "pending".to_string(),
                        gas_used: None,
                        error: None,
                    };
                    results.push(result);
                }
            }
        }

        let response = GetTransactionsResponse { results };

        Ok(Response::new(response))
    }

    #[instrument(name = "grpc_server::revert_transaction", skip(self), level = "debug")]
    async fn revert_transaction(
        &self,
        request: Request<RevertTransactionRequest>,
    ) -> Result<Response<RevertTransactionResponse>, Status> {
        let request = request.into_inner();
        debug!(
            tx_hash = %request.tx_hash,
            reason = %request.reason,
            "Processing gRPC revertTransaction request"
        );

        // TODO: Implement actual reversion logic
        // This would involve:
        // 1. Remove transaction from engine queue if still pending
        // 2. Invalidate cache entries for this transaction
        // 3. Rollback any speculative state changes
        // 4. Update transaction results to mark as reverted

        // For now, just acknowledge the reversion
        // Note: This would need to be implemented in TransactionsState
        // self.transactions_state.mark_transaction_reverted(&request.tx_hash, &request.reason).await;

        info!("Transaction {} reverted due to: {}", request.tx_hash, request.reason);

        let response = RevertTransactionResponse {
            status: "reverted".to_string(),
            tx_hash: request.tx_hash.clone(),
            message: format!("Transaction {} reverted: {}", request.tx_hash, request.reason),
        };

        Ok(Response::new(response))
    }

    #[instrument(name = "grpc_server::health", skip(self), level = "trace")]
    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        trace!("Health check requested via gRPC");

        let response = HealthResponse {
            status: "healthy".to_string(),
            message: "gRPC sidecar transport is running".to_string(),
        };

        Ok(Response::new(response))
    }

    #[instrument(name = "grpc_server::send_transactions_stream", skip(self), level = "debug")]
    async fn send_transactions_stream(
        &self,
        request: Request<tonic::Streaming<SendTransactionsRequest>>,
    ) -> Result<Response<tonic::Streaming<SendTransactionsResponse>>, Status> {
        let mut stream = request.into_inner();

        let (tx, rx) = tokio::sync::mpsc::channel(100);

        let has_blockenv = self.has_blockenv.clone();
        let tx_sender = self.tx_sender.clone();

        // Spawn task to handle streaming requests
        tokio::spawn(async move {
            while let Some(request) = stream.message().await.unwrap_or(None) {
                debug!("Processing streaming transaction batch with {} transactions", request.transactions.len());

                // Check block environment
                if !has_blockenv.load(Ordering::Relaxed) {
                    let error_response = SendTransactionsResponse {
                        status: "error".to_string(),
                        transaction_count: 0,
                        message: "Block environment not available".to_string(),
                        failed: request.transactions.iter().map(|t| t.hash.clone()).collect(),
                    };
                    
                    if tx.send(Ok(error_response)).await.is_err() {
                        break;
                    }
                    continue;
                }

                // Process transactions
                match GrpcTransactionDecoder::decode_send_transactions(&request) {
                    Ok(queue_transactions) => {
                        let mut processed = 0;
                        let mut failed = Vec::new();

                        for queue_tx in queue_transactions {
                            match &queue_tx {
                                TxQueueContents::Tx(tx) => {
                                    if tx_sender.send(queue_tx).is_ok() {
                                        processed += 1;
                                    } else {
                                        failed.push(tx.tx_hash.to_string());
                                    }
                                }
                                _ => {}
                            }
                        }

                        let response = SendTransactionsResponse {
                            status: "accepted".to_string(),
                            transaction_count: processed,
                            message: format!("Processed {} transactions", processed),
                            failed,
                        };

                        if tx.send(Ok(response)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let error_response = SendTransactionsResponse {
                            status: "error".to_string(),
                            transaction_count: 0,
                            message: format!("Decode error: {}", e),
                            failed: request.transactions.iter().map(|t| t.hash.clone()).collect(),
                        };

                        if tx.send(Ok(error_response)).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });

        let output_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream)))
    }
}
