//! # gRPC processing server
//!
//! This file contains helper functions and implementations for processing messages
//! generated from the protobuf definition in `./sidecar.proto`.
//!
//! These methods get called later when we receive a corresponding protobuf message
//! in the transport.

use super::pb::{
    BasicAck,
    BlockEnvEnvelope,
    GetTransactionsRequest,
    GetTransactionsResponse,
    ReorgRequest,
    SendTransactionsRequest,
    SendTransactionsResponse,
    Transaction,
    TransactionEnv,
    TransactionResult as PbTransactionResult,
    sidecar_transport_server::SidecarTransport,
};
use crate::{
    engine::{
        TransactionResult,
        queue::{
            QueueBlockEnv,
            QueueTransaction,
            TransactionQueueSender,
            TxQueueContents,
        },
    },
    transport::{
        common::{
            HttpDecoderError,
            TxEnvParams,
            to_tx_env_from_fields,
        },
        http::transactions_results::QueryTransactionsResults,
    },
};
use assertion_executor::primitives::ExecutionResult;
use revm::primitives::{
    B256,
    alloy_primitives::TxHash,
};
use std::sync::{
    Arc,
    atomic::{
        AtomicBool,
        Ordering,
    },
};
use tonic::{
    Request,
    Response,
    Status,
};
use tracing::{
    debug,
    instrument,
    trace,
    warn,
};

#[derive(Clone)]
pub struct GrpcService {
    has_blockenv: Arc<AtomicBool>,
    tx_sender: TransactionQueueSender,
    transactions_results: QueryTransactionsResults,
}

impl GrpcService {
    pub fn new(
        has_blockenv: Arc<AtomicBool>,
        tx_sender: TransactionQueueSender,
        transactions_results: QueryTransactionsResults,
    ) -> Self {
        Self {
            has_blockenv,
            tx_sender,
            transactions_results,
        }
    }
}

#[tonic::async_trait]
impl SidecarTransport for GrpcService {
    /// Handle gRPC request for `SendBlockEnv`.
    #[instrument(
        name = "grpc_server::SendBlockEnv",
        skip(self, request),
        level = "debug"
    )]
    async fn send_block_env(
        &self,
        request: Request<BlockEnvEnvelope>,
    ) -> Result<Response<BasicAck>, Status> {
        let payload = request.into_inner();
        trace!("Processing gRPC SendBlockEnv request");

        // Build a combined JSON object to reuse existing QueueBlockEnv deserializer
        let mut base: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&payload.block_env_json)
                .map_err(|e| Status::invalid_argument(format!("invalid block_env_json: {e}")))?;

        if payload.n_transactions > 0 && payload.last_tx_hash.is_empty() {
            return Err(Status::invalid_argument(
                "when n_transactions > 0, last_tx_hash must be provided",
            ));
        }

        if !payload.last_tx_hash.is_empty() {
            base.insert(
                "last_tx_hash".to_string(),
                serde_json::Value::String(payload.last_tx_hash.clone()),
            );
        }
        if payload.n_transactions != 0 {
            base.insert(
                "n_transactions".to_string(),
                serde_json::Value::Number(serde_json::Number::from(payload.n_transactions)),
            );
        }

        let block: QueueBlockEnv = serde_json::from_value(serde_json::Value::Object(base))
            .map_err(|e| Status::invalid_argument(format!("invalid block env contents: {e}")))?;

        let span = tracing::Span::current();
        let event = TxQueueContents::Block(block, span);

        self.transactions_results.add_accepted_tx(&event);
        self.tx_sender
            .send(event)
            .map_err(|e| Status::internal(format!("failed to queue block env: {e}")))?;

        if !self.has_blockenv.load(Ordering::Relaxed) {
            self.has_blockenv.store(true, Ordering::Release);
        }

        Ok(Response::new(BasicAck {
            accepted: true,
            message: "block env accepted".into(),
        }))
    }

    /// Handle gRPC request for `SendTransactions`.
    #[instrument(
        name = "grpc_server::SendTransactions",
        skip(self, request),
        level = "debug"
    )]
    async fn send_transactions(
        &self,
        request: Request<SendTransactionsRequest>,
    ) -> Result<Response<SendTransactionsResponse>, Status> {
        trace!("Processing gRPC SendTransactions request");
        if !self.has_blockenv.load(Ordering::Relaxed) {
            debug!("Rejecting transactions - no block environment available");
            return Err(Status::failed_precondition(
                "block environment not available",
            ));
        }

        let req = request.into_inner();
        let total = req.transactions.len() as u64;
        let mut accepted: u64 = 0;

        for t in req.transactions {
            let queue_tx = match to_queue_tx(&t) {
                Ok(tx) => tx,
                Err(e) => {
                    warn!(error = %e, "Skipping invalid transaction in gRPC batch");
                    continue;
                }
            };

            if let TxQueueContents::Tx(tx, _) = &queue_tx
                && self.transactions_results.is_tx_received(&tx.tx_hash)
            {
                warn!(tx_hash = %tx.tx_hash, "TX hash already received, skipping");
                continue;
            }

            self.transactions_results.add_accepted_tx(&queue_tx);
            self.tx_sender
                .send(queue_tx)
                .map_err(|e| Status::internal(format!("failed to queue tx: {e}")))?;
            accepted += 1;
        }

        Ok(Response::new(SendTransactionsResponse {
            accepted_count: accepted,
            request_count: total,
            message: "Requests processed successfully".into(),
        }))
    }

    /// Handle gRPC request for `Reorg` messages.
    #[instrument(name = "grpc_server::Reorg", skip(self, request), level = "debug")]
    async fn reorg(&self, request: Request<ReorgRequest>) -> Result<Response<BasicAck>, Status> {
        trace!("Processing gRPC Reorg request");
        let payload = request.into_inner();
        let hash: B256 = payload
            .removed_tx_hash
            .parse()
            .map_err(|_| Status::invalid_argument("invalid removed_tx_hash"))?;

        let span = tracing::Span::current();
        let event = TxQueueContents::Reorg(hash, span);
        self.transactions_results.add_accepted_tx(&event);
        self.tx_sender
            .send(event)
            .map_err(|e| Status::internal(format!("failed to queue reorg: {e}")))?;

        Ok(Response::new(BasicAck {
            accepted: true,
            message: "reorg accepted".into(),
        }))
    }

    /// Handle gRPC request for `GetTransactions` messages.
    #[instrument(
        name = "grpc_server::GetTransactions",
        skip(self, request),
        level = "debug"
    )]
    async fn get_transactions(
        &self,
        request: Request<GetTransactionsRequest>,
    ) -> Result<Response<GetTransactionsResponse>, Status> {
        trace!("Processing gRPC GetTransactions request");
        if !self.has_blockenv.load(Ordering::Relaxed) {
            debug!("Rejecting query - no block environment available");
            return Err(Status::failed_precondition(
                "block environment not available",
            ));
        }

        let payload = request.into_inner();
        let mut received = Vec::new();
        let mut not_found = Vec::new();

        for h in &payload.tx_hashes {
            match h.parse::<TxHash>() {
                Ok(hash) => {
                    if self.transactions_results.is_tx_received(&hash) {
                        received.push((h.clone(), hash));
                    } else {
                        not_found.push(h.clone());
                    }
                }
                Err(_) => not_found.push(h.clone()),
            }
        }

        let mut results = Vec::with_capacity(received.len());
        for (h, hash) in received {
            let result = match self.transactions_results.request_transaction_result(&hash) {
                crate::transactions_state::RequestTransactionResult::Result(r) => r,
                crate::transactions_state::RequestTransactionResult::Channel(rx) => {
                    match rx.await {
                        Ok(r) => r,
                        Err(_) => return Err(Status::internal("engine unavailable")),
                    }
                }
            };
            results.push(into_pb_transaction_result(h, &result));
        }

        Ok(Response::new(GetTransactionsResponse {
            results,
            not_found,
        }))
    }
}

fn into_pb_transaction_result(hash: String, result: &TransactionResult) -> PbTransactionResult {
    match result {
        TransactionResult::ValidationCompleted {
            execution_result,
            is_valid,
        } => {
            let gas_used = execution_result.gas_used();
            if !*is_valid {
                return PbTransactionResult {
                    hash,
                    status: "assertion_failed".into(),
                    gas_used,
                    error: String::new(),
                };
            }
            match execution_result {
                ExecutionResult::Success { .. } => {
                    PbTransactionResult {
                        hash,
                        status: "success".into(),
                        gas_used,
                        error: String::new(),
                    }
                }
                ExecutionResult::Revert { .. } => {
                    PbTransactionResult {
                        hash,
                        status: "reverted".into(),
                        gas_used,
                        error: String::new(),
                    }
                }
                ExecutionResult::Halt { reason, .. } => {
                    PbTransactionResult {
                        hash,
                        status: "halted".into(),
                        gas_used,
                        error: format!("Transaction halted: {reason:?}"),
                    }
                }
            }
        }
        TransactionResult::ValidationError(error) => {
            PbTransactionResult {
                hash,
                status: "failed".into(),
                gas_used: 0,
                error: format!("Validation error: {error}"),
            }
        }
    }
}

fn to_tx_env(env: &TransactionEnv) -> Result<revm::context::TxEnv, HttpDecoderError> {
    to_tx_env_from_fields(&TxEnvParams {
        caller: &env.caller,
        gas_limit: env.gas_limit,
        gas_price: &env.gas_price,
        transact_to: Some(&env.transact_to),
        value: &env.value,
        data: &env.data,
        nonce: env.nonce,
        chain_id: env.chain_id,
    })
}

fn to_queue_tx(t: &Transaction) -> Result<TxQueueContents, HttpDecoderError> {
    let tx_hash: B256 = t
        .hash
        .parse()
        .map_err(|_| HttpDecoderError::InvalidHash(t.hash.clone()))?;
    let tx_env = to_tx_env(t.tx_env.as_ref().ok_or(HttpDecoderError::MissingParams)?)?;
    let span = tracing::Span::current();
    Ok(TxQueueContents::Tx(
        QueueTransaction { tx_hash, tx_env },
        span,
    ))
}
