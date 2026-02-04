//! # gRPC processing server
//!
//! This file contains helper functions and implementations for processing messages
//! generated from the protobuf definition in `./sidecar.proto`.
//!
//! These methods get called later when we receive a corresponding protobuf message
//! in the transport.
//!
//! ## Streaming Architecture
//! - `StreamEvents`: Bidirectional stream for events (commits, iterations, transactions, reorgs)
//! - `SubscribeResults`: Server stream pushing transaction results as they complete

use super::pb::{
    self,
    BasicAck,
    BlockEnv as PbBlockEnv,
    CommitHead as PbCommitHead,
    Event,
    GetTransactionRequest,
    GetTransactionResponse,
    GetTransactionsRequest,
    GetTransactionsResponse,
    NewIteration as PbNewIteration,
    ResultStatus,
    StreamAck,
    SubscribeResultsRequest,
    Transaction,
    TransactionEnv,
    TransactionResult as PbTransactionResult,
    TxExecutionId as PbTxExecutionId,
    event::Event as EventVariant,
    get_transaction_response::Outcome as GetTransactionOutcome,
    sidecar_transport_server::SidecarTransport,
};
use crate::{
    engine::{
        TransactionResult,
        queue::{
            CommitHead,
            NewIteration,
            QueueTransaction,
            ReorgRequest,
            TransactionQueueSender,
            TxQueueContents,
        },
    },
    execution_ids::TxExecutionId,
    transactions_state::TransactionResultEvent,
    transport::{
        event_id_deduplication::EventIdBuffer,
        invalidation_dupe::{
            ContentHashCache,
            tx_content_hash,
        },
        rpc_metrics::RpcRequestDuration,
        transactions_results::{
            AcceptedState,
            QueryTransactionsResults,
            wait_for_pending_transactions,
        },
    },
};
use alloy::{
    eips::eip7702::{
        Authorization,
        RecoveredAuthorization,
        SignedAuthorization,
    },
    signers::Either,
};
use assertion_executor::primitives::{
    Address,
    Bytes,
    ExecutionResult,
    TxKind,
    U256,
};
use futures::stream::{
    FuturesUnordered,
    StreamExt,
};
use metrics::{
    counter,
    histogram,
};
use revm::{
    context::{
        BlockEnv as RevmBlockEnv,
        TxEnv,
        transaction::{
            AccessList,
            AccessListItem,
        },
    },
    context_interface::block::BlobExcessGasAndPrice as RevmBlobExcessGasAndPrice,
    primitives::{
        B256,
        alloy_primitives::TxHash,
    },
};
use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            AtomicU64,
            Ordering,
        },
    },
    time::Instant,
};
use thiserror::Error;
use tokio::{
    sync::{
        Mutex,
        broadcast,
        mpsc,
    },
    task::AbortHandle,
};
use tokio_stream::Stream;
use tonic::{
    Request,
    Response,
    Status,
    Streaming,
};
use tracing::{
    debug,
    error,
    info,
    instrument,
    trace,
    warn,
};

mod error_messages {
    pub const INVALID_TX_HASH: &str = "invalid tx_hash: expected 32 bytes";
    pub const INVALID_BENEFICIARY: &str = "invalid beneficiary: expected 20 bytes";
    pub const INVALID_DIFFICULTY: &str = "invalid difficulty: expected 32 bytes";
    pub const INVALID_PREVRANDAO: &str = "invalid prevrandao: expected 32 bytes";
    pub const INVALID_BLOB_GASPRICE: &str = "invalid blob_gasprice: expected 16 bytes";
    pub const INVALID_BLOCK_NUMBER: &str = "invalid block_number: expected 32 bytes";
    pub const INVALID_TIMESTAMP: &str = "invalid timestamp: expected 32 bytes";
    pub const MISSING_TX_EXECUTION_ID: &str = "missing tx_execution_id";
    pub const SELECTED_ITERATION_REQUIRED: &str = "selected_iteration_id is required";
    pub const BLOCK_ENV_REQUIRED: &str = "block_env is required";
    pub const EVENT_PAYLOAD_MISSING: &str = "event payload missing";
    pub const COMMIT_HEAD_REQUIRED: &str = "commit head must be received before other events";
    pub const INVALID_LAST_TX_HASH: &str = "invalid last_tx_hash: expected 32 bytes";
    pub const N_TX_ZERO_HASH_PRESENT: &str =
        "when n_transactions is 0, last_tx_hash must be null, empty, or missing";
    pub const N_TX_POSITIVE_HASH_MISSING: &str =
        "when n_transactions > 0, last_tx_hash must be provided";
    pub const QUEUE_TX_FAILED: &str = "failed to queue transaction";
    pub const QUEUE_REORG_FAILED: &str = "failed to queue reorg";
    pub const INVALID_BLOCK_HASH: &str = "invalid block_hash: expected 32 bytes";
    pub const INVALID_PARENT_BEACON_BLOCK_ROOT: &str =
        "invalid parent_beacon_block_root: expected 32 bytes";
}

/// Expected byte lengths for binary-encoded fields.
const ADDRESS_LEN: usize = 20;
const HASH_LEN: usize = 32;
const U128_LEN: usize = 16;
const U256_LEN: usize = 32;

// Optimized error type that avoids heap allocations for common error cases.
// Only stores minimal metadata needed to reconstruct the error if necessary.
#[derive(Debug, Clone, Error)]
pub enum GrpcDecodeError {
    #[error("missing tx_env")]
    MissingTxEnv,
    #[error("missing tx_execution_id")]
    MissingTxExecutionId,
    #[error("invalid address: expected {ADDRESS_LEN} bytes, got {0}")]
    InvalidAddress(usize),
    #[error("invalid hash: expected {HASH_LEN} bytes, got {0}")]
    InvalidHash(usize),
    #[error("invalid u128: expected {U128_LEN} bytes, got {0}")]
    InvalidU128(usize),
    #[error("invalid u256: expected {U256_LEN} bytes, got {0}")]
    InvalidU256(usize),
    #[error("invalid tx_type: {0}")]
    InvalidTxType(u32),
    #[error("invalid signature")]
    InvalidSignature,
}

#[inline]
fn decode_address(bytes: &[u8]) -> Result<Address, GrpcDecodeError> {
    if bytes.len() != ADDRESS_LEN {
        return Err(GrpcDecodeError::InvalidAddress(bytes.len()));
    }
    Ok(Address::from_slice(bytes))
}

#[inline]
fn decode_b256(bytes: &[u8]) -> Result<B256, GrpcDecodeError> {
    if bytes.len() != HASH_LEN {
        return Err(GrpcDecodeError::InvalidHash(bytes.len()));
    }
    Ok(B256::from_slice(bytes))
}

#[inline]
fn decode_u128_be(bytes: &[u8]) -> Result<u128, GrpcDecodeError> {
    if bytes.len() != U128_LEN {
        return Err(GrpcDecodeError::InvalidU128(bytes.len()));
    }
    Ok(u128::from_be_bytes(bytes.try_into().unwrap()))
}

#[inline]
fn decode_u256_be(bytes: &[u8]) -> Result<U256, GrpcDecodeError> {
    if bytes.len() != U256_LEN {
        return Err(GrpcDecodeError::InvalidU256(bytes.len()));
    }
    Ok(U256::from_be_bytes::<32>(bytes.try_into().unwrap()))
}

#[inline]
fn encode_u256_be(value: U256) -> Vec<u8> {
    value.to_be_bytes::<32>().to_vec()
}

#[inline]
fn decode_tx_kind(bytes: &[u8]) -> Result<TxKind, GrpcDecodeError> {
    if bytes.is_empty() {
        Ok(TxKind::Create)
    } else {
        Ok(TxKind::Call(decode_address(bytes)?))
    }
}

#[inline]
fn encode_address(addr: Address) -> Vec<u8> {
    addr.as_slice().to_vec()
}

#[inline]
fn encode_b256(hash: B256) -> Vec<u8> {
    hash.as_slice().to_vec()
}

#[inline]
fn encode_tx_execution_id(id: TxExecutionId) -> PbTxExecutionId {
    PbTxExecutionId {
        block_number: encode_u256_be(id.block_number),
        iteration_id: id.iteration_id,
        tx_hash: encode_b256(id.tx_hash),
        index: id.index,
    }
}

#[inline]
fn decode_tx_execution_id(pb: &PbTxExecutionId) -> Result<TxExecutionId, GrpcDecodeError> {
    let block_number = decode_u256_be(&pb.block_number)?;
    let tx_hash = decode_b256(&pb.tx_hash)?;
    Ok(TxExecutionId::new(
        block_number,
        pb.iteration_id,
        tx_hash,
        pb.index,
    ))
}

#[inline]
fn decode_access_list(items: &[pb::AccessListItem]) -> Result<AccessList, GrpcDecodeError> {
    items
        .iter()
        .map(|item| {
            let address = decode_address(&item.address)?;
            let storage_keys: Vec<B256> = item
                .storage_keys
                .iter()
                .map(|k| decode_b256(k))
                .collect::<Result<_, _>>()?;
            Ok(AccessListItem {
                address,
                storage_keys,
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(AccessList)
}

fn decode_authorization_list(
    auths: &[pb::Authorization],
) -> Result<Vec<Either<SignedAuthorization, RecoveredAuthorization>>, GrpcDecodeError> {
    auths
        .iter()
        .map(|auth| {
            let chain_id = decode_u256_be(&auth.chain_id)?;
            let address = decode_address(&auth.address)?;
            let r = decode_u256_be(&auth.r)?;
            let s = decode_u256_be(&auth.s)?;
            let y_parity =
                u8::try_from(auth.y_parity).map_err(|_| GrpcDecodeError::InvalidSignature)?;

            let inner = Authorization {
                chain_id,
                address,
                nonce: auth.nonce,
            };

            let signed = SignedAuthorization::new_unchecked(inner, y_parity, r, s);
            Ok(Either::Left(signed))
        })
        .collect()
}

/// Convert protobuf TransactionEnv to revm TxEnv using binary decoding.
pub fn decode_tx_env(pb: &TransactionEnv) -> Result<TxEnv, GrpcDecodeError> {
    let caller = decode_address(&pb.caller)?;
    let gas_price = decode_u128_be(&pb.gas_price)?;
    let kind = decode_tx_kind(&pb.transact_to)?;
    let value = decode_u256_be(&pb.value)?;
    let data = Bytes::from(pb.data.clone());
    let access_list = decode_access_list(&pb.access_list)?;

    let gas_priority_fee = pb
        .gas_priority_fee
        .as_ref()
        .filter(|b| !b.is_empty())
        .map(|b| decode_u128_be(b))
        .transpose()?;

    let blob_hashes: Vec<B256> = pb
        .blob_hashes
        .iter()
        .map(|h| decode_b256(h))
        .collect::<Result<_, _>>()?;

    let max_fee_per_blob_gas = if pb.max_fee_per_blob_gas.is_empty() {
        0u128
    } else {
        decode_u128_be(&pb.max_fee_per_blob_gas)?
    };

    let authorization_list = decode_authorization_list(&pb.authorization_list)?;

    Ok(TxEnv {
        tx_type: u8::try_from(pb.tx_type)
            .map_err(|_| GrpcDecodeError::InvalidTxType(pb.tx_type))?,
        caller,
        gas_limit: pb.gas_limit,
        gas_price,
        kind,
        value,
        data,
        nonce: pb.nonce,
        chain_id: pb.chain_id,
        access_list,
        gas_priority_fee,
        blob_hashes,
        max_fee_per_blob_gas,
        authorization_list,
    })
}

/// Convert a Transaction to queue transaction contents.
pub fn decode_transaction(t: &Transaction) -> Result<TxQueueContents, GrpcDecodeError> {
    let tx_env = decode_tx_env(t.tx_env.as_ref().ok_or(GrpcDecodeError::MissingTxEnv)?)?;

    let pb_id = t
        .tx_execution_id
        .as_ref()
        .ok_or(GrpcDecodeError::MissingTxExecutionId)?;

    let tx_execution_id = decode_tx_execution_id(pb_id)?;

    let prev_tx_hash = t
        .prev_tx_hash
        .as_ref()
        .filter(|b| !b.is_empty())
        .map(|b| decode_b256(b))
        .transpose()?;

    Ok(TxQueueContents::Tx(QueueTransaction {
        tx_execution_id,
        tx_env,
        prev_tx_hash,
    }))
}

#[inline]
fn decode_commit_head(pb: &PbCommitHead) -> Result<CommitHead, Status> {
    let last_tx_hash = pb
        .last_tx_hash
        .as_ref()
        .filter(|b| !b.is_empty())
        .map(|b| decode_b256(b))
        .transpose()
        .map_err(|_| Status::invalid_argument(error_messages::INVALID_LAST_TX_HASH))?;

    let block_number = decode_u256_be(&pb.block_number)
        .map_err(|_| Status::invalid_argument(error_messages::INVALID_BLOCK_NUMBER))?;

    let timestamp = decode_u256_be(&pb.timestamp)
        .map_err(|_| Status::invalid_argument(error_messages::INVALID_TIMESTAMP))?;

    if pb.n_transactions == 0 && last_tx_hash.is_some() {
        return Err(Status::invalid_argument(
            error_messages::N_TX_ZERO_HASH_PRESENT,
        ));
    }

    if pb.n_transactions > 0 && last_tx_hash.is_none() {
        return Err(Status::invalid_argument(
            error_messages::N_TX_POSITIVE_HASH_MISSING,
        ));
    }

    if pb.block_hash.is_empty() {
        return Err(Status::invalid_argument("block_hash is required"));
    }

    // Decode required block_hash for EIP-2935
    let block_hash = decode_b256(&pb.block_hash)
        .map_err(|_| Status::invalid_argument(error_messages::INVALID_BLOCK_HASH))?;

    // Decode parent_beacon_block_root for EIP-4788
    let parent_beacon_block_root = pb
        .parent_beacon_block_root
        .as_ref()
        .filter(|b| !b.is_empty())
        .map(|b| decode_b256(b))
        .transpose()
        .map_err(|_| Status::invalid_argument(error_messages::INVALID_PARENT_BEACON_BLOCK_ROOT))?;

    Ok(CommitHead::new(
        block_number,
        pb.selected_iteration_id,
        last_tx_hash,
        pb.n_transactions,
        block_hash,
        parent_beacon_block_root,
        timestamp,
    ))
}

#[inline]
fn decode_block_env(pb: PbBlockEnv) -> Result<RevmBlockEnv, Status> {
    let number = decode_u256_be(&pb.number)
        .map_err(|_| Status::invalid_argument(error_messages::INVALID_BLOCK_NUMBER))?;

    let timestamp = decode_u256_be(&pb.timestamp)
        .map_err(|_| Status::invalid_argument(error_messages::INVALID_TIMESTAMP))?;

    let beneficiary = decode_address(&pb.beneficiary)
        .map_err(|_| Status::invalid_argument(error_messages::INVALID_BENEFICIARY))?;

    let difficulty = decode_u256_be(&pb.difficulty)
        .map_err(|_| Status::invalid_argument(error_messages::INVALID_DIFFICULTY))?;

    let prevrandao = pb
        .prevrandao
        .filter(|b| !b.is_empty())
        .map(|b| decode_b256(&b))
        .transpose()
        .map_err(|_| Status::invalid_argument(error_messages::INVALID_PREVRANDAO))?;

    let blob_excess_gas_and_price = pb
        .blob_excess_gas_and_price
        .map(|b| -> Result<RevmBlobExcessGasAndPrice, Status> {
            let blob_gasprice = decode_u128_be(&b.blob_gasprice)
                .map_err(|_| Status::invalid_argument(error_messages::INVALID_BLOB_GASPRICE))?;
            Ok(RevmBlobExcessGasAndPrice {
                excess_blob_gas: b.excess_blob_gas,
                blob_gasprice,
            })
        })
        .transpose()?;

    Ok(RevmBlockEnv {
        number,
        beneficiary,
        timestamp,
        gas_limit: pb.gas_limit,
        basefee: pb.basefee,
        difficulty,
        prevrandao,
        blob_excess_gas_and_price,
    })
}

#[inline]
fn decode_new_iteration(pb: PbNewIteration) -> Result<NewIteration, Status> {
    let block_env = decode_block_env(
        pb.block_env
            .ok_or_else(|| Status::invalid_argument(error_messages::BLOCK_ENV_REQUIRED))?,
    )?;

    // Decode parent_block_hash for EIP-2935
    let parent_block_hash = pb
        .parent_block_hash
        .as_ref()
        .filter(|b| !b.is_empty())
        .map(|b| decode_b256(b))
        .transpose()
        .map_err(|_| Status::invalid_argument(error_messages::INVALID_BLOCK_HASH))?;

    // Decode parent_beacon_block_root for EIP-4788
    let parent_beacon_block_root = pb
        .parent_beacon_block_root
        .as_ref()
        .filter(|b| !b.is_empty())
        .map(|b| decode_b256(b))
        .transpose()
        .map_err(|_| Status::invalid_argument(error_messages::INVALID_PARENT_BEACON_BLOCK_ROOT))?;

    Ok(NewIteration::new(
        pb.iteration_id,
        block_env,
        parent_block_hash,
        parent_beacon_block_root,
    ))
}

#[inline]
fn encode_transaction_result(
    tx_execution_id: TxExecutionId,
    result: &TransactionResult,
) -> PbTransactionResult {
    let pb_tx_id = Some(encode_tx_execution_id(tx_execution_id));

    match result {
        TransactionResult::ValidationCompleted {
            execution_result,
            is_valid,
        } => {
            let gas_used = execution_result.gas_used();
            if !*is_valid {
                return PbTransactionResult {
                    tx_execution_id: pb_tx_id,
                    status: ResultStatus::AssertionFailed.into(),
                    gas_used,
                    error: String::new(),
                };
            }
            match execution_result {
                ExecutionResult::Success { .. } => {
                    PbTransactionResult {
                        tx_execution_id: pb_tx_id,
                        status: ResultStatus::Success.into(),
                        gas_used,
                        error: String::new(),
                    }
                }
                ExecutionResult::Revert { .. } => {
                    PbTransactionResult {
                        tx_execution_id: pb_tx_id,
                        status: ResultStatus::Reverted.into(),
                        gas_used,
                        error: String::new(),
                    }
                }
                ExecutionResult::Halt { reason, .. } => {
                    PbTransactionResult {
                        tx_execution_id: pb_tx_id,
                        status: ResultStatus::Halted.into(),
                        gas_used,
                        error: format!("Transaction halted: {reason:?}"),
                    }
                }
            }
        }
        TransactionResult::ValidationError(error) => {
            PbTransactionResult {
                tx_execution_id: pb_tx_id,
                status: ResultStatus::Failed.into(),
                gas_used: 0,
                error: format!("Validation error: {error}"),
            }
        }
    }
}

/// Type alias for the streaming results output.
type ResultStream = Pin<Box<dyn Stream<Item = Result<PbTransactionResult, Status>> + Send>>;

/// Type alias for the streaming acks output.
type AckStream = Pin<Box<dyn Stream<Item = Result<StreamAck, Status>> + Send>>;

/// Buffer size for the result broadcaster channel.
const RESULT_BROADCAST_BUFFER: usize = 16_384;

/// Shared state that lives as long as at least one GrpcService clone exists.
struct GrpcServiceInner {
    commit_head_seen: AtomicBool,
    result_broadcaster: broadcast::Sender<PbTransactionResult>,
    abort_handles: Mutex<HashMap<u64, AbortHandle>>,
    next_abort_id: AtomicU64,
    /// Buffer for detecting duplicate event IDs
    event_id_buffer: EventIdBuffer,
    /// Tracks when the last commit head was received for inter-commit-head duration metrics.
    last_commit_head_time: std::sync::Mutex<Option<Instant>>,
    /// Content-hash deduplication cache for transactions.
    content_hash_cache: ContentHashCache,
}

impl Drop for GrpcServiceInner {
    fn drop(&mut self) {
        // get_mut() avoids async lock—safe because we have &mut self (exclusive access)
        let handles = self.abort_handles.get_mut();
        for handle in handles.drain().map(|(_, handle)| handle) {
            handle.abort();
        }
    }
}

#[derive(Clone)]
pub struct GrpcService {
    tx_sender: TransactionQueueSender,
    transactions_results: QueryTransactionsResults,
    inner: Arc<GrpcServiceInner>,
}

impl GrpcService {
    /// Create a new GrpcService.
    ///
    /// If `result_event_rx` is provided, spawns a background task that consumes
    /// transaction result events and broadcasts them to gRPC subscribers.
    pub fn new(
        tx_sender: TransactionQueueSender,
        transactions_results: QueryTransactionsResults,
        result_event_rx: Option<flume::Receiver<TransactionResultEvent>>,
        event_buffer_capacity: usize,
        content_hash_cache: ContentHashCache,
    ) -> Self {
        let (result_broadcaster, _) = broadcast::channel(RESULT_BROADCAST_BUFFER);

        let mut abort_handle = vec![];
        // Spawn background task to forward results from flume to broadcast
        if let Some(rx) = result_event_rx {
            let broadcaster = result_broadcaster.clone();
            let handle = tokio::spawn(async move {
                Self::result_forwarder_task(rx, broadcaster).await;
            });
            // Use blocking_lock since we're not in an async context
            abort_handle.push(handle.abort_handle());
        }

        let next_abort_id = u64::try_from(abort_handle.len())
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        let inner = Arc::new(GrpcServiceInner {
            commit_head_seen: AtomicBool::new(false),
            result_broadcaster,
            abort_handles: Mutex::new(
                abort_handle
                    .into_iter()
                    .enumerate()
                    .map(|(id, handle)| (u64::try_from(id).unwrap_or(u64::MAX), handle))
                    .collect(),
            ),
            next_abort_id: AtomicU64::new(next_abort_id),
            event_id_buffer: EventIdBuffer::new(event_buffer_capacity),
            last_commit_head_time: std::sync::Mutex::new(None),
            content_hash_cache,
        });

        Self {
            tx_sender,
            transactions_results,
            inner,
        }
    }

    /// Background task that consumes from the flume channel and broadcasts to subscribers.
    async fn result_forwarder_task(
        rx: flume::Receiver<TransactionResultEvent>,
        broadcaster: broadcast::Sender<PbTransactionResult>,
    ) {
        // Use async recv to avoid blocking the runtime
        while let Ok(event) = rx.recv_async().await {
            let pb_result = encode_transaction_result(event.tx_execution_id, &event.result);

            // Ignore send errors - just means no subscribers
            let subscriber_count = broadcaster.send(pb_result).unwrap_or(0);

            if subscriber_count > 0 {
                debug!(
                    target = "transport::grpc",
                    tx_hash = ?event.tx_execution_id.tx_hash,
                    subscribers = subscriber_count,
                    "Forwarded result to gRPC subscribers"
                );
            }
        }

        info!(
            target = "transport::grpc",
            "Result forwarder task ended - channel closed"
        );
    }

    #[inline]
    fn send_queue_event(&self, contents: TxQueueContents) -> Result<(), Status> {
        self.tx_sender
            .send(contents)
            .map_err(|_| Status::internal(error_messages::QUEUE_TX_FAILED))
    }

    #[inline]
    fn mark_commit_head_seen(&self) {
        self.inner.commit_head_seen.store(true, Ordering::Release);
    }

    /// Records the duration since the last commit head and resets the timer.
    fn record_commit_head_duration(&self) {
        let mut last = self
            .inner
            .last_commit_head_time
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(prev) = *last {
            histogram!("sidecar_transport_block_processing_duration_seconds")
                .record(prev.elapsed());
        }
        *last = Some(Instant::now());
    }

    #[inline]
    fn ensure_commit_head_seen(&self) -> Result<(), Status> {
        if self.inner.commit_head_seen.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(Status::failed_precondition(
                error_messages::COMMIT_HEAD_REQUIRED,
            ))
        }
    }

    /// Process a single event from the stream.
    fn process_event(&self, event: Event, events_processed: &AtomicU64) -> Result<(), Status> {
        // Check for duplicate event_id first (fast path)
        if self
            .inner
            .event_id_buffer
            .check_duplicate_and_insert(event.event_id)
        {
            debug!(
                target = "transport::grpc",
                event_id = event.event_id,
                "Duplicate event_id detected, skipping"
            );
            return Ok(());
        }

        let variant = event
            .event
            .ok_or_else(|| Status::invalid_argument(error_messages::EVENT_PAYLOAD_MISSING))?;

        match variant {
            EventVariant::CommitHead(commit) => {
                let commit_head = decode_commit_head(&commit)?;
                self.send_queue_event(TxQueueContents::CommitHead(commit_head))?;
                self.record_commit_head_duration();
                self.mark_commit_head_seen();
            }
            EventVariant::NewIteration(iteration) => {
                self.ensure_commit_head_seen()?;
                let new_iteration = decode_new_iteration(iteration)?;
                self.send_queue_event(TxQueueContents::NewIteration(new_iteration))?;
            }
            EventVariant::Transaction(transaction) => {
                self.ensure_commit_head_seen()?;
                let queue_tx = decode_transaction(&transaction)
                    .map_err(|e| Status::invalid_argument(format!("invalid transaction: {e}")))?;

                // Content-hash filter: log known-invalidating transactions with identical content
                // (same calldata, value, etc.) even if nonce/gas parameters differ.
                // We don't early-reject; we just increment counters for observability.
                // Note: Engine handles dedup detection and emits debug logs there.
                if let TxQueueContents::Tx(ref tx) = queue_tx {
                    let content_hash = tx_content_hash(&tx.tx_env);
                    if self.inner.content_hash_cache.contains(content_hash) {
                        trace!(
                            tx_hash = ?tx.tx_execution_id.tx_hash,
                            content_hash = ?content_hash,
                            "Known-invalidating content hash detected"
                        );
                    }
                }

                if let TxQueueContents::Tx(ref tx) = queue_tx
                    && self
                        .transactions_results
                        .is_tx_received_now(&tx.tx_execution_id)
                {
                    warn!(
                        tx_hash = ?tx.tx_execution_id.tx_hash,
                        "TX hash already received, skipping"
                    );
                    return Ok(());
                }

                self.transactions_results.add_accepted_tx(&queue_tx);
                self.send_queue_event(queue_tx)?;
            }
            EventVariant::Reorg(reorg) => {
                self.ensure_commit_head_seen()?;
                let pb_id = reorg.tx_execution_id.ok_or_else(|| {
                    Status::invalid_argument(error_messages::MISSING_TX_EXECUTION_ID)
                })?;
                let tx_execution_id = decode_tx_execution_id(&pb_id)
                    .map_err(|e| Status::invalid_argument(e.to_string()))?;
                let mut tx_hashes = Vec::with_capacity(reorg.tx_hashes.len());
                for raw_hash in &reorg.tx_hashes {
                    let parsed = decode_b256(raw_hash)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    tx_hashes.push(parsed);
                }
                // tx_hashes must be non-empty (depth is derived from length)
                if tx_hashes.is_empty() {
                    return Err(Status::invalid_argument("tx_hashes must not be empty"));
                }
                if tx_hashes.last() != Some(&tx_execution_id.tx_hash) {
                    return Err(Status::invalid_argument(
                        "tx_hashes last entry must match tx_hash",
                    ));
                }
                let event = TxQueueContents::Reorg(ReorgRequest {
                    tx_execution_id,
                    tx_hashes,
                });
                self.transactions_results.add_accepted_tx(&event);
                self.tx_sender
                    .send(event)
                    .map_err(|_| Status::internal(error_messages::QUEUE_REORG_FAILED))?;
            }
        }

        events_processed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    #[inline]
    async fn fetch_transaction_result(
        &self,
        tx_execution_id: TxExecutionId,
    ) -> Result<PbTransactionResult, Status> {
        let fetch_started_at = Instant::now();
        let result = match self
            .transactions_results
            .request_transaction_result(&tx_execution_id)
        {
            crate::transactions_state::RequestTransactionResult::Result(r) => r,
            crate::transactions_state::RequestTransactionResult::Channel(mut rx) => {
                rx.recv()
                    .await
                    .map_err(|_| Status::internal("engine unavailable"))?
            }
        };

        let pb_result = encode_transaction_result(tx_execution_id, &result);
        histogram!("sidecar_fetch_transaction_result_duration").record(fetch_started_at.elapsed());

        Ok(pb_result)
    }

    /// Collect transaction results with parallel fetching.
    /// Optimized to minimize clones - only clones hash bytes when actually needed for errors.
    async fn collect_results_parallel<'a, I>(
        &self,
        ids: I,
    ) -> Result<(Vec<PbTransactionResult>, Vec<Vec<u8>>), Status>
    where
        I: IntoIterator<Item = &'a PbTxExecutionId>,
    {
        let iter = ids.into_iter();
        let (lower_bound, _) = iter.size_hint();

        // Phase 1: Parse all IDs and categorize
        // Store references to avoid cloning until necessary
        let mut ready_ids = Vec::with_capacity(lower_bound);
        let mut waiters: Vec<(TxExecutionId, Vec<u8>, _)> = Vec::new();
        let mut not_found = Vec::new();

        for pb_id in iter {
            match decode_tx_execution_id(pb_id) {
                Ok(tx_execution_id) => {
                    match self.transactions_results.is_tx_received(&tx_execution_id) {
                        AcceptedState::Yes => ready_ids.push(tx_execution_id),
                        AcceptedState::NotYet(rx) => {
                            info!(
                                target = "transport::grpc",
                                tx_execution_id = ?tx_execution_id,
                                "Transaction not yet received, waiting for it"
                            );
                            // Clone hash bytes only when needed for waiting
                            waiters.push((tx_execution_id, pb_id.tx_hash.clone(), rx));
                        }
                    }
                }
                // Only clone on parse error
                Err(_) => not_found.push(pb_id.tx_hash.clone()),
            }
        }

        // Phase 2: Fetch ready results in parallel using FuturesUnordered
        let results_capacity = ready_ids.len() + waiters.len();
        let mut results = Vec::with_capacity(results_capacity);

        if !ready_ids.is_empty() {
            let mut futures: FuturesUnordered<_> = ready_ids
                .into_iter()
                .map(|id| self.fetch_transaction_result(id))
                .collect();

            while let Some(res) = futures.next().await {
                results.push(res?);
            }
        }

        // Phase 3: Handle pending transactions
        if !waiters.is_empty() {
            // Convert to owned data only for the wait operation
            let waiters_owned: Vec<_> = waiters
                .into_iter()
                .map(|(id, hash, rx)| ((id, hash), rx))
                .collect();

            let wait_outcomes = wait_for_pending_transactions(waiters_owned).await;

            // Collect IDs that became ready
            let mut pending_ready_ids = Vec::with_capacity(wait_outcomes.len());

            for ((tx_execution_id, hash), wait_result) in wait_outcomes {
                match wait_result {
                    Ok(Ok(true)) => pending_ready_ids.push(tx_execution_id),
                    Ok(Ok(false) | Err(_)) | Err(_) => {
                        not_found.push(hash);
                    }
                }
            }

            // Fetch pending-but-now-ready results in parallel
            if !pending_ready_ids.is_empty() {
                let mut pending_futures: FuturesUnordered<_> = pending_ready_ids
                    .into_iter()
                    .map(|id| self.fetch_transaction_result(id))
                    .collect();

                while let Some(res) = pending_futures.next().await {
                    results.push(res?);
                }
            }
        }

        Ok((results, not_found))
    }
}

#[tonic::async_trait]
impl SidecarTransport for GrpcService {
    type StreamEventsStream = AckStream;
    type SubscribeResultsStream = ResultStream;

    /// Client sends events and the server responds with an ACK for each event received.
    /// Each ACK includes the event_id from the corresponding event for explicit matching.
    #[instrument(
        name = "grpc_server::StreamEvents",
        skip(self, request),
        level = "debug"
    )]
    async fn stream_events(
        &self,
        request: Request<Streaming<Event>>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        let mut stream = request.into_inner();
        let service = self.clone();
        let events_processed = Arc::new(AtomicU64::new(0));
        let events_processed_clone = events_processed.clone();

        let (tx, rx) = mpsc::channel(256);
        let inner = Arc::downgrade(&self.inner);
        let abort_id = self.inner.next_abort_id.fetch_add(1, Ordering::Relaxed);

        // Spawn task to process incoming events
        let handle = tokio::spawn(async move {
            let mut send_final_ack = true;

            while let Some(result) = stream.next().await {
                match result {
                    Ok(event) => {
                        // Extract event_id before processing
                        let event_id = event.event_id;

                        // Process the event
                        if let Err(e) = service.process_event(event, &events_processed_clone) {
                            // Send error ACK with event_id
                            let error_ack = StreamAck {
                                success: false,
                                message: e.message().to_string(),
                                events_processed: events_processed_clone.load(Ordering::Relaxed),
                                event_id,
                            };
                            let _ = tx.send(Ok(error_ack)).await;
                            send_final_ack = false;
                            break;
                        }

                        // Send success ACK with event_id
                        let ack = StreamAck {
                            success: true,
                            message: String::new(),
                            events_processed: events_processed_clone.load(Ordering::Relaxed),
                            event_id,
                        };
                        if tx.send(Ok(ack)).await.is_err() {
                            // Client disconnected
                            send_final_ack = false;
                            break;
                        }
                    }
                    Err(e) => {
                        error!(error = ?e, "Error receiving event from stream");
                        let _ = tx.send(Err(e)).await;
                        send_final_ack = false;
                        break;
                    }
                }
            }

            if send_final_ack {
                // Send final ack on stream completion (event_id 0 indicates completion)
                let final_ack = StreamAck {
                    success: true,
                    message: "stream completed".into(),
                    events_processed: events_processed_clone.load(Ordering::Relaxed),
                    event_id: 0,
                };
                let _ = tx.send(Ok(final_ack)).await;
            }

            if let Some(inner) = inner.upgrade() {
                inner.abort_handles.lock().await.remove(&abort_id);
            }
        });

        self.inner
            .abort_handles
            .lock()
            .await
            .insert(abort_id, handle.abort_handle());

        let output_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream)))
    }

    /// Handle subscription to transaction results.
    /// Server pushes results as they complete.
    #[instrument(
        name = "grpc_server::SubscribeResults",
        skip(self, request),
        level = "debug"
    )]
    async fn subscribe_results(
        &self,
        request: Request<SubscribeResultsRequest>,
    ) -> Result<Response<Self::SubscribeResultsStream>, Status> {
        info!(target = "transport::grpc", "New results subscription");

        let req = request.into_inner();
        // Decode from_block filter (optional U256)
        let from_block: Option<U256> = req
            .from_block
            .filter(|b| !b.is_empty())
            .map(|b| decode_u256_be(&b))
            .transpose()
            .map_err(|_| Status::invalid_argument(error_messages::INVALID_BLOCK_NUMBER))?;

        // Subscribe to the broadcast channel
        let mut rx = self.inner.result_broadcaster.subscribe();

        let (tx, stream_rx) = mpsc::channel(256);

        // Spawn task to filter and forward results to the gRPC stream
        tokio::spawn(async move {
            'main: loop {
                match rx.recv().await {
                    Ok(pb_result) => {
                        // Optional: filter by block number if requested
                        if let Some(min_block) = from_block
                            && let Some(ref tx_id) = pb_result.tx_execution_id
                        {
                            // Decode the block number from the result for comparison
                            if let Ok(result_block) = decode_u256_be(&tx_id.block_number)
                                && result_block < min_block
                            {
                                continue;
                            }
                        }

                        if tx.send(Ok(pb_result)).await.is_err() {
                            // Client disconnected
                            break 'main;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(
                            target = "transport::grpc",
                            lagged = n,
                            "Result subscriber lagged, some results were dropped"
                        );
                        // Continue receiving
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Channel closed, end stream
                        break 'main;
                    }
                }
            }
        });

        let output_stream = tokio_stream::wrappers::ReceiverStream::new(stream_rx);
        Ok(Response::new(Box::pin(output_stream)))
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
        let _rpc_timer =
            RpcRequestDuration::new(concat!("sidecar_rpc_duration_", "GetTransactions"));
        self.ensure_commit_head_seen()?;

        let payload = request.into_inner();
        info!(
            target = "transport::grpc",
            method = "GetTransactions",
            count = payload.tx_execution_ids.len(),
            "GetTransactions received"
        );

        let (results, not_found) = self
            .collect_results_parallel(payload.tx_execution_ids.iter())
            .await?;

        Ok(Response::new(GetTransactionsResponse {
            results,
            not_found,
        }))
    }

    /// Handle gRPC request for `GetTransaction` messages.
    #[instrument(
        name = "grpc_server::GetTransaction",
        skip(self, request),
        level = "debug"
    )]
    async fn get_transaction(
        &self,
        request: Request<GetTransactionRequest>,
    ) -> Result<Response<GetTransactionResponse>, Status> {
        let _rpc_timer =
            RpcRequestDuration::new(concat!("sidecar_rpc_duration_", "GetTransaction"));
        self.ensure_commit_head_seen()?;

        let payload = request.into_inner();
        info!(
            target = "transport::grpc",
            method = "GetTransaction",
            tx_execution_id = ?payload.tx_execution_id,
            "GetTransaction received"
        );

        let pb_id = payload
            .tx_execution_id
            .ok_or_else(|| Status::invalid_argument(error_messages::MISSING_TX_EXECUTION_ID))?;

        let hash_bytes = pb_id.tx_hash.clone();

        let (mut results, mut not_found) = self
            .collect_results_parallel(std::iter::once(&pb_id))
            .await?;

        if let Some(result) = results.pop() {
            debug!(
                target = "transport::grpc",
                method = "GetTransaction",
                "Sending transaction result"
            );
            Ok(Response::new(GetTransactionResponse {
                outcome: Some(GetTransactionOutcome::Result(result)),
            }))
        } else {
            let not_found_hash = not_found.pop().unwrap_or(hash_bytes);
            debug!(
                target = "transport::grpc",
                method = "GetTransaction",
                "Transaction not found"
            );
            Ok(Response::new(GetTransactionResponse {
                outcome: Some(GetTransactionOutcome::NotFound(not_found_hash)),
            }))
        }
    }
}

/// Convert protobuf TransactionEnv to revm TxEnv.
#[inline]
pub fn convert_pb_tx_env_to_revm(pb_tx_env: &TransactionEnv) -> Result<TxEnv, GrpcDecodeError> {
    decode_tx_env(pb_tx_env)
}

/// Convert a Transaction to queue transaction contents.
#[inline]
pub fn to_queue_tx(t: &Transaction) -> Result<TxQueueContents, GrpcDecodeError> {
    decode_transaction(t)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TransactionsState;
    use std::{
        sync::{
            Arc,
            atomic::AtomicU64,
        },
        time::Duration,
    };

    /// Integration test: verifies that known-invalidating transactions are still
    /// queued to the engine (no early rejection). The cache is used for metrics
    /// and logging only.
    #[tokio::test]
    async fn known_invalidating_duplicate_is_still_queued_to_engine() {
        let (tx_sender, tx_receiver) = flume::unbounded();
        let transactions_state = TransactionsState::new();
        let transactions_results =
            QueryTransactionsResults::new(Arc::clone(&transactions_state), Duration::from_secs(5));

        let content_hash_cache = ContentHashCache::new(1_000);

        let service = GrpcService::new(
            tx_sender,
            transactions_results,
            None,
            100,
            content_hash_cache.clone(),
        );
        service
            .inner
            .commit_head_seen
            .store(true, std::sync::atomic::Ordering::Release);

        // First transaction - will be sent to engine
        let pb_tx_env_1 = TransactionEnv {
            tx_type: 2,
            caller: Address::repeat_byte(0x42).as_slice().to_vec(),
            gas_limit: 100_000,
            gas_price: 1u128.to_be_bytes().to_vec(),
            transact_to: Address::repeat_byte(0x99).as_slice().to_vec(),
            value: U256::from(1000u64).to_be_bytes::<32>().to_vec(),
            data: vec![0xde, 0xad, 0xbe, 0xef],
            nonce: 5, // Original nonce
            chain_id: Some(1),
            access_list: vec![],
            gas_priority_fee: Some(10u128.to_be_bytes().to_vec()),
            blob_hashes: vec![],
            max_fee_per_blob_gas: vec![],
            authorization_list: vec![],
        };

        let pb_tx_execution_id_1 = PbTxExecutionId {
            block_number: U256::from(100u64).to_be_bytes::<32>().to_vec(),
            iteration_id: 1,
            tx_hash: [0x11u8; 32].to_vec(),
            index: 0,
        };

        let tx_1 = Transaction {
            tx_execution_id: Some(pb_tx_execution_id_1.clone()),
            tx_env: Some(pb_tx_env_1.clone()),
            prev_tx_hash: None,
        };

        let event_1 = Event {
            event_id: 1,
            event: Some(EventVariant::Transaction(tx_1)),
        };

        // Process first transaction - should go to engine queue
        let events_processed = AtomicU64::new(0);
        service.process_event(event_1, &events_processed).unwrap();

        // Verify first transaction was sent to the engine queue
        let queued = tx_receiver.try_recv().expect("first tx should be queued");
        assert!(matches!(
            queued,
            crate::engine::queue::TxQueueContents::Tx(_)
        ));

        // Simulate engine processing: transaction invalidates assertions
        // Engine would call record_invalidating when !is_valid
        let decoded_tx_env = decode_tx_env(&pb_tx_env_1).unwrap();
        let content_hash = tx_content_hash(&decoded_tx_env);
        content_hash_cache.record_invalidating(content_hash, 100);

        // Verify content hash is now in cache
        assert!(
            content_hash_cache.contains(content_hash),
            "cache should contain the invalidating content hash"
        );

        // Second transaction - same content, different nonce and gas parameters
        // This simulates a user re-submitting with bumped nonce/gas
        let mut pb_tx_env_2 = pb_tx_env_1.clone();
        pb_tx_env_2.nonce = 6; // Different nonce
        pb_tx_env_2.gas_price = 2u128.to_be_bytes().to_vec(); // Different gas price
        pb_tx_env_2.gas_priority_fee = Some(20u128.to_be_bytes().to_vec()); // Different priority fee

        // Verify the content hash is the same despite different nonce/gas
        let decoded_tx_env_2 = decode_tx_env(&pb_tx_env_2).unwrap();
        let content_hash_2 = tx_content_hash(&decoded_tx_env_2);
        assert_eq!(
            content_hash, content_hash_2,
            "content hash should be identical despite different nonce/gas"
        );

        let pb_tx_execution_id_2 = PbTxExecutionId {
            block_number: U256::from(100u64).to_be_bytes::<32>().to_vec(),
            iteration_id: 1,
            tx_hash: [0x22u8; 32].to_vec(), // Different tx hash
            index: 1,
        };

        let tx_2 = Transaction {
            tx_execution_id: Some(pb_tx_execution_id_2),
            tx_env: Some(pb_tx_env_2),
            prev_tx_hash: None,
        };

        let event_2 = Event {
            event_id: 2,
            event: Some(EventVariant::Transaction(tx_2)),
        };

        // Process second transaction - should still be queued (no early rejection)
        service.process_event(event_2, &events_processed).unwrap();

        // Verify duplicate WAS sent to the engine queue (no early rejection)
        let queued_2 = tx_receiver
            .try_recv()
            .expect("known-invalidating tx should still be queued to engine");
        assert!(matches!(
            queued_2,
            crate::engine::queue::TxQueueContents::Tx(_)
        ));
    }
}
