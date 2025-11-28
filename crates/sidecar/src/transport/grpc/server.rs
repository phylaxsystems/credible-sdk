//! # gRPC processing server
//!
//! This file contains helper functions and implementations for processing messages
//! generated from the protobuf definition in `./sidecar.proto`.
//!
//! These methods get called later when we receive a corresponding protobuf message
//! in the transport.

use super::pb::{
    BasicAck,
    CommitHead as PbCommitHead,
    GetTransactionRequest,
    GetTransactionResponse,
    GetTransactionsRequest,
    GetTransactionsResponse,
    NewIteration as PbNewIteration,
    ReorgRequest,
    SendEvents as PbSendEvents,
    SendTransactionsRequest,
    SendTransactionsResponse,
    Transaction,
    TransactionEnv,
    TransactionResult as PbTransactionResult,
    TxExecutionId as PbTxExecutionId,
    get_transaction_response::Outcome as GetTransactionOutcome,
    send_events::event::Event as SendEventVariant,
    sidecar_transport_server::SidecarTransport,
};
use crate::{
    engine::{
        TransactionResult,
        queue::{
            CommitHead,
            NewIteration,
            QueueTransaction,
            TransactionQueueSender,
            TxQueueContents,
        },
    },
    execution_ids::TxExecutionId,
    transport::{
        common::HttpDecoderError,
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
use metrics::histogram;
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
        ruint::ParseError,
    },
};
use std::{
    num::ParseIntError,
    str::FromStr,
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
    time::Instant,
};
use thiserror::Error;
use tonic::{
    Request,
    Response,
    Status,
};
use tracing::{
    debug,
    info,
    instrument,
    warn,
};

mod error_messages {
    pub const INVALID_TX_HASH: &str = "invalid tx_execution_id.tx_hash";
    pub const INVALID_BENEFICIARY: &str = "invalid beneficiary";
    pub const INVALID_DIFFICULTY: &str = "invalid difficulty";
    pub const INVALID_PREVRANDAO: &str = "invalid prevrandao";
    pub const INVALID_BLOB_GASPRICE: &str = "invalid blob_gasprice";
    pub const MISSING_TX_EXECUTION_ID: &str = "missing tx_execution_id";
    pub const SELECTED_ITERATION_REQUIRED: &str = "selected_iteration_id is required";
    pub const BLOCK_ENV_REQUIRED: &str = "block_env is required";
    pub const EVENTS_EMPTY: &str = "events must not be empty";
    pub const EVENT_PAYLOAD_MISSING: &str = "event payload missing";
    pub const COMMIT_HEAD_REQUIRED: &str = "commit head must be received before other events";
    pub const INVALID_LAST_TX_HASH: &str = "invalid last_tx_hash";
    pub const N_TX_ZERO_HASH_PRESENT: &str =
        "when n_transactions is 0, last_tx_hash must be null, empty, or missing";
    pub const N_TX_POSITIVE_HASH_MISSING: &str =
        "when n_transactions > 0, last_tx_hash must be provided";
    pub const QUEUE_TX_FAILED: &str = "failed to queue transaction";
    pub const QUEUE_REORG_FAILED: &str = "failed to queue reorg";
}

/// Status string constants to avoid repeated heap allocations
mod status_strings {
    pub const SUCCESS: &str = "success";
    pub const REVERTED: &str = "reverted";
    pub const HALTED: &str = "halted";
    pub const FAILED: &str = "failed";
    pub const ASSERTION_FAILED: &str = "assertion_failed";
}

// Optimized error type that avoids heap allocations for common error cases.
// Only stores minimal metadata needed to reconstruct the error if necessary.
#[derive(Debug, Clone, Error)]
pub enum FastHttpDecoderError {
    #[error("missing tx_env")]
    MissingTxEnv,
    #[error("missing tx_execution_id")]
    MissingTxExecutionId,
    #[error("invalid data")]
    InvalidData,
    #[error("invalid value")]
    InvalidValue,
    #[error("invalid access_list")]
    InvalidAccessList,
    #[error("invalid blob_hashes")]
    InvalidBlobHashes,
    #[error("invalid authorization_list")]
    InvalidAuthorizationList,
    #[error("invalid signature")]
    InvalidSignature,
    #[error("invalid gas_priority_fee")]
    InvalidGasPriorityFee,
    #[error("invalid max_fee_per_blob_gas")]
    InvalidMaxFeePerBlobGas,
    #[error("invalid chain_id")]
    InvalidChainId,
    #[error("invalid y_parity")]
    InvalidYParity,
    #[error("invalid hash (len={input_len})")]
    InvalidHash { input_len: usize },
    #[error("invalid address (len={input_len})")]
    InvalidAddress { input_len: usize },
    #[error("invalid caller (len={input_len})")]
    InvalidCaller { input_len: usize },
    #[error("invalid gas_price (len={input_len})")]
    InvalidGasPrice { input_len: usize },
    #[error("invalid kind (len={input_len})")]
    InvalidKind { input_len: usize },
    #[error("invalid tx_type: {0}")]
    InvalidTxType(u32),
    #[error("invalid hex (len={input_len})")]
    InvalidHex { input_len: usize },
}

impl FastHttpDecoderError {
    #[inline]
    const fn invalid_hash(s: &str) -> Self {
        Self::InvalidHash { input_len: s.len() }
    }

    #[inline]
    const fn invalid_hex(s: &str) -> Self {
        Self::InvalidHex { input_len: s.len() }
    }
}

impl From<FastHttpDecoderError> for HttpDecoderError {
    fn from(e: FastHttpDecoderError) -> Self {
        match e {
            FastHttpDecoderError::MissingTxEnv => HttpDecoderError::MissingTxEnv,
            FastHttpDecoderError::MissingTxExecutionId => HttpDecoderError::MissingTxExecutionId,
            FastHttpDecoderError::InvalidData => HttpDecoderError::InvalidData,
            FastHttpDecoderError::InvalidValue => HttpDecoderError::InvalidValue,
            FastHttpDecoderError::InvalidAccessList => HttpDecoderError::InvalidAccessList,
            FastHttpDecoderError::InvalidBlobHashes => HttpDecoderError::InvalidBlobHashes,
            FastHttpDecoderError::InvalidAuthorizationList => {
                HttpDecoderError::InvalidAuthorizationList
            }
            FastHttpDecoderError::InvalidSignature => HttpDecoderError::InvalidSignature,
            FastHttpDecoderError::InvalidGasPriorityFee => HttpDecoderError::InvalidGasPriorityFee,
            FastHttpDecoderError::InvalidMaxFeePerBlobGas => {
                HttpDecoderError::InvalidMaxFeePerBlobGas(String::new())
            }
            FastHttpDecoderError::InvalidChainId => HttpDecoderError::InvalidChainId(String::new()),
            FastHttpDecoderError::InvalidYParity => HttpDecoderError::InvalidYParity(String::new()),
            FastHttpDecoderError::InvalidHash { .. } => {
                HttpDecoderError::InvalidHash(String::new())
            }
            FastHttpDecoderError::InvalidAddress { .. } => {
                HttpDecoderError::InvalidAddress(String::new())
            }
            FastHttpDecoderError::InvalidCaller { .. } => {
                HttpDecoderError::InvalidCaller(String::new())
            }
            FastHttpDecoderError::InvalidGasPrice { .. } => {
                HttpDecoderError::InvalidGasPrice(String::new())
            }
            FastHttpDecoderError::InvalidKind { .. } => {
                HttpDecoderError::InvalidKind(String::new())
            }
            FastHttpDecoderError::InvalidTxType(t) => {
                HttpDecoderError::InvalidTxType(t.to_string())
            }
            FastHttpDecoderError::InvalidHex { .. } => HttpDecoderError::InvalidHex(String::new()),
        }
    }
}

#[inline]
fn parse_pb_tx_execution_id(pb: &PbTxExecutionId) -> Result<TxExecutionId, Status> {
    let tx_hash = pb
        .tx_hash
        .parse::<TxHash>()
        .map_err(|_| Status::invalid_argument(error_messages::INVALID_TX_HASH))?;
    Ok(TxExecutionId::new(
        pb.block_number,
        pb.iteration_id,
        tx_hash,
        pb.index,
    ))
}

#[inline]
fn into_pb_tx_execution_id(tx_execution_id: TxExecutionId) -> PbTxExecutionId {
    PbTxExecutionId {
        block_number: tx_execution_id.block_number,
        iteration_id: tx_execution_id.iteration_id,
        tx_hash: tx_execution_id.tx_hash_hex(),
        index: tx_execution_id.index,
    }
}

#[inline]
fn parse_pb_tx_execution_id_fast(
    pb: &PbTxExecutionId,
) -> Result<TxExecutionId, FastHttpDecoderError> {
    let tx_hash = TxHash::from_str(&pb.tx_hash)
        .map_err(|_| FastHttpDecoderError::invalid_hash(&pb.tx_hash))?;
    Ok(TxExecutionId::new(
        pb.block_number,
        pb.iteration_id,
        tx_hash,
        pb.index,
    ))
}

#[inline]
fn parse_address(addr_str: &str) -> Result<Address, FastHttpDecoderError> {
    Address::from_str(addr_str).map_err(|_| {
        FastHttpDecoderError::InvalidAddress {
            input_len: addr_str.len(),
        }
    })
}

#[inline]
fn parse_tx_kind(to_str: &str) -> Result<TxKind, FastHttpDecoderError> {
    if to_str.is_empty() || to_str == "0x" || to_str == "0x0" {
        Ok(TxKind::Create)
    } else {
        Ok(TxKind::Call(parse_address(to_str)?))
    }
}

/// Parse a U256 from a decimal or hex string.
#[inline]
fn parse_u256(value_str: &str) -> Result<U256, ParseError> {
    let bytes = value_str.as_bytes();
    if bytes.len() >= 2 && bytes[0] == b'0' && (bytes[1] | 0x20) == b'x' {
        U256::from_str(value_str)
    } else {
        U256::from_str_radix(value_str, 10)
    }
}

#[inline]
fn parse_u128(value_str: &str) -> Result<u128, ParseIntError> {
    value_str.parse::<u128>()
}

/// Parse an u8 from a decimal or hex string.
#[inline]
fn parse_u8(value_str: &str) -> Result<u8, ParseIntError> {
    let bytes = value_str.as_bytes();
    if bytes.len() >= 2 && bytes[0] == b'0' && (bytes[1] | 0x20) == b'x' {
        u8::from_str_radix(&value_str[2..], 16)
    } else {
        value_str.parse::<u8>()
    }
}

/// Parse bytes from a hex string with optimized allocation.
#[inline]
fn parse_bytes(data_str: &str) -> Result<Bytes, FastHttpDecoderError> {
    if data_str.is_empty() {
        return Ok(Bytes::new());
    }

    let hex_str = data_str.strip_prefix("0x").unwrap_or(data_str);
    if hex_str.is_empty() {
        return Ok(Bytes::new());
    }

    // Pre-allocate the exact size needed
    let len = hex_str.len() / 2;
    let mut buf = vec![0u8; len];
    hex::decode_to_slice(hex_str, &mut buf)
        .map_err(|_| FastHttpDecoderError::invalid_hex(data_str))?;
    Ok(Bytes::from(buf))
}

#[inline]
fn parse_b256(hash_str: &str) -> Result<B256, FastHttpDecoderError> {
    B256::from_str(hash_str).map_err(|_| FastHttpDecoderError::invalid_hex(hash_str))
}

#[inline]
fn parse_access_list(
    pb_list: &[crate::transport::grpc::pb::AccessListItem],
) -> Result<AccessList, FastHttpDecoderError> {
    pb_list
        .iter()
        .map(|item| {
            let address = parse_address(&item.address)?;
            let storage_keys: Vec<B256> = item
                .storage_keys
                .iter()
                .map(|key| parse_b256(key))
                .collect::<Result<_, _>>()?;
            Ok(AccessListItem {
                address,
                storage_keys,
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(AccessList)
}

#[inline]
fn parse_blob_hashes(hashes: &[String]) -> Result<Vec<B256>, FastHttpDecoderError> {
    hashes.iter().map(|h| parse_b256(h)).collect()
}

fn parse_authorization_list(
    pb_auths: &[crate::transport::grpc::pb::Authorization],
) -> Result<Vec<Either<SignedAuthorization, RecoveredAuthorization>>, FastHttpDecoderError> {
    pb_auths
        .iter()
        .map(|auth| {
            let address = parse_address(&auth.address)?;
            let chain_id =
                parse_u256(&auth.chain_id).map_err(|_| FastHttpDecoderError::InvalidChainId)?;
            let y_parity =
                parse_u8(&auth.y_parity).map_err(|_| FastHttpDecoderError::InvalidYParity)?;
            let r = parse_u256(&auth.r).map_err(|_| FastHttpDecoderError::InvalidSignature)?;
            let s = parse_u256(&auth.s).map_err(|_| FastHttpDecoderError::InvalidSignature)?;

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

#[inline]
fn parse_commit_metadata(
    last_tx_hash_raw: &str,
    n_transactions: u64,
) -> Result<Option<TxHash>, Status> {
    let parsed_hash = if last_tx_hash_raw.is_empty() {
        None
    } else {
        Some(
            last_tx_hash_raw
                .parse::<TxHash>()
                .map_err(|_| Status::invalid_argument(error_messages::INVALID_LAST_TX_HASH))?,
        )
    };

    if n_transactions == 0 && parsed_hash.is_some() {
        return Err(Status::invalid_argument(
            error_messages::N_TX_ZERO_HASH_PRESENT,
        ));
    }

    if n_transactions > 0 && parsed_hash.is_none() {
        return Err(Status::invalid_argument(
            error_messages::N_TX_POSITIVE_HASH_MISSING,
        ));
    }

    Ok(parsed_hash)
}

#[inline]
fn convert_pb_commit_head(commit_head: &PbCommitHead) -> Result<CommitHead, Status> {
    let selected_iteration_id = commit_head
        .selected_iteration_id
        .ok_or_else(|| Status::invalid_argument(error_messages::SELECTED_ITERATION_REQUIRED))?;
    let last_tx_hash =
        parse_commit_metadata(&commit_head.last_tx_hash, commit_head.n_transactions)?;

    Ok(CommitHead::new(
        commit_head.block_number,
        selected_iteration_id,
        last_tx_hash,
        commit_head.n_transactions,
    ))
}

#[inline]
fn convert_pb_new_iteration(new_iteration: PbNewIteration) -> Result<NewIteration, Status> {
    let block_env_pb = new_iteration
        .block_env
        .ok_or_else(|| Status::invalid_argument(error_messages::BLOCK_ENV_REQUIRED))?;
    let block_env = convert_pb_block_env(block_env_pb)?;

    Ok(NewIteration::new(new_iteration.iteration_id, block_env))
}

#[inline]
fn convert_pb_block_env(block_env: super::pb::BlockEnv) -> Result<RevmBlockEnv, Status> {
    let beneficiary = parse_address(&block_env.beneficiary)
        .map_err(|_| Status::invalid_argument(error_messages::INVALID_BENEFICIARY))?;

    let difficulty = parse_u256(&block_env.difficulty)
        .map_err(|_| Status::invalid_argument(error_messages::INVALID_DIFFICULTY))?;

    let prevrandao = block_env
        .prevrandao
        .filter(|v| !v.is_empty())
        .map(|v| parse_b256(&v))
        .transpose()
        .map_err(|_| Status::invalid_argument(error_messages::INVALID_PREVRANDAO))?;

    let blob_excess_gas_and_price = block_env
        .blob_excess_gas_and_price
        .as_ref()
        .map(convert_pb_blob_excess_gas_and_price)
        .transpose()?;

    Ok(RevmBlockEnv {
        number: block_env.number,
        beneficiary,
        timestamp: block_env.timestamp,
        gas_limit: block_env.gas_limit,
        basefee: block_env.basefee,
        difficulty,
        prevrandao,
        blob_excess_gas_and_price,
    })
}

#[inline]
fn convert_pb_blob_excess_gas_and_price(
    blob: &super::pb::BlobExcessGasAndPrice,
) -> Result<RevmBlobExcessGasAndPrice, Status> {
    let blob_gasprice = parse_u128(&blob.blob_gasprice)
        .map_err(|_| Status::invalid_argument(error_messages::INVALID_BLOB_GASPRICE))?;

    Ok(RevmBlobExcessGasAndPrice {
        excess_blob_gas: blob.excess_blob_gas,
        blob_gasprice,
    })
}

/// Convert a Transaction to queue transaction contents.
#[inline]
pub fn to_queue_tx(t: &Transaction) -> Result<TxQueueContents, HttpDecoderError> {
    let tx_env =
        convert_pb_tx_env_to_revm_fast(t.tx_env.as_ref().ok_or(HttpDecoderError::MissingTxEnv)?)
            .map_err(HttpDecoderError::from)?;

    let pb_tx_execution_id = t
        .tx_execution_id
        .as_ref()
        .ok_or(HttpDecoderError::MissingTxExecutionId)?;

    let tx_execution_id =
        parse_pb_tx_execution_id_fast(pb_tx_execution_id).map_err(HttpDecoderError::from)?;

    let prev_tx_hash = t
        .prev_tx_hash
        .as_ref()
        .map(|prev_tx_hash| {
            TxHash::from_str(prev_tx_hash)
                .map_err(|_| HttpDecoderError::InvalidHash(prev_tx_hash.clone()))
        })
        .transpose()?;

    Ok(TxQueueContents::Tx(
        QueueTransaction {
            tx_execution_id,
            tx_env,
            prev_tx_hash,
        },
        tracing::Span::current(),
    ))
}

/// Convert protobuf TransactionEnv to revm TxEnv using optimized parsing.
pub fn convert_pb_tx_env_to_revm_fast(
    pb_tx_env: &TransactionEnv,
) -> Result<TxEnv, FastHttpDecoderError> {
    let caller = parse_address(&pb_tx_env.caller).map_err(|_| {
        FastHttpDecoderError::InvalidCaller {
            input_len: pb_tx_env.caller.len(),
        }
    })?;

    let gas_price = parse_u128(&pb_tx_env.gas_price).map_err(|_| {
        FastHttpDecoderError::InvalidGasPrice {
            input_len: pb_tx_env.gas_price.len(),
        }
    })?;

    let kind = parse_tx_kind(&pb_tx_env.kind).map_err(|_| {
        FastHttpDecoderError::InvalidKind {
            input_len: pb_tx_env.kind.len(),
        }
    })?;

    let value = parse_u256(&pb_tx_env.value).map_err(|_| FastHttpDecoderError::InvalidValue)?;
    let data = parse_bytes(&pb_tx_env.data)?;

    let access_list = parse_access_list(&pb_tx_env.access_list)
        .map_err(|_| FastHttpDecoderError::InvalidAccessList)?;

    let gas_priority_fee = pb_tx_env
        .gas_priority_fee
        .as_ref()
        .filter(|s| !s.is_empty())
        .map(|s| parse_u128(s))
        .transpose()
        .map_err(|_| FastHttpDecoderError::InvalidGasPriorityFee)?;

    let blob_hashes = parse_blob_hashes(&pb_tx_env.blob_hashes)
        .map_err(|_| FastHttpDecoderError::InvalidBlobHashes)?;

    let max_fee_per_blob_gas = parse_u128(&pb_tx_env.max_fee_per_blob_gas)
        .map_err(|_| FastHttpDecoderError::InvalidMaxFeePerBlobGas)?;

    let authorization_list = parse_authorization_list(&pb_tx_env.authorization_list)
        .map_err(|_| FastHttpDecoderError::InvalidAuthorizationList)?;

    Ok(TxEnv {
        tx_type: u8::try_from(pb_tx_env.tx_type)
            .map_err(|_| FastHttpDecoderError::InvalidTxType(pb_tx_env.tx_type))?,
        caller,
        gas_limit: pb_tx_env.gas_limit,
        gas_price,
        kind,
        value,
        data,
        nonce: pb_tx_env.nonce,
        chain_id: pb_tx_env.chain_id,
        access_list,
        gas_priority_fee,
        blob_hashes,
        max_fee_per_blob_gas,
        authorization_list,
    })
}

#[inline]
fn into_pb_transaction_result(
    tx_execution_id: TxExecutionId,
    result: &TransactionResult,
) -> PbTransactionResult {
    let pb_tx_id = Some(into_pb_tx_execution_id(tx_execution_id));

    match result {
        TransactionResult::ValidationCompleted {
            execution_result,
            is_valid,
        } => {
            let gas_used = execution_result.gas_used();
            if !*is_valid {
                return PbTransactionResult {
                    tx_execution_id: pb_tx_id,
                    status: status_strings::ASSERTION_FAILED.into(),
                    gas_used,
                    error: String::new(),
                };
            }
            match execution_result {
                ExecutionResult::Success { .. } => {
                    PbTransactionResult {
                        tx_execution_id: pb_tx_id,
                        status: status_strings::SUCCESS.into(),
                        gas_used,
                        error: String::new(),
                    }
                }
                ExecutionResult::Revert { .. } => {
                    PbTransactionResult {
                        tx_execution_id: pb_tx_id,
                        status: status_strings::REVERTED.into(),
                        gas_used,
                        error: String::new(),
                    }
                }
                ExecutionResult::Halt { reason, .. } => {
                    PbTransactionResult {
                        tx_execution_id: pb_tx_id,
                        status: status_strings::HALTED.into(),
                        gas_used,
                        error: format!("Transaction halted: {reason:?}"),
                    }
                }
            }
        }
        TransactionResult::ValidationError(error) => {
            PbTransactionResult {
                tx_execution_id: pb_tx_id,
                status: status_strings::FAILED.into(),
                gas_used: 0,
                error: format!("Validation error: {error}"),
            }
        }
    }
}

#[derive(Clone)]
pub struct GrpcService {
    tx_sender: TransactionQueueSender,
    transactions_results: QueryTransactionsResults,
    commit_head_seen: Arc<AtomicBool>,
}

impl GrpcService {
    pub fn new(
        tx_sender: TransactionQueueSender,
        transactions_results: QueryTransactionsResults,
    ) -> Self {
        Self {
            tx_sender,
            transactions_results,
            commit_head_seen: Arc::new(AtomicBool::new(false)),
        }
    }

    #[inline]
    fn send_queue_event(&self, contents: TxQueueContents) -> Result<(), Status> {
        self.tx_sender
            .send(contents)
            .map_err(|_| Status::internal(error_messages::QUEUE_TX_FAILED))
    }

    #[inline]
    fn mark_commit_head_seen(&self) {
        self.commit_head_seen.store(true, Ordering::Release);
    }

    #[inline]
    fn ensure_commit_head_seen(&self) -> Result<(), Status> {
        if self.commit_head_seen.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(Status::failed_precondition(
                error_messages::COMMIT_HEAD_REQUIRED,
            ))
        }
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

        let pb_result = into_pb_transaction_result(tx_execution_id, &result);
        histogram!("sidecar_fetch_transaction_result_duration").record(fetch_started_at.elapsed());

        Ok(pb_result)
    }

    /// Collect transaction results with parallel fetching.
    /// Optimized to minimize clones - only clones hash strings when actually needed for errors.
    async fn collect_transaction_results_parallel<'a, I>(
        &self,
        ids: I,
    ) -> Result<(Vec<PbTransactionResult>, Vec<String>), Status>
    where
        I: IntoIterator<Item = &'a PbTxExecutionId>,
    {
        let iter = ids.into_iter();
        let (lower_bound, _) = iter.size_hint();

        // Phase 1: Parse all IDs and categorize
        // Store references to avoid cloning until necessary
        let mut ready_ids = Vec::with_capacity(lower_bound);
        let mut waiters: Vec<(TxExecutionId, &'a str, _)> = Vec::new();
        let mut not_found = Vec::new();

        for pb_tx_execution_id in iter {
            match parse_pb_tx_execution_id(pb_tx_execution_id) {
                Ok(tx_execution_id) => {
                    match self.transactions_results.is_tx_received(&tx_execution_id) {
                        AcceptedState::Yes => ready_ids.push(tx_execution_id),
                        AcceptedState::NotYet(rx) => {
                            info!(
                                target = "transport::grpc",
                                method = "GetTransactions",
                                tx_execution_id = ?tx_execution_id,
                                "Transaction not yet received, waiting for it"
                            );
                            // Store reference to hash, only clone if tx not found later
                            waiters.push((tx_execution_id, &pb_tx_execution_id.tx_hash, rx));
                        }
                    }
                }
                // Only clone on parse error
                Err(_) => not_found.push(pb_tx_execution_id.tx_hash.clone()),
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
                .map(|(id, hash, rx)| ((id, hash.to_owned()), rx))
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
    /// Handle gRPC request for batched iteration events.
    #[instrument(name = "grpc_server::SendEvents", skip(self, request), level = "debug")]
    async fn send_events(
        &self,
        request: Request<PbSendEvents>,
    ) -> Result<Response<BasicAck>, Status> {
        let mut rpc_timer = None;
        let payload = request.into_inner();

        if payload.events.is_empty() {
            return Err(Status::invalid_argument(error_messages::EVENTS_EMPTY));
        }

        for event in payload.events {
            let event_variant = event
                .event
                .ok_or_else(|| Status::invalid_argument(error_messages::EVENT_PAYLOAD_MISSING))?;

            match event_variant {
                SendEventVariant::CommitHead(commit) => {
                    let commit_head = convert_pb_commit_head(&commit)?;
                    self.send_queue_event(TxQueueContents::CommitHead(
                        commit_head,
                        tracing::Span::current(),
                    ))?;
                    self.mark_commit_head_seen();
                }
                SendEventVariant::NewIteration(iteration) => {
                    self.ensure_commit_head_seen()?;
                    let new_iteration = convert_pb_new_iteration(iteration)?;
                    self.send_queue_event(TxQueueContents::NewIteration(
                        new_iteration,
                        tracing::Span::current(),
                    ))?;
                }
                SendEventVariant::Transaction(transaction) => {
                    self.ensure_commit_head_seen()?;
                    let queue_tx = to_queue_tx(&transaction).map_err(|e| {
                        Status::invalid_argument(format!("invalid transaction: {e}"))
                    })?;

                    if let TxQueueContents::Tx(ref tx, _) = queue_tx
                        && self
                            .transactions_results
                            .is_tx_received_now(&tx.tx_execution_id)
                    {
                        warn!(
                            tx_hash = %tx.tx_execution_id.tx_hash_hex(),
                            "TX hash already received, skipping"
                        );
                        continue;
                    }

                    self.transactions_results.add_accepted_tx(&queue_tx);
                    if matches!(&queue_tx, TxQueueContents::Tx(_, _)) {
                        rpc_timer.get_or_insert_with(|| {
                            RpcRequestDuration::new(concat!("sidecar_rpc_duration_", "SendEvents"))
                        });
                    }
                    self.send_queue_event(queue_tx)?;
                }
            }
        }

        Ok(Response::new(BasicAck {
            accepted: true,
            message: "events processed".into(),
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
        let _rpc_timer =
            RpcRequestDuration::new(concat!("sidecar_rpc_duration_", "SendTransactions"));
        self.ensure_commit_head_seen()?;

        let req = request.into_inner();
        debug!(
            target = "transport::grpc",
            method = "SendTransactions",
            req = ?req,
            "SendTransactions received"
        );

        let total = req.transactions.len() as u64;
        let mut accepted: u64 = 0;

        for t in req.transactions {
            let queue_tx = match to_queue_tx(&t) {
                Ok(tx) => tx,
                Err(e) => {
                    warn!(error = ?e, "Skipping invalid transaction in gRPC batch");
                    continue;
                }
            };

            if let TxQueueContents::Tx(ref tx, _) = queue_tx
                && self
                    .transactions_results
                    .is_tx_received_now(&tx.tx_execution_id)
            {
                warn!(
                    tx_hash = %tx.tx_execution_id.tx_hash_hex(),
                    "TX hash already received, skipping"
                );
                continue;
            }

            self.transactions_results.add_accepted_tx(&queue_tx);
            self.tx_sender
                .send(queue_tx)
                .map_err(|_| Status::internal(error_messages::QUEUE_TX_FAILED))?;
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
        let _rpc_timer = RpcRequestDuration::new(concat!("sidecar_rpc_duration_", "Reorg"));
        self.ensure_commit_head_seen()?;

        let payload = request.into_inner();
        let pb_tx_execution_id = payload
            .tx_execution_id
            .ok_or_else(|| Status::invalid_argument(error_messages::MISSING_TX_EXECUTION_ID))?;

        let tx_execution_id = parse_pb_tx_execution_id(&pb_tx_execution_id)?;

        let event = TxQueueContents::Reorg(tx_execution_id, tracing::Span::current());
        self.transactions_results.add_accepted_tx(&event);
        self.tx_sender
            .send(event)
            .map_err(|_| Status::internal(error_messages::QUEUE_REORG_FAILED))?;

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
        let _rpc_timer =
            RpcRequestDuration::new(concat!("sidecar_rpc_duration_", "GetTransactions"));
        self.ensure_commit_head_seen()?;

        let payload = request.into_inner();
        info!(
            target = "transport::grpc",
            method = "GetTransactions",
            tx_execution_id = ?payload.tx_execution_id,
            "GetTransactions received"
        );

        let (results, not_found) = self
            .collect_transaction_results_parallel(payload.tx_execution_id.iter())
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

        let pb_tx_execution_id = payload
            .tx_execution_id
            .ok_or_else(|| Status::invalid_argument(error_messages::MISSING_TX_EXECUTION_ID))?;

        let hash_value = pb_tx_execution_id.tx_hash.clone();

        let (mut results, mut not_found) = self
            .collect_transaction_results_parallel(std::iter::once(&pb_tx_execution_id))
            .await?;

        if let Some(result) = results.pop() {
            debug!(
                target = "transport::grpc",
                method = "GetTransaction",
                tx_execution_id = ?pb_tx_execution_id,
                "Sending transaction result"
            );
            Ok(Response::new(GetTransactionResponse {
                outcome: Some(GetTransactionOutcome::Result(result)),
            }))
        } else {
            let not_found_hash = not_found.pop().unwrap_or(hash_value);
            debug!(
                target = "transport::grpc",
                method = "GetTransaction",
                tx_execution_id = ?pb_tx_execution_id,
                "Transaction not found"
            );
            Ok(Response::new(GetTransactionResponse {
                outcome: Some(GetTransactionOutcome::NotFound(not_found_hash)),
            }))
        }
    }
}

/// Convert protobuf TransactionEnv to revm TxEnv.
/// This is the legacy interface that returns HttpDecoderError.
#[inline]
pub fn convert_pb_tx_env_to_revm(pb_tx_env: &TransactionEnv) -> Result<TxEnv, HttpDecoderError> {
    convert_pb_tx_env_to_revm_fast(pb_tx_env).map_err(HttpDecoderError::from)
}
