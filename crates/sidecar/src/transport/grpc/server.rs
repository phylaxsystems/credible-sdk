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
        http::transactions_results::QueryTransactionsResults,
        rpc_metrics::RpcRequestDuration,
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
};
use tonic::{
    Request,
    Response,
    Status,
};
use tracing::{
    Span,
    debug,
    instrument,
    warn,
};

fn parse_pb_tx_execution_id(pb: &PbTxExecutionId) -> Result<TxExecutionId, Status> {
    let tx_hash = pb
        .tx_hash
        .parse::<TxHash>()
        .map_err(|_| Status::invalid_argument("invalid tx_execution_id.tx_hash"))?;
    Ok(TxExecutionId::new(
        pb.block_number,
        pb.iteration_id,
        tx_hash,
    ))
}

fn into_pb_tx_execution_id(tx_execution_id: TxExecutionId) -> PbTxExecutionId {
    PbTxExecutionId {
        block_number: tx_execution_id.block_number,
        iteration_id: tx_execution_id.iteration_id,
        tx_hash: tx_execution_id.tx_hash_hex(),
    }
}

fn parse_pb_tx_execution_id_http(pb: &PbTxExecutionId) -> Result<TxExecutionId, HttpDecoderError> {
    let tx_hash = TxHash::from_str(&pb.tx_hash)
        .map_err(|_| HttpDecoderError::InvalidHash(pb.tx_hash.clone()))?;
    Ok(TxExecutionId::new(
        pb.block_number,
        pb.iteration_id,
        tx_hash,
    ))
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

    fn send_queue_event(
        &self,
        contents: TxQueueContents,
        item_name: &'static str,
    ) -> Result<(), Status> {
        self.tx_sender
            .send(contents)
            .map_err(|e| Status::internal(format!("failed to queue {item_name}: {e}")))
    }

    fn mark_commit_head_seen(&self) {
        self.commit_head_seen.store(true, Ordering::Release);
    }

    fn ensure_commit_head_seen(&self) -> Result<(), Status> {
        if self.commit_head_seen.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(Status::failed_precondition(
                "commit head must be received before other events",
            ))
        }
    }

    async fn fetch_transaction_result(
        &self,
        tx_execution_id: TxExecutionId,
    ) -> Result<PbTransactionResult, Status> {
        let result = match self
            .transactions_results
            .request_transaction_result(&tx_execution_id)
        {
            crate::transactions_state::RequestTransactionResult::Result(r) => r,
            crate::transactions_state::RequestTransactionResult::Channel(rx) => {
                match rx.await {
                    Ok(r) => r,
                    Err(_) => return Err(Status::internal("engine unavailable")),
                }
            }
        };

        Ok(into_pb_transaction_result(tx_execution_id, &result))
    }

    async fn collect_transaction_results<'a, I>(
        &self,
        ids: I,
    ) -> Result<(Vec<PbTransactionResult>, Vec<String>), Status>
    where
        I: IntoIterator<Item = &'a PbTxExecutionId>,
    {
        let mut iter = ids.into_iter();
        let (lower_bound, _) = iter.size_hint();
        let mut results = Vec::with_capacity(lower_bound);
        let mut not_found = Vec::new();

        for pb_tx_execution_id in iter {
            match parse_pb_tx_execution_id(pb_tx_execution_id) {
                Ok(tx_execution_id) => {
                    if self.transactions_results.is_tx_received(&tx_execution_id) {
                        results.push(self.fetch_transaction_result(tx_execution_id).await?);
                    } else {
                        not_found.push(pb_tx_execution_id.tx_hash.clone());
                    }
                }
                Err(_) => not_found.push(pb_tx_execution_id.tx_hash.clone()),
            }
        }

        Ok((results, not_found))
    }
}

fn convert_pb_commit_head(commit_head: &PbCommitHead) -> Result<CommitHead, Status> {
    let selected_iteration_id = commit_head
        .selected_iteration_id
        .ok_or_else(|| Status::invalid_argument("selected_iteration_id is required"))?;
    let last_tx_hash =
        parse_commit_metadata(&commit_head.last_tx_hash, commit_head.n_transactions)?;

    Ok(CommitHead::new(
        commit_head.block_number,
        selected_iteration_id,
        last_tx_hash,
        commit_head.n_transactions,
    ))
}

fn convert_pb_new_iteration(mut new_iteration: PbNewIteration) -> Result<NewIteration, Status> {
    let block_env_pb = new_iteration
        .block_env
        .take()
        .ok_or_else(|| Status::invalid_argument("block_env is required"))?;
    let block_env = convert_pb_block_env(&block_env_pb)?;

    Ok(NewIteration::new(new_iteration.iteration_id, block_env))
}

#[tonic::async_trait]
impl SidecarTransport for GrpcService {
    /// Handle gRPC request for batched iteration events.
    #[instrument(name = "grpc_server::SendEvents", skip(self, request), level = "debug")]
    async fn send_events(
        &self,
        request: Request<PbSendEvents>,
    ) -> Result<Response<BasicAck>, Status> {
        let mut _rpc_timer =
            RpcRequestDuration::new(concat!("sidecar_rpc_duration_", "SendEvents"));
        let mut payload = request.into_inner();
        if payload.events.is_empty() {
            return Err(Status::invalid_argument("events must not be empty"));
        }
        let single_event = payload.events.len() == 1;

        for event in payload.events {
            let Some(event_variant) = event.event else {
                return Err(Status::invalid_argument("event payload missing"));
            };

            match event_variant {
                SendEventVariant::CommitHead(commit) => {
                    let commit_head = convert_pb_commit_head(&commit)?;
                    self.send_queue_event(
                        TxQueueContents::CommitHead(commit_head, tracing::Span::current()),
                        "commit head",
                    )?;
                    self.mark_commit_head_seen();
                }
                SendEventVariant::NewIteration(iteration) => {
                    self.ensure_commit_head_seen()?;
                    let new_iteration = convert_pb_new_iteration(iteration)?;
                    self.send_queue_event(
                        TxQueueContents::NewIteration(new_iteration, tracing::Span::current()),
                        "new iteration",
                    )?;
                }
                SendEventVariant::Transaction(transaction) => {
                    self.ensure_commit_head_seen()?;
                    let queue_tx = to_queue_tx(&transaction).map_err(|e| {
                        Status::invalid_argument(format!("invalid transaction: {e}"))
                    })?;

                    if let TxQueueContents::Tx(tx, _) = &queue_tx
                        && self
                            .transactions_results
                            .is_tx_received(&tx.tx_execution_id)
                    {
                        warn!(
                            tx_hash = %tx.tx_execution_id.tx_hash_hex(),
                            "TX hash already received, skipping"
                        );
                        continue;
                    }

                    self.transactions_results.add_accepted_tx(&queue_tx);
                    if single_event {
                        if let TxQueueContents::Tx(tx, _) = &queue_tx {
                            _rpc_timer.set_tx_execution_id(&tx.tx_execution_id);
                        }
                    }
                    self.send_queue_event(queue_tx, "transaction")?;
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
        let mut _rpc_timer =
            RpcRequestDuration::new(concat!("sidecar_rpc_duration_", "SendTransactions"));
        self.ensure_commit_head_seen()?;

        let req = request.into_inner();
        let single_tx_request = req.transactions.len() == 1;
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

            if let TxQueueContents::Tx(tx, _) = &queue_tx
                && self
                    .transactions_results
                    .is_tx_received(&tx.tx_execution_id)
            {
                warn!(
                    tx_hash = %tx.tx_execution_id.tx_hash_hex(),
                    "TX hash already received, skipping"
                );
                continue;
            }

            self.transactions_results.add_accepted_tx(&queue_tx);
            if single_tx_request {
                if let TxQueueContents::Tx(tx, _) = &queue_tx {
                    _rpc_timer.set_tx_execution_id(&tx.tx_execution_id);
                }
            }
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
        let mut _rpc_timer = RpcRequestDuration::new(concat!("sidecar_rpc_duration_", "Reorg"));
        self.ensure_commit_head_seen()?;

        let payload = request.into_inner();
        let Some(pb_tx_execution_id) = payload.tx_execution_id else {
            return Err(Status::invalid_argument("missing tx_execution_id"));
        };
        let tx_execution_id = parse_pb_tx_execution_id(&pb_tx_execution_id)?;
        _rpc_timer.set_tx_execution_id(&tx_execution_id);

        let span = tracing::Span::current();
        let event = TxQueueContents::Reorg(tx_execution_id, span);
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
        let mut _rpc_timer =
            RpcRequestDuration::new(concat!("sidecar_rpc_duration_", "GetTransactions"));
        self.ensure_commit_head_seen()?;

        let payload = request.into_inner();
        if payload.tx_execution_id.len() == 1 {
            if let Some(first) = payload.tx_execution_id.first() {
                if let Ok(tx_execution_id) = parse_pb_tx_execution_id(first) {
                    _rpc_timer.set_tx_execution_id(&tx_execution_id);
                }
            }
        }
        let (results, not_found) = self
            .collect_transaction_results(payload.tx_execution_id.iter())
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
        let mut _rpc_timer =
            RpcRequestDuration::new(concat!("sidecar_rpc_duration_", "GetTransaction"));
        self.ensure_commit_head_seen()?;

        let payload = request.into_inner();

        let Some(pb_tx_execution_id) = payload.tx_execution_id else {
            return Err(Status::invalid_argument("missing tx_execution_id"));
        };

        let hash_value = pb_tx_execution_id.tx_hash.clone();
        if let Ok(tx_execution_id) = parse_pb_tx_execution_id(&pb_tx_execution_id) {
            _rpc_timer.set_tx_execution_id(&tx_execution_id);
        }

        let (mut results, mut not_found) = self
            .collect_transaction_results(std::iter::once(&pb_tx_execution_id))
            .await?;

        if let Some(result) = results.pop() {
            Ok(Response::new(GetTransactionResponse {
                outcome: Some(GetTransactionOutcome::Result(result)),
            }))
        } else {
            let not_found_hash = not_found.pop().unwrap_or(hash_value);
            Ok(Response::new(GetTransactionResponse {
                outcome: Some(GetTransactionOutcome::NotFound(not_found_hash)),
            }))
        }
    }
}

fn into_pb_transaction_result(
    tx_execution_id: TxExecutionId,
    result: &TransactionResult,
) -> PbTransactionResult {
    match result {
        TransactionResult::ValidationCompleted {
            execution_result,
            is_valid,
        } => {
            let gas_used = execution_result.gas_used();
            if !*is_valid {
                return PbTransactionResult {
                    tx_execution_id: Some(into_pb_tx_execution_id(tx_execution_id)),
                    status: "assertion_failed".into(),
                    gas_used,
                    error: String::new(),
                };
            }
            match execution_result {
                ExecutionResult::Success { .. } => {
                    PbTransactionResult {
                        tx_execution_id: Some(into_pb_tx_execution_id(tx_execution_id)),
                        status: "success".into(),
                        gas_used,
                        error: String::new(),
                    }
                }
                ExecutionResult::Revert { .. } => {
                    PbTransactionResult {
                        tx_execution_id: Some(into_pb_tx_execution_id(tx_execution_id)),
                        status: "reverted".into(),
                        gas_used,
                        error: String::new(),
                    }
                }
                ExecutionResult::Halt { reason, .. } => {
                    PbTransactionResult {
                        tx_execution_id: Some(into_pb_tx_execution_id(tx_execution_id)),
                        status: "halted".into(),
                        gas_used,
                        error: format!("Transaction halted: {reason:?}"),
                    }
                }
            }
        }
        TransactionResult::ValidationError(error) => {
            PbTransactionResult {
                tx_execution_id: Some(into_pb_tx_execution_id(tx_execution_id)),
                status: "failed".into(),
                gas_used: 0,
                error: format!("Validation error: {error}"),
            }
        }
    }
}

/// Convert a Transaction to queue transaction contents
pub fn to_queue_tx(t: &Transaction) -> Result<TxQueueContents, HttpDecoderError> {
    let tx_env =
        convert_pb_tx_env_to_revm(t.tx_env.as_ref().ok_or(HttpDecoderError::MissingTxEnv)?)?;

    let Some(pb_tx_execution_id) = t.tx_execution_id.as_ref() else {
        return Err(HttpDecoderError::MissingTxExecutionId);
    };
    let tx_execution_id = parse_pb_tx_execution_id_http(pb_tx_execution_id)?;

    Ok(TxQueueContents::Tx(
        QueueTransaction {
            tx_execution_id,
            tx_env,
        },
        tracing::Span::current(),
    ))
}

/// Convert protobuf TransactionEnv to revm TxEnv
pub fn convert_pb_tx_env_to_revm(pb_tx_env: &TransactionEnv) -> Result<TxEnv, HttpDecoderError> {
    // Parse caller address
    let caller = parse_address(&pb_tx_env.caller)
        .map_err(|_| HttpDecoderError::InvalidCaller(pb_tx_env.caller.clone()))?;

    // Parse gas price
    let gas_price = parse_u128(&pb_tx_env.gas_price)
        .map_err(|_| HttpDecoderError::InvalidGasPrice(pb_tx_env.gas_price.clone()))?;

    // Parse transaction kind (to address or create)
    let kind = parse_tx_kind(&pb_tx_env.kind)
        .map_err(|_| HttpDecoderError::InvalidKind(pb_tx_env.kind.clone()))?;

    // Parse value
    let value = parse_u256(&pb_tx_env.value).map_err(|_| HttpDecoderError::InvalidValue)?;

    // Parse data
    let data = parse_bytes(&pb_tx_env.data).map_err(|_| HttpDecoderError::InvalidData)?;

    // Parse access list
    let access_list = parse_access_list(&pb_tx_env.access_list)
        .map_err(|_| HttpDecoderError::InvalidAccessList)?;

    // Parse priority fee (optional)
    let gas_priority_fee = pb_tx_env
        .gas_priority_fee
        .as_ref()
        .filter(|s| !s.is_empty())
        .map(|s| parse_u128(s))
        .transpose()
        .map_err(|_| HttpDecoderError::InvalidGasPriorityFee)?;

    // Parse blob hashes
    let blob_hashes = parse_blob_hashes(&pb_tx_env.blob_hashes)
        .map_err(|_| HttpDecoderError::InvalidBlobHashes)?;

    // Parse max fee per blob gas
    let max_fee_per_blob_gas = parse_u128(&pb_tx_env.max_fee_per_blob_gas).map_err(|_| {
        HttpDecoderError::InvalidMaxFeePerBlobGas(pb_tx_env.max_fee_per_blob_gas.clone())
    })?;

    // Parse authorization list
    let authorization_list = parse_authorization_list(&pb_tx_env.authorization_list)
        .map_err(|_| HttpDecoderError::InvalidAuthorizationList)?;

    Ok(TxEnv {
        tx_type: u8::try_from(pb_tx_env.tx_type)
            .map_err(|_| HttpDecoderError::InvalidTxType(pb_tx_env.tx_type.to_string()))?,
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

fn parse_address(addr_str: &str) -> Result<Address, HttpDecoderError> {
    Address::from_str(addr_str).map_err(|_| HttpDecoderError::InvalidAddress(addr_str.to_string()))
}

/// Parse transaction kind from to address
fn parse_tx_kind(to_str: &str) -> Result<TxKind, HttpDecoderError> {
    if to_str.is_empty() || to_str == "0x" || to_str == "0x0" {
        Ok(TxKind::Create)
    } else {
        Ok(TxKind::Call(parse_address(to_str)?))
    }
}

/// Parse a U256 from a decimal string
fn parse_u256(value_str: &str) -> Result<U256, ParseError> {
    // Handle both decimal and hex strings
    if value_str.starts_with("0x") || value_str.starts_with("0X") {
        U256::from_str(value_str)
    } else {
        // Parse as decimal
        U256::from_str_radix(value_str, 10)
    }
}

/// Parse a u128 from a decimal string
fn parse_u128(value_str: &str) -> Result<u128, ParseIntError> {
    value_str.parse::<u128>()
}

/// Parse a U8 from a decimal string
fn parse_u8(value_str: &str) -> Result<u8, ParseIntError> {
    // Handle both decimal and hex strings
    if value_str.starts_with("0x") || value_str.starts_with("0X") {
        u8::from_str(value_str)
    } else {
        // Parse as decimal
        value_str.parse::<u8>()
    }
}

/// Parse bytes from a hex string
fn parse_bytes(data_str: &str) -> Result<Bytes, HttpDecoderError> {
    if data_str.is_empty() {
        return Ok(Bytes::new());
    }

    let hex_str = data_str.strip_prefix("0x").unwrap_or(data_str);

    hex::decode(hex_str)
        .map(Bytes::from)
        .map_err(|_| HttpDecoderError::InvalidHex(data_str.to_string()))
}

/// Parse B256 from a hex string
fn parse_b256(hash_str: &str) -> Result<B256, HttpDecoderError> {
    B256::from_str(hash_str).map_err(|_| HttpDecoderError::InvalidHex(hash_str.to_string()))
}

/// Parse access list from protobuf
fn parse_access_list(
    pb_list: &[crate::transport::grpc::pb::AccessListItem],
) -> Result<AccessList, HttpDecoderError> {
    let items = pb_list
        .iter()
        .map(|item| {
            let address = parse_address(&item.address)?;
            let storage_keys = item
                .storage_keys
                .iter()
                .map(|key| parse_b256(key))
                .collect::<Result<Vec<_>, _>>()?;

            Ok(AccessListItem {
                address,
                storage_keys,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(AccessList(items))
}

/// Parse blob hashes from string array
fn parse_blob_hashes(hashes: &[String]) -> Result<Vec<B256>, HttpDecoderError> {
    hashes.iter().map(|hash| parse_b256(hash)).collect()
}

/// Parse authorization list from protobuf
fn parse_authorization_list(
    pb_auths: &[crate::transport::grpc::pb::Authorization],
) -> Result<Vec<Either<SignedAuthorization, RecoveredAuthorization>>, HttpDecoderError> {
    pb_auths
        .iter()
        .map(|auth| {
            // Parse the address being authorized (the account giving permission)
            let address = parse_address(&auth.address)?;

            // Parse nonce
            let nonce = auth.nonce;

            // Parse chain_id
            let chain_id = parse_u256(&auth.chain_id)
                .map_err(|_| HttpDecoderError::InvalidChainId(auth.chain_id.clone()))?;

            // Parse y_parity
            let y_parity = parse_u8(&auth.y_parity)
                .map_err(|_| HttpDecoderError::InvalidYParity(auth.y_parity.clone()))?;

            // Parse signature components
            let r = parse_u256(&auth.r).map_err(|_| HttpDecoderError::InvalidSignature)?;
            let s = parse_u256(&auth.s).map_err(|_| HttpDecoderError::InvalidSignature)?;

            // Create the inner Authorization
            let inner = Authorization {
                chain_id,
                address, // The account that is authorizing
                nonce,
            };

            // Create a SignedAuthorization since we have the signature
            let signed = SignedAuthorization::new_unchecked(inner, y_parity, r, s);

            Ok(Either::Left(signed))
        })
        .collect()
}

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
                .map_err(|_| Status::invalid_argument("invalid last_tx_hash"))?,
        )
    };

    if n_transactions == 0 && parsed_hash.is_some() {
        return Err(Status::invalid_argument(
            "when n_transactions is 0, last_tx_hash must be null, empty, or missing",
        ));
    }

    if n_transactions > 0 && parsed_hash.is_none() {
        return Err(Status::invalid_argument(
            "when n_transactions > 0, last_tx_hash must be provided",
        ));
    }

    Ok(parsed_hash)
}

struct BlockEnvelope {
    block_env: RevmBlockEnv,
    last_tx_hash: Option<TxHash>,
    n_transactions: u64,
    selected_iteration_id: Option<u64>,
}

fn convert_pb_block_env(block_env: &super::pb::BlockEnv) -> Result<RevmBlockEnv, Status> {
    let beneficiary = parse_address(&block_env.beneficiary)
        .map_err(|e| Status::invalid_argument(format!("invalid beneficiary: {e}")))?;

    let difficulty = parse_u256(&block_env.difficulty)
        .map_err(|e| Status::invalid_argument(format!("invalid difficulty: {e}")))?;

    let prevrandao = match block_env.prevrandao.as_ref() {
        Some(value) if !value.is_empty() => {
            Some(
                parse_b256(value)
                    .map_err(|e| Status::invalid_argument(format!("invalid prevrandao: {e}")))?,
            )
        }
        _ => None,
    };

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

fn convert_pb_blob_excess_gas_and_price(
    blob: &super::pb::BlobExcessGasAndPrice,
) -> Result<RevmBlobExcessGasAndPrice, Status> {
    let blob_gasprice = parse_u128(&blob.blob_gasprice).map_err(|_| {
        Status::invalid_argument("invalid blob_gasprice: expected decimal string for u128")
    })?;

    Ok(RevmBlobExcessGasAndPrice {
        excess_blob_gas: blob.excess_blob_gas,
        blob_gasprice,
    })
}
