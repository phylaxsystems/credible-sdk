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
    GetTransactionRequest,
    GetTransactionResponse,
    GetTransactionsRequest,
    GetTransactionsResponse,
    ReorgRequest,
    SendTransactionsRequest,
    SendTransactionsResponse,
    Transaction,
    TransactionEnv,
    TransactionResult as PbTransactionResult,
    TxExecutionId,
    get_transaction_response::Outcome as GetTransactionOutcome,
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
        common::HttpDecoderError,
        http::transactions_results::QueryTransactionsResults,
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
        let span = tracing::Span::current();
        trace!("Processing gRPC SendBlockEnv request");

        // Decode into proper structs instead of manually merging JSON
        let block = decode_block_env_envelope(&payload)?;
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
                    warn!(error = ?e, "Skipping invalid transaction in gRPC batch");
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
        let Some(tx_execution_id) = payload.tx_execution_id else {
            return Err(Status::invalid_argument("missing tx_execution_id"));
        };
        let hash: B256 = tx_execution_id
            .tx_hash
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

        for tx_execution_id in &payload.tx_execution_id {
            match tx_execution_id.tx_hash.parse::<TxHash>() {
                Ok(hash) => {
                    if self.transactions_results.is_tx_received(&hash) {
                        received.push((tx_execution_id.clone(), hash));
                    } else {
                        not_found.push(tx_execution_id.tx_hash.clone());
                    }
                }
                Err(_) => not_found.push(tx_execution_id.tx_hash.clone()),
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
            results.push(into_pb_transaction_result(h.tx_hash, &result));
        }

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
        trace!("Processing gRPC GetTransaction request");

        if !self.has_blockenv.load(Ordering::Relaxed) {
            debug!("Rejecting query - no block environment available");
            return Err(Status::failed_precondition(
                "block environment not available",
            ));
        }

        let payload = request.into_inner();

        let Some(tx_execution_id) = payload.tx_execution_id else {
            return Err(Status::invalid_argument("missing tx_execution_id"));
        };

        let hash_value = tx_execution_id.tx_hash;

        let Ok(parsed_hash) = hash_value.parse::<TxHash>() else {
            return Ok(Response::new(GetTransactionResponse {
                outcome: Some(GetTransactionOutcome::NotFound(hash_value)),
            }));
        };

        if !self.transactions_results.is_tx_received(&parsed_hash) {
            return Ok(Response::new(GetTransactionResponse {
                outcome: Some(GetTransactionOutcome::NotFound(hash_value)),
            }));
        }

        let result = match self
            .transactions_results
            .request_transaction_result(&parsed_hash)
        {
            crate::transactions_state::RequestTransactionResult::Result(r) => r,
            crate::transactions_state::RequestTransactionResult::Channel(rx) => {
                match rx.await {
                    Ok(r) => r,
                    Err(_) => return Err(Status::internal("engine unavailable")),
                }
            }
        };

        Ok(Response::new(GetTransactionResponse {
            outcome: Some(GetTransactionOutcome::Result(into_pb_transaction_result(
                hash_value, &result,
            ))),
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
                    tx_execution_id: Some(TxExecutionId {
                        tx_hash: hash,
                        block_number: 0,
                        iteration_id: 0,
                    }),
                    status: "assertion_failed".into(),
                    gas_used,
                    error: String::new(),
                };
            }
            match execution_result {
                ExecutionResult::Success { .. } => {
                    PbTransactionResult {
                        tx_execution_id: Some(TxExecutionId {
                            tx_hash: hash,
                            block_number: 0,
                            iteration_id: 0,
                        }),
                        status: "success".into(),
                        gas_used,
                        error: String::new(),
                    }
                }
                ExecutionResult::Revert { .. } => {
                    PbTransactionResult {
                        tx_execution_id: Some(TxExecutionId {
                            tx_hash: hash,
                            block_number: 0,
                            iteration_id: 0,
                        }),
                        status: "reverted".into(),
                        gas_used,
                        error: String::new(),
                    }
                }
                ExecutionResult::Halt { reason, .. } => {
                    PbTransactionResult {
                        tx_execution_id: Some(TxExecutionId {
                            tx_hash: hash,
                            block_number: 0,
                            iteration_id: 0,
                        }),
                        status: "halted".into(),
                        gas_used,
                        error: format!("Transaction halted: {reason:?}"),
                    }
                }
            }
        }
        TransactionResult::ValidationError(error) => {
            PbTransactionResult {
                tx_execution_id: Some(TxExecutionId {
                    tx_hash: hash,
                    block_number: 0,
                    iteration_id: 0,
                }),
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

    let Some(tx_execution_id) = t.tx_execution_id.as_ref() else {
        return Err(HttpDecoderError::MissingTxExecutionId);
    };
    let tx_hash = B256::from_str(&tx_execution_id.tx_hash)
        .map_err(|_| HttpDecoderError::InvalidHash(tx_execution_id.tx_hash.clone()))?;

    Ok(TxQueueContents::Tx(
        QueueTransaction { tx_hash, tx_env },
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

/// Decode `BlockEnvEnvelope` into a `QueueBlockEnv`.
fn decode_block_env_envelope(payload: &BlockEnvEnvelope) -> Result<QueueBlockEnv, Status> {
    let block_env_pb = payload
        .block_env
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("block_env is required"))?;
    let block_env = convert_pb_block_env(block_env_pb)?;

    // Parse optional last_tx_hash (empty string indicates absence for parity with HTTP schema)
    let last_tx_hash = if payload.last_tx_hash.is_empty() {
        None
    } else {
        Some(
            payload
                .last_tx_hash
                .parse::<TxHash>()
                .map_err(|_| Status::invalid_argument("invalid last_tx_hash"))?,
        )
    };

    // Validate invariants consistent with `QueueBlockEnv` JSON deserializer
    if payload.n_transactions == 0 && last_tx_hash.is_some() {
        return Err(Status::invalid_argument(
            "when n_transactions is 0, last_tx_hash must be null, empty, or missing",
        ));
    }
    if payload.n_transactions > 0 && last_tx_hash.is_none() {
        return Err(Status::invalid_argument(
            "when n_transactions > 0, last_tx_hash must be provided",
        ));
    }

    Ok(QueueBlockEnv {
        block_env,
        last_tx_hash,
        n_transactions: payload.n_transactions,
        selected_iteration_id: payload.selected_iteration_id,
    })
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
