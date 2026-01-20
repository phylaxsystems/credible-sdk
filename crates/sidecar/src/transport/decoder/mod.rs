//! `decoder`
//!
//! This mod contains traits and implementations of transaction decoders.
//! Transactions arrive in various formats from transports, and its the job
//! of the decoders to convert them into events that can be passed down to
//! the core engine.

#[cfg(test)]
mod tests;

use crate::{
    engine::queue::{
        CommitHead,
        NewIteration,
        QueueTransaction,
        ReorgRequest,
        TxQueueContents,
    },
    execution_ids::TxExecutionId,
    transport::{
        common::HttpDecoderError,
        http::server::{
            JsonRpcRequest,
            METHOD_REORG,
            METHOD_SEND_EVENTS,
            METHOD_SEND_TRANSACTIONS,
            SendTransactionsParams,
        },
    },
};
use alloy::primitives::{
    B256,
    U256,
};
use revm::{
    context::BlockEnv,
    primitives::alloy_primitives::TxHash,
};
use serde::Deserialize;
use serde_json::Value;
use std::str::FromStr;

pub trait Decoder {
    type RawEvent: Send + Clone + Sized;
    type Error: std::error::Error + Send + Clone;

    fn to_tx_queue_contents(
        raw_event: &Self::RawEvent,
    ) -> Result<Vec<TxQueueContents>, Self::Error>;
}

/// Parameters for a reorg request, supporting direct deserialization.
/// Note: We parse fields manually instead of using #[serde(flatten)] because
/// TxExecutionId has a custom Deserialize impl that doesn't work with flatten.
#[derive(Debug, Deserialize)]
struct ReorgParams {
    block_number: U256,
    iteration_id: u64,
    tx_hash: String,
    #[serde(default)]
    index: u64,
    /// Transaction hashes to reorg. Must be non-empty and sequential with
    /// the last entry matching tx_hash.
    tx_hashes: Vec<TxHash>,
}

/// Parses and validates reorg request parameters, returning a TxExecutionId.
fn parse_and_validate_reorg_params(
    params: &ReorgParams,
) -> Result<TxExecutionId, HttpDecoderError> {
    // Parse the tx_hash string
    let tx_hash = normalize_tx_hash(&params.tx_hash)
        .map_err(|_| HttpDecoderError::InvalidHash(params.tx_hash.clone()))?;

    // tx_hashes must be non-empty (depth is derived from its length)
    if params.tx_hashes.is_empty() {
        return Err(HttpDecoderError::ReorgValidation(
            "tx_hashes must not be empty".to_string(),
        ));
    }

    // Last entry in tx_hashes must match the tx_hash parameter
    if params.tx_hashes.last() != Some(&tx_hash) {
        return Err(HttpDecoderError::ReorgValidation(
            "tx_hashes last entry must match tx_hash".to_string(),
        ));
    }

    Ok(TxExecutionId::new(
        params.block_number,
        params.iteration_id,
        tx_hash,
        params.index,
    ))
}

/// Normalizes a transaction hash string, adding 0x prefix if missing.
fn normalize_tx_hash(value: &str) -> Result<TxHash, alloy::primitives::hex::FromHexError> {
    let trimmed = value.trim();
    let normalized = if trimmed.starts_with("0x") || trimmed.starts_with("0X") {
        trimmed.to_owned()
    } else {
        format!("0x{trimmed}")
    };
    TxHash::from_str(&normalized)
}

#[derive(Debug, Default, Clone, Copy)]
pub struct HttpTransactionDecoder;

impl HttpTransactionDecoder {
    fn to_events(req: &JsonRpcRequest) -> Result<Vec<TxQueueContents>, HttpDecoderError> {
        let params = req.params.as_ref().ok_or(HttpDecoderError::MissingParams)?;

        let send_params: SendEventsParams =
            serde_json::from_value(params.clone()).map_err(|e| map_send_events_error(&e))?;

        if send_params.events.is_empty() {
            return Err(HttpDecoderError::NoEvents);
        }

        let mut queue_events = Vec::with_capacity(send_params.events.len());

        for event in send_params.events {
            let current_span = tracing::Span::current();
            match event {
                SendEvent::CommitHead(commit_head) => {
                    let commit_head = convert_commit_head_event(&commit_head)?;
                    queue_events.push(TxQueueContents::CommitHead(commit_head, current_span));
                }
                SendEvent::NewIteration(new_iteration) => {
                    let new_iteration = convert_new_iteration_event(new_iteration);
                    queue_events.push(TxQueueContents::NewIteration(new_iteration, current_span));
                }
                SendEvent::Transaction(transaction) => {
                    let tx_execution_id = transaction.tx_execution_id;
                    let tx_env = transaction.tx_env;
                    let prev_tx_hash = transaction.prev_tx_hash;

                    queue_events.push(TxQueueContents::Tx(
                        QueueTransaction {
                            tx_execution_id,
                            tx_env,
                            prev_tx_hash,
                        },
                        current_span,
                    ));
                }
            }
        }

        Ok(queue_events)
    }

    fn to_transaction(req: &JsonRpcRequest) -> Result<Vec<TxQueueContents>, HttpDecoderError> {
        let params = req.params.as_ref().ok_or(HttpDecoderError::MissingParams)?;

        let send_params: SendTransactionsParams =
            serde_json::from_value(params.clone()).map_err(|e| {
                let msg = e.to_string();

                // Map serde errors to specific HttpDecoderError variants
                if msg.contains("missing field `transactions`") {
                    HttpDecoderError::MissingTransactionsField
                } else if msg.contains("missing field `tx_env`") {
                    HttpDecoderError::MissingTxEnv
                } else if msg.contains("missing field `hash`") {
                    HttpDecoderError::MissingHashField
                } else if msg.contains("invalid tx_execution_id") && msg.contains("invalid tx_hash")
                {
                    HttpDecoderError::InvalidHash(msg)
                } else if msg.contains("invalid tx_env") {
                    HttpDecoderError::InvalidTransaction(msg)
                } else if msg.contains("invalid hash") {
                    HttpDecoderError::InvalidHash(msg)
                } else if msg.contains("invalid transactions array") {
                    HttpDecoderError::InvalidTransaction(msg)
                } else if msg.contains("caller") || msg.contains("address") {
                    HttpDecoderError::InvalidAddress(msg)
                } else if msg.contains("value") {
                    HttpDecoderError::InvalidValue
                } else if msg.contains("data") {
                    HttpDecoderError::InvalidData
                } else if msg.contains("nonce") {
                    HttpDecoderError::InvalidNonce(msg)
                } else if msg.contains("gas_limit") {
                    HttpDecoderError::InvalidGasLimit(msg)
                } else if msg.contains("gas_price") {
                    HttpDecoderError::InvalidGasPrice(msg)
                } else if msg.contains("chain_id") {
                    HttpDecoderError::InvalidChainId(msg)
                } else {
                    HttpDecoderError::InvalidTransaction(msg)
                }
            })?;

        if send_params.transactions.is_empty() {
            return Err(HttpDecoderError::NoTransactions);
        }

        let mut queue_transactions = Vec::with_capacity(send_params.transactions.len());

        for transaction in send_params.transactions {
            let tx_execution_id = transaction.tx_execution_id;
            let current_span = tracing::Span::current();
            queue_transactions.push(TxQueueContents::Tx(
                QueueTransaction {
                    tx_execution_id,
                    tx_env: transaction.tx_env,
                    prev_tx_hash: transaction.prev_tx_hash,
                },
                current_span,
            ));
        }

        Ok(queue_transactions)
    }
}

impl Decoder for HttpTransactionDecoder {
    type RawEvent = JsonRpcRequest;
    type Error = HttpDecoderError;

    fn to_tx_queue_contents(req: &Self::RawEvent) -> Result<Vec<TxQueueContents>, Self::Error> {
        match req.method.as_str() {
            METHOD_SEND_TRANSACTIONS => Self::to_transaction(req),
            METHOD_SEND_EVENTS => Self::to_events(req),
            METHOD_REORG => {
                let params = req.params.as_ref().ok_or(HttpDecoderError::MissingParams)?;
                let reorg_params: ReorgParams = serde_json::from_value(params.clone())
                    .map_err(|e| HttpDecoderError::ReorgValidation(e.to_string()))?;

                let tx_execution_id = parse_and_validate_reorg_params(&reorg_params)?;

                let reorg = ReorgRequest {
                    tx_execution_id,
                    tx_hashes: reorg_params.tx_hashes,
                };

                let current_span = tracing::Span::current();
                Ok(vec![TxQueueContents::Reorg(reorg, current_span.clone())])
            }
            _ => {
                Err(HttpDecoderError::InvalidTransaction(
                    "unknown method".to_string(),
                ))
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct SendEventsParams {
    events: Vec<SendEvent>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SendEvent {
    CommitHead(CommitHeadEvent),
    NewIteration(NewIterationEvent),
    Transaction(super::http::server::Transaction),
}

#[derive(Debug, Deserialize)]
struct CommitHeadEvent {
    #[serde(default)]
    last_tx_hash: Option<String>,
    n_transactions: u64,
    block_number: U256,
    selected_iteration_id: Option<u64>,
    block_hash: B256,
    parent_beacon_block_root: Option<B256>,
    timestamp: U256,
}

#[derive(Debug, Deserialize)]
struct NewIterationEvent {
    iteration_id: u64,
    block_env: BlockEnv,
}

fn convert_commit_head_event(
    commit_head: &CommitHeadEvent,
) -> Result<CommitHead, HttpDecoderError> {
    let last_tx_hash = match commit_head.last_tx_hash.as_deref() {
        Some("") | None => None,
        Some(hash) => {
            Some(
                TxHash::from_str(hash)
                    .map_err(|_| HttpDecoderError::InvalidLastTxHash(hash.to_string()))?,
            )
        }
    };

    match commit_head.n_transactions {
        0 => {
            if last_tx_hash.is_some() {
                return Err(HttpDecoderError::InvalidTransaction(
                    "last_tx_hash must be null, empty, or missing when n_transactions is 0"
                        .to_string(),
                ));
            }
        }
        _ => {
            if last_tx_hash.is_none() {
                return Err(HttpDecoderError::InvalidTransaction(
                    "last_tx_hash must be provided when n_transactions > 0".to_string(),
                ));
            }
        }
    }

    let selected_iteration_id = commit_head
        .selected_iteration_id
        .ok_or(HttpDecoderError::MissingSelectedIterationId)?;

    Ok(CommitHead::new(
        commit_head.block_number,
        selected_iteration_id,
        last_tx_hash,
        commit_head.n_transactions,
        commit_head.block_hash,
        commit_head.parent_beacon_block_root,
        commit_head.timestamp,
    ))
}

fn convert_new_iteration_event(new_iteration: NewIterationEvent) -> NewIteration {
    NewIteration::new(new_iteration.iteration_id, new_iteration.block_env)
}

fn parse_block_env_payload(params: Value) -> Result<BlockEnvPayload, HttpDecoderError> {
    let Value::Object(map) = params else {
        return Err(HttpDecoderError::InvalidTransaction(
            "BlockEnv payload must be an object".to_string(),
        ));
    };

    let block_env_value = map.get("block_env").ok_or_else(|| {
        HttpDecoderError::InvalidTransaction("missing field: block_env".to_string())
    })?;

    let block_env = serde_json::from_value::<BlockEnv>(block_env_value.clone())
        .map_err(|e| map_block_env_error(&e))?;

    let last_tx_hash = match map.get("last_tx_hash") {
        Some(Value::Null) | None => None,
        Some(Value::String(s)) if s.is_empty() => None,
        Some(Value::String(s)) => {
            Some(TxHash::from_str(s).map_err(|_| HttpDecoderError::InvalidLastTxHash(s.clone()))?)
        }
        Some(other) => {
            return Err(HttpDecoderError::InvalidLastTxHash(other.to_string()));
        }
    };

    let n_transactions = match map.get("n_transactions") {
        Some(Value::Null) | None => 0,
        Some(v) => {
            serde_json::from_value::<u64>(v.clone())
                .map_err(|e| HttpDecoderError::InvalidNTransactions(e.to_string()))?
        }
    };

    let selected_iteration_id = match map.get("selected_iteration_id") {
        Some(Value::Null) | None => None,
        Some(v) => {
            Some(serde_json::from_value::<u64>(v.clone()).map_err(|e| {
                HttpDecoderError::InvalidTransaction(format!("invalid selected_iteration_id: {e}"))
            })?)
        }
    };

    match n_transactions {
        0 if last_tx_hash.is_some() => {
            return Err(HttpDecoderError::BlockEnvValidation(
                "last_tx_hash must be null, empty, or missing when n_transactions is 0".to_string(),
            ));
        }
        n if n > 0 && last_tx_hash.is_none() => {
            return Err(HttpDecoderError::BlockEnvValidation(
                "last_tx_hash must be provided when n_transactions > 0".to_string(),
            ));
        }
        _ => {}
    }

    Ok(BlockEnvPayload {
        block_env,
        last_tx_hash,
        n_transactions,
        selected_iteration_id,
    })
}

struct BlockEnvPayload {
    block_env: BlockEnv,
    last_tx_hash: Option<TxHash>,
    n_transactions: u64,
    selected_iteration_id: Option<u64>,
}

fn map_block_env_error(e: &serde_json::Error) -> HttpDecoderError {
    let msg = e.to_string();
    let msg_lower = msg.to_lowercase();

    // Check for missing fields
    if msg_lower.contains("missing field: number") {
        return HttpDecoderError::MissingBlockNumber;
    } else if msg.contains("missing field: beneficiary") {
        return HttpDecoderError::MissingBeneficiary;
    } else if msg.contains("missing field: timestamp") {
        return HttpDecoderError::MissingTimestamp;
    } else if msg.contains("missing field: gas_limit") {
        return HttpDecoderError::MissingBlockGasLimit;
    } else if msg.contains("missing field: basefee") {
        return HttpDecoderError::MissingBasefee;
    } else if msg.contains("missing field: difficulty") {
        return HttpDecoderError::MissingDifficulty;
    }

    // Check for invalid field values
    if msg_lower.contains("invalid block number") {
        return HttpDecoderError::InvalidBlockNumber(msg);
    } else if msg.contains("invalid beneficiary address") {
        return HttpDecoderError::InvalidBeneficiary(msg);
    } else if msg.contains("invalid timestamp") {
        return HttpDecoderError::InvalidTimestamp(msg);
    } else if msg.contains("invalid gas_limit") {
        return HttpDecoderError::InvalidBlockGasLimit(msg);
    } else if msg.contains("invalid basefee") {
        return HttpDecoderError::InvalidBasefee(msg);
    } else if msg.contains("invalid difficulty") {
        return HttpDecoderError::InvalidDifficulty(msg);
    } else if msg.contains("invalid prevrandao") {
        return HttpDecoderError::InvalidPrevrandao(msg);
    } else if msg.contains("invalid blob_excess_gas_and_price") {
        return HttpDecoderError::InvalidBlobExcessGas(msg);
    } else if msg.contains("invalid last_tx_hash") {
        return HttpDecoderError::InvalidLastTxHash(msg);
    } else if msg.contains("invalid n_transactions") {
        return HttpDecoderError::InvalidNTransactions(msg);
    } else if msg.contains("validation error:") {
        return HttpDecoderError::BlockEnvValidation(msg);
    }

    HttpDecoderError::BlockEnvValidation(msg)
}

fn map_send_events_error(e: &serde_json::Error) -> HttpDecoderError {
    let msg = e.to_string();

    if msg.contains("missing field `events`") {
        HttpDecoderError::MissingEventsField
    } else {
        HttpDecoderError::InvalidEvent(msg)
    }
}
