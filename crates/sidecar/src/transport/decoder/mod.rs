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
        CommitHead as QueueCommitHead,
        NewIteration as QueueNewIteration,
        QueueBlockEnv,
        QueueTransaction,
        TxQueueContents,
    },
    execution_ids::TxExecutionId,
    transport::{
        common::HttpDecoderError,
        http::server::{
            JsonRpcRequest,
            METHOD_BLOCK_ENV,
            METHOD_REORG,
            METHOD_SEND_EVENTS,
            METHOD_SEND_TRANSACTIONS,
            SendTransactionsParams,
        },
    },
};
use revm::{
    context::BlockEnv,
    primitives::alloy_primitives::TxHash,
};
use serde::Deserialize;
use std::str::FromStr;

pub trait Decoder {
    type RawEvent: Send + Clone + Sized;
    type Error: std::error::Error + Send + Clone;

    fn to_tx_queue_contents(
        raw_event: &Self::RawEvent,
    ) -> Result<Vec<TxQueueContents>, Self::Error>;
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
                SendEvent::CommitHead { commit_head } => {
                    let queue_commit_head = convert_commit_head_event(&commit_head)?;
                    queue_events.push(TxQueueContents::CommitHead(queue_commit_head, current_span));
                }
                SendEvent::NewIteration { new_iteration } => {
                    let queue_new_iteration = convert_new_iteration_event(new_iteration);
                    queue_events.push(TxQueueContents::NewIteration(
                        queue_new_iteration,
                        current_span,
                    ));
                }
                SendEvent::Transaction { transaction } => {
                    let tx_execution_id = transaction.tx_execution_id;
                    let tx_env = transaction.tx_env;

                    queue_events.push(TxQueueContents::Tx(
                        QueueTransaction {
                            tx_execution_id,
                            tx_env,
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
            METHOD_BLOCK_ENV => {
                let params = req.params.as_ref().ok_or(HttpDecoderError::MissingParams)?;
                let block = serde_json::from_value::<QueueBlockEnv>(params.clone())
                    .map_err(|e| map_block_env_error(&e))?;
                let current_span = tracing::Span::current();
                Ok(vec![TxQueueContents::Block(block, current_span)])
            }
            METHOD_REORG => {
                let params = req.params.as_ref().ok_or(HttpDecoderError::MissingParams)?;
                let reorg = serde_json::from_value::<TxExecutionId>(params.clone())
                    .map_err(|e| HttpDecoderError::ReorgValidation(e.to_string()))?;

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
#[serde(untagged)]
enum SendEvent {
    CommitHead {
        commit_head: CommitHeadEvent,
    },
    NewIteration {
        new_iteration: NewIterationEvent,
    },
    Transaction {
        transaction: super::http::server::Transaction,
    },
}

#[derive(Debug, Deserialize)]
struct CommitHeadEvent {
    #[serde(default)]
    last_tx_hash: Option<String>,
    n_transactions: u64,
    selected_iteration_id: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct NewIterationEvent {
    iteration_id: u64,
    block_env: BlockEnv,
}

fn convert_commit_head_event(
    commit_head: &CommitHeadEvent,
) -> Result<QueueCommitHead, HttpDecoderError> {
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

    Ok(QueueCommitHead::new(
        last_tx_hash,
        commit_head.n_transactions,
        selected_iteration_id,
    ))
}

fn convert_new_iteration_event(new_iteration: NewIterationEvent) -> QueueNewIteration {
    QueueNewIteration::new(new_iteration.iteration_id, new_iteration.block_env)
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
