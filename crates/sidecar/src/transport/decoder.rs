//! `decoder`
//!
//! This mod contains traits and implemntations of transaction decoders.
//! Transactions arrive in various formats from transports, and its the job
//! of the decoders to convert them into events that can be passed down to
//! the core engine.

use crate::{
    engine::queue::QueueTransaction,
    transport::http::server::{
        JsonRpcRequest,
        SendTransactionsParams,
    },
};
use assertion_executor::primitives::hex;
use revm::{
    context::TxEnv,
    primitives::{
        Address,
        B256,
        Bytes,
        TxKind,
        U256,
    },
};
use std::str::FromStr;

pub trait Decoder {
    type RawEvent: Send + Clone;
    type Error: std::error::Error + Send + Clone;

    fn to_transaction(raw_event: Self::RawEvent) -> Result<QueueTransaction, Self::Error>;
    fn to_blockenv(raw_event: Self::RawEvent) -> Result<QueueTransaction, Self::Error>;
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum HttpDecoderError {
    #[error("Request not in proper schema")]
    SchemaError,
    #[error("Invalid address format: {0}")]
    InvalidAddress(String),
    #[error("Invalid hash format: {0}")]
    InvalidHash(String),
    #[error("Invalid hex value: {0}")]
    InvalidHex(String),
    #[error("Missing transaction parameters")]
    MissingParams,
    #[error("No transactions found in request")]
    NoTransactions,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct HttpTransactionDecoder;

impl Decoder for HttpTransactionDecoder {
    type RawEvent = JsonRpcRequest;
    type Error = HttpDecoderError;

    fn to_transaction(req: Self::RawEvent) -> Result<QueueTransaction, Self::Error> {
        let params = req.params.ok_or(HttpDecoderError::MissingParams)?;
        let send_params: SendTransactionsParams =
            serde_json::from_value(params).map_err(|_| HttpDecoderError::SchemaError)?;

        let transaction = send_params
            .transactions
            .into_iter()
            .next()
            .ok_or(HttpDecoderError::NoTransactions)?;

        let tx_hash = B256::from_str(&transaction.hash)
            .map_err(|_| HttpDecoderError::InvalidHash(transaction.hash.clone()))?;

        let caller = Address::from_str(&transaction.tx_env.caller)
            .map_err(|_| HttpDecoderError::InvalidAddress(transaction.tx_env.caller.clone()))?;

        let kind = if transaction.tx_env.transact_to.is_empty()
            || transaction.tx_env.transact_to == "0x"
        {
            TxKind::Create
        } else {
            let to_addr = Address::from_str(&transaction.tx_env.transact_to).map_err(|_| {
                HttpDecoderError::InvalidAddress(transaction.tx_env.transact_to.clone())
            })?;
            TxKind::Call(to_addr)
        };

        let value = U256::from_str(&transaction.tx_env.value)
            .map_err(|_| HttpDecoderError::InvalidHex(transaction.tx_env.value.clone()))?;

        let gas_price: u128 = transaction
            .tx_env
            .gas_price
            .parse()
            .map_err(|_| HttpDecoderError::InvalidHex(transaction.tx_env.gas_price.clone()))?;

        let data = if transaction.tx_env.data.starts_with("0x") {
            let hex_data = &transaction.tx_env.data[2..];
            Bytes::from(
                hex::decode(hex_data)
                    .map_err(|_| HttpDecoderError::InvalidHex(transaction.tx_env.data.clone()))?,
            )
        } else if transaction.tx_env.data.is_empty() {
            Bytes::new()
        } else {
            return Err(HttpDecoderError::InvalidHex(transaction.tx_env.data));
        };

        let tx_env = TxEnv {
            caller,
            gas_limit: transaction.tx_env.gas_limit,
            gas_price,
            kind,
            value,
            data,
            nonce: transaction.tx_env.nonce,
            chain_id: Some(transaction.tx_env.chain_id),
            access_list: vec![].into(),
            ..Default::default()
        };

        Ok(QueueTransaction { tx_hash, tx_env })
    }

    fn to_blockenv(_req: Self::RawEvent) -> Result<QueueTransaction, Self::Error> {
        unimplemented!()
    }
}
