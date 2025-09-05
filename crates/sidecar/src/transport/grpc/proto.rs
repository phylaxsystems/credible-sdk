//! Generated protobuf code for gRPC transport

// Include the generated protobuf code
tonic::include_proto!("sidecar");

use crate::{
    engine::queue::{QueueTransaction, TxQueueContents},
    transport::decoder::{Decoder, HttpDecoderError},
};
use revm::{
    context::{BlockEnv, TxEnv},
    primitives::{Address, B256, Bytes, TxKind, U256},
};
use std::str::FromStr;

/// Convert protobuf TransactionEnv to revm TxEnv
impl TryFrom<&TransactionEnv> for TxEnv {
    type Error = HttpDecoderError;

    fn try_from(tx_env: &TransactionEnv) -> Result<Self, Self::Error> {
        let caller = Address::from_str(&tx_env.caller)
            .map_err(|_| HttpDecoderError::InvalidAddress(tx_env.caller.clone()))?;

        let gas_price: u128 = tx_env
            .gas_price
            .parse()
            .map_err(|_| HttpDecoderError::InvalidHex(tx_env.gas_price.clone()))?;

        let kind = parse_tx_kind(tx_env.transact_to.as_deref())?;

        let value = U256::from_str(&tx_env.value)
            .map_err(|_| HttpDecoderError::InvalidHex(tx_env.value.clone()))?;

        let data = parse_hex_data(&tx_env.data)?;

        Ok(Self {
            caller,
            gas_limit: tx_env.gas_limit,
            gas_price,
            kind,
            value,
            data,
            nonce: tx_env.nonce,
            chain_id: Some(tx_env.chain_id),
            ..Default::default()
        })
    }
}

/// Convert protobuf SendBlockEnvRequest to revm BlockEnv
impl TryFrom<&SendBlockEnvRequest> for BlockEnv {
    type Error = HttpDecoderError;

    fn try_from(block_env: &SendBlockEnvRequest) -> Result<Self, Self::Error> {
        let coinbase = Address::from_str(&block_env.coinbase)
            .map_err(|_| HttpDecoderError::InvalidAddress(block_env.coinbase.clone()))?;

        let difficulty = if let Some(diff_str) = &block_env.difficulty {
            U256::from_str(diff_str)
                .map_err(|_| HttpDecoderError::InvalidHex(diff_str.clone()))?
        } else {
            U256::ZERO
        };

        let prevrandao = if let Some(randao_str) = &block_env.prevrandao {
            Some(B256::from_str(randao_str)
                .map_err(|_| HttpDecoderError::InvalidHex(randao_str.clone()))?)
        } else {
            None
        };

        Ok(Self {
            number: block_env.number,
            beneficiary: coinbase,
            timestamp: block_env.timestamp.unwrap_or(0),
            difficulty,
            prevrandao,
            basefee: block_env.basefee,
            gas_limit: block_env.gas_limit.unwrap_or(30_000_000),
            ..Default::default()
        })
    }
}

/// Helper functions (reused from HTTP decoder)
fn parse_tx_kind(transact_to: Option<&str>) -> Result<TxKind, HttpDecoderError> {
    match transact_to {
        None => Ok(TxKind::Create),
        Some("") | Some("0x") => Ok(TxKind::Create),
        Some(addr_str) => {
            let addr = Address::from_str(addr_str)
                .map_err(|_| HttpDecoderError::InvalidAddress(addr_str.to_string()))?;
            Ok(TxKind::Call(addr))
        }
    }
}

fn parse_hex_data(data: &str) -> Result<Bytes, HttpDecoderError> {
    if data.is_empty() {
        return Ok(Bytes::new());
    }

    let hex_data = if let Some(stripped) = data.strip_prefix("0x") {
        stripped
    } else {
        data
    };

    assertion_executor::primitives::hex::decode(hex_data)
        .map(Bytes::from)
        .map_err(|_| HttpDecoderError::InvalidHex(data.to_string()))
}

/// Decoder implementation for gRPC requests
#[derive(Debug, Default, Clone, Copy)]
pub struct GrpcTransactionDecoder;

impl GrpcTransactionDecoder {
    /// Convert gRPC SendTransactionsRequest to TxQueueContents
    pub fn decode_send_transactions(
        request: &SendTransactionsRequest,
    ) -> Result<Vec<TxQueueContents>, HttpDecoderError> {
        if request.transactions.is_empty() {
            return Err(HttpDecoderError::NoTransactions);
        }

        let mut queue_transactions = Vec::with_capacity(request.transactions.len());

        for transaction in &request.transactions {
            let tx_hash = B256::from_str(&transaction.hash)
                .map_err(|_| HttpDecoderError::InvalidHash(transaction.hash.clone()))?;

            let tx_env = TxEnv::try_from(&transaction.tx_env)?;

            queue_transactions.push(TxQueueContents::Tx(QueueTransaction { tx_hash, tx_env }));
        }

        Ok(queue_transactions)
    }

    /// Convert gRPC SendBlockEnvRequest to TxQueueContents
    pub fn decode_block_env(
        request: &SendBlockEnvRequest,
    ) -> Result<TxQueueContents, HttpDecoderError> {
        let block_env = BlockEnv::try_from(request)?;
        Ok(TxQueueContents::Block(block_env))
    }
}
