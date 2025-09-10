//! `decoder`
//!
//! This mod contains traits and implemntations of transaction decoders.
//! Transactions arrive in various formats from transports, and its the job
//! of the decoders to convert them into events that can be passed down to
//! the core engine.

use crate::{
    engine::queue::{
        QueueBlockEnv,
        QueueTransaction,
        TxQueueContents,
    },
    transport::http::server::{
        JsonRpcRequest,
        METHOD_BLOCK_ENV,
        METHOD_REVERT_TX,
        METHOD_SEND_TRANSACTIONS,
        SendTransactionsParams,
        TransactionEnv,
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
    type RawEvent: Send + Clone + Sized;
    type Error: std::error::Error + Send + Clone;

    fn to_tx_queue_contents(
        raw_event: &Self::RawEvent,
    ) -> Result<Vec<TxQueueContents>, Self::Error>;
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

fn parse_tx_kind(transact_to: Option<&str>) -> Result<TxKind, HttpDecoderError> {
    match transact_to {
        // If transact_to is None or "" or "0x", it's a contract creation transaction
        None | Some("" | "0x") => Ok(TxKind::Create),
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

    hex::decode(hex_data)
        .map(Bytes::from)
        .map_err(|_| HttpDecoderError::InvalidHex(data.to_string()))
}

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

#[derive(Debug, Default, Clone, Copy)]
pub struct HttpTransactionDecoder;

impl HttpTransactionDecoder {
    fn to_transaction(req: &JsonRpcRequest) -> Result<Vec<TxQueueContents>, HttpDecoderError> {
        let params = req.params.as_ref().ok_or(HttpDecoderError::MissingParams)?;
        let send_params: SendTransactionsParams =
            serde_json::from_value(params.clone()).map_err(|_| HttpDecoderError::SchemaError)?;

        if send_params.transactions.is_empty() {
            return Err(HttpDecoderError::NoTransactions);
        }

        let mut queue_transactions = Vec::with_capacity(send_params.transactions.len());

        for transaction in send_params.transactions {
            let tx_hash = B256::from_str(&transaction.hash)
                .map_err(|_| HttpDecoderError::InvalidHash(transaction.hash.clone()))?;

            let tx_env = TxEnv::try_from(&transaction.tx_env)?;

            let current_span = tracing::Span::current();
            queue_transactions.push(TxQueueContents::Tx(
                QueueTransaction { tx_hash, tx_env },
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
            METHOD_BLOCK_ENV => {
                let params = req.params.as_ref().ok_or(HttpDecoderError::MissingParams)?;
                let block = serde_json::from_value::<QueueBlockEnv>(params.clone())
                    .map_err(|_| HttpDecoderError::SchemaError)?;
                let current_span = tracing::Span::current();
                Ok(vec![TxQueueContents::Block(block, current_span)])
            }
            METHOD_REVERT_TX => {
                let params = req.params.as_ref().ok_or(HttpDecoderError::MissingParams)?;
                let hash = params
                    .get("removedTxHash")
                    .ok_or(HttpDecoderError::MissingParams)?
                    .as_str()
                    .unwrap();
                let hash: B256 = match hash.parse() {
                    Ok(rax) => rax,
                    Err(_) => {
                        return Err(HttpDecoderError::InvalidHash("bad hash size".to_string()));
                    }
                };

                let current_span = tracing::Span::current();
                Ok(vec![TxQueueContents::Reorg(hash, current_span)])
            }
            _ => Err(HttpDecoderError::SchemaError),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::match_wildcard_for_single_variants)]

    use super::*;
    use crate::transport::http::server::{
        JsonRpcRequest,
        SendTransactionsParams,
        Transaction,
        TransactionEnv,
    };
    use assertion_executor::primitives::BlockEnv;
    use revm::{
        context_interface::block::BlobExcessGasAndPrice,
        primitives::{
            Address,
            TxKind,
            U256,
            bytes,
        },
    };
    use serde_json::json;
    use std::str::FromStr;

    #[allow(clippy::too_many_arguments)]
    fn create_test_transaction(
        hash: &str,
        caller: Address,
        to: &str,
        value: &str,
        data: &str,
        gas_limit: u64,
        gas_price: &str,
        nonce: u64,
        chain_id: u64,
    ) -> Transaction {
        Transaction {
            hash: hash.to_string(),
            tx_env: TransactionEnv {
                caller: format!("{caller:?}"),
                gas_limit,
                gas_price: gas_price.to_string(),
                transact_to: if to.is_empty() {
                    None
                } else {
                    Some(to.to_string())
                },
                value: value.to_string(),
                data: data.to_string(),
                nonce,
                chain_id,
                access_list: vec![],
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn create_test_transaction_with_to(
        hash: &str,
        caller: Address,
        to: Address,
        value: &str,
        data: &str,
        gas_limit: u64,
        gas_price: &str,
        nonce: u64,
        chain_id: u64,
    ) -> Transaction {
        Transaction {
            hash: hash.to_string(),
            tx_env: TransactionEnv {
                caller: format!("{caller:?}"),
                gas_limit,
                gas_price: gas_price.to_string(),
                transact_to: Some(format!("{to:?}")),
                value: value.to_string(),
                data: data.to_string(),
                nonce,
                chain_id,
                access_list: vec![],
            },
        }
    }

    fn create_test_request(method: &str, transactions: Vec<Transaction>) -> JsonRpcRequest {
        let params = SendTransactionsParams { transactions };
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: method.to_string(),
            params: Some(serde_json::to_value(params).unwrap()),
            id: Some(serde_json::Value::Number(serde_json::Number::from(1))),
        }
    }

    #[test]
    fn test_single_transaction_success() {
        let caller = Address::random();
        let to = Address::random();

        let transaction = create_test_transaction_with_to(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            caller,
            to,
            "1000000000000000000",
            "0x",
            21000,
            "20000000000",
            42, // nonce
            1,  // mainnet
        );

        let request = create_test_request("sendTransactions", vec![transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_ok());
        let transactions = result
            .unwrap()
            .into_iter()
            .filter_map(|queue_content| {
                match queue_content {
                    TxQueueContents::Tx(tx, _) => Some(tx),
                    _ => None,
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(transactions.len(), 1);

        let decoded_tx = &transactions[0];
        assert_eq!(
            decoded_tx.tx_hash,
            B256::from_str("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
                .unwrap()
        );
        assert_eq!(decoded_tx.tx_env.caller, caller);
        assert_eq!(
            decoded_tx.tx_env.value,
            U256::from_str("1000000000000000000").unwrap()
        );
        assert_eq!(decoded_tx.tx_env.gas_limit, 21000);
        assert_eq!(decoded_tx.tx_env.gas_price, 20000000000);
        assert_eq!(decoded_tx.tx_env.nonce, 42);
        assert_eq!(decoded_tx.tx_env.chain_id, Some(1));

        if let TxKind::Call(to_addr) = decoded_tx.tx_env.kind {
            assert_eq!(to_addr, to);
        } else {
            panic!("Expected Call transaction kind");
        }
    }

    #[test]
    fn test_multiple_transactions_success() {
        let caller1 = Address::random();
        let to1 = Address::random();
        let caller2 = Address::random();
        let to2 = Address::random();

        let tx1 = create_test_transaction_with_to(
            "0x1111111111111111111111111111111111111111111111111111111111111111",
            caller1,
            to1,
            "1000000000000000000",
            "0x",
            21000,
            "20000000000",
            42,
            1,
        );

        let tx2 = create_test_transaction_with_to(
            "0x2222222222222222222222222222222222222222222222222222222222222222",
            caller2,
            to2,
            "2000000000000000000",
            "0x1234",
            50000,
            "25000000000",
            43,
            1,
        );

        let request = create_test_request("sendTransactions", vec![tx1, tx2]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_ok());
        let transactions = result
            .unwrap()
            .into_iter()
            .filter_map(|queue_content| {
                match queue_content {
                    TxQueueContents::Tx(tx, _) => Some(tx),
                    _ => None,
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(transactions.len(), 2);

        // Verify first transaction
        let first_tx = &transactions[0];
        assert_eq!(
            first_tx.tx_hash,
            B256::from_str("0x1111111111111111111111111111111111111111111111111111111111111111")
                .unwrap()
        );
        assert_eq!(first_tx.tx_env.caller, caller1);
        assert_eq!(
            first_tx.tx_env.value,
            U256::from_str("1000000000000000000").unwrap()
        );

        // Verify second transaction
        let second_tx = &transactions[1];
        assert_eq!(
            second_tx.tx_hash,
            B256::from_str("0x2222222222222222222222222222222222222222222222222222222222222222")
                .unwrap()
        );
        assert_eq!(second_tx.tx_env.caller, caller2);
        assert_eq!(
            second_tx.tx_env.value,
            U256::from_str("2000000000000000000").unwrap()
        );
        assert_eq!(
            second_tx.tx_env.data,
            revm::primitives::Bytes::from(vec![0x12, 0x34])
        );
    }

    #[test]
    fn test_all_contract_creation_variations() {
        let caller = Address::random();
        let hash = "0x4444444444444444444444444444444444444444444444444444444444444444";
        let deploy_code = "0x608060405234801561001057600080fd5b50";

        // Create base transaction JSON
        let mut base_tx = json!({
            "hash": hash,
            "txEnv": {
                "caller": format!("{caller:?}"),
                "gas_limit": 2000000,
                "gas_price": "20000000000",
                "value": "0",
                "data": deploy_code,
                "nonce": 1,
                "chain_id": 1,
                "access_list": []
            }
        });

        // Test cases with different transact_to values
        let test_cases = vec![
            (None, "missing field"),
            (Some(json!(null)), "null value"),
            (Some(json!("")), "empty string"),
            (Some(json!("0x")), "0x string"),
        ];

        for (transact_to_value, description) in test_cases {
            // Modify or remove transact_to field
            if let Some(value) = transact_to_value {
                base_tx["txEnv"]["transact_to"] = value;
            } else {
                // Remove the field if it exists
                base_tx["txEnv"]
                    .as_object_mut()
                    .unwrap()
                    .remove("transact_to");
            }

            // Update nonce for each test to ensure uniqueness
            base_tx["txEnv"]["nonce"] = json!(base_tx["txEnv"]["nonce"].as_u64().unwrap() + 1);

            // Deserialize the Transaction
            let transaction: Transaction = serde_json::from_value(base_tx.clone())
                .unwrap_or_else(|_| panic!("Failed to deserialize transaction with {description}"));

            // Create a request with this transaction
            let request = create_test_request("sendTransactions", vec![transaction]);

            // Decode the transaction
            let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
            assert!(
                result.is_ok(),
                "Failed to decode transaction with {description}: {result:?}",
            );

            let transactions = result
                .unwrap()
                .into_iter()
                .filter_map(|queue_content| {
                    match queue_content {
                        TxQueueContents::Tx(tx, _) => Some(tx),
                        _ => None,
                    }
                })
                .collect::<Vec<_>>();

            let decoded_tx = &transactions[0];
            assert_eq!(
                decoded_tx.tx_env.caller, caller,
                "Caller mismatch for {description}",
            );
            assert!(
                matches!(decoded_tx.tx_env.kind, TxKind::Create),
                "Expected TxKind::Create for {}, got {:?}",
                description,
                decoded_tx.tx_env.kind
            );
        }
    }

    #[test]
    fn test_contract_call_transaction() {
        let caller = Address::random();
        let to = Address::random();

        let transaction = create_test_transaction_with_to(
            "0x4444444444444444444444444444444444444444444444444444444444444444",
            caller,
            to,
            "0",
            "0xa9059cbb000000000000000000000000000000000000000000000000000000000001",
            100000,
            "20000000000",
            45,
            1,
        );

        let request = create_test_request("sendTransactions", vec![transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_ok());
        let transactions = result
            .unwrap()
            .into_iter()
            .filter_map(|queue_content| {
                match queue_content {
                    TxQueueContents::Tx(tx, _) => Some(tx),
                    _ => None,
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(transactions.len(), 1);

        let decoded_tx = &transactions[0];
        assert_eq!(decoded_tx.tx_env.caller, caller);
        if let TxKind::Call(to_addr) = decoded_tx.tx_env.kind {
            assert_eq!(to_addr, to);
        } else {
            panic!("Expected Call transaction kind");
        }
    }

    #[test]
    fn test_wrong_method_error() {
        let caller = Address::random();
        let to = Address::random();

        let transaction = create_test_transaction_with_to(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            caller,
            to,
            "1000000000000000000",
            "0x",
            21000,
            "20000000000",
            42,
            1,
        );

        let request = create_test_request("wrongMethod", vec![transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HttpDecoderError::SchemaError));
    }

    #[test]
    fn test_missing_params_error() {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "sendTransactions".to_string(),
            params: None,
            id: Some(serde_json::Value::Number(serde_json::Number::from(1))),
        };

        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HttpDecoderError::MissingParams
        ));
    }

    #[test]
    fn test_empty_transactions_error() {
        let request = create_test_request("sendTransactions", vec![]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HttpDecoderError::NoTransactions
        ));
    }

    #[test]
    fn test_invalid_hash_error() {
        let caller = Address::random();
        let to = Address::random();

        let transaction = create_test_transaction_with_to(
            "invalid_hash", // invalid hash format
            caller,
            to,
            "1000000000000000000",
            "0x",
            21000,
            "20000000000",
            42,
            1,
        );

        let request = create_test_request("sendTransactions", vec![transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_err());
        if let HttpDecoderError::InvalidHash(hash) = result.unwrap_err() {
            assert_eq!(hash, "invalid_hash");
        } else {
            panic!("Expected InvalidHash error");
        }
    }

    #[test]
    fn test_invalid_address_error() {
        let transaction = Transaction {
            hash: "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string(),
            tx_env: TransactionEnv {
                caller: "invalid_address".to_string(), // invalid address format
                gas_limit: 21000,
                gas_price: "20000000000".to_string(),
                transact_to: Some(format!("{:?}", Address::random())),
                value: "1000000000000000000".to_string(),
                data: "0x".to_string(),
                nonce: 42,
                chain_id: 1,
                access_list: vec![],
            },
        };

        let request = create_test_request("sendTransactions", vec![transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_err());
        if let HttpDecoderError::InvalidAddress(addr) = result.unwrap_err() {
            assert_eq!(addr, "invalid_address");
        } else {
            panic!("Expected InvalidAddress error");
        }
    }

    #[test]
    fn test_invalid_value_error() {
        let caller = Address::random();
        let to = Address::random();

        let transaction = create_test_transaction_with_to(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            caller,
            to,
            "invalid_value", // invalid value format
            "0x",
            21000,
            "20000000000",
            42,
            1,
        );

        let request = create_test_request("sendTransactions", vec![transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_err());
        if let HttpDecoderError::InvalidHex(value) = result.unwrap_err() {
            assert_eq!(value, "invalid_value");
        } else {
            panic!("Expected InvalidHex error");
        }
    }

    #[test]
    fn test_invalid_gas_price_error() {
        let caller = Address::random();
        let to = Address::random();

        let transaction = create_test_transaction_with_to(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            caller,
            to,
            "1000000000000000000",
            "0x",
            21000,
            "invalid_gas_price", // invalid gas price format
            42,
            1,
        );

        let request = create_test_request("sendTransactions", vec![transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_err());
        if let HttpDecoderError::InvalidHex(gas_price) = result.unwrap_err() {
            assert_eq!(gas_price, "invalid_gas_price");
        } else {
            panic!("Expected InvalidHex error");
        }
    }

    #[test]
    fn test_invalid_data_error() {
        let caller = Address::random();
        let to = Address::random();

        let transaction = create_test_transaction_with_to(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            caller,
            to,
            "1000000000000000000",
            "invalid_hex_data_with_non_hex_chars",
            21000,
            "20000000000",
            42,
            1,
        );

        let request = create_test_request("sendTransactions", vec![transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_err());
        if let HttpDecoderError::InvalidHex(data) = result.unwrap_err() {
            assert_eq!(data, "invalid_hex_data_with_non_hex_chars");
        } else {
            panic!("Expected InvalidHex error");
        }
    }

    #[test]
    fn test_invalid_schema_error() {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "sendTransactions".to_string(),
            params: Some(serde_json::json!({"invalid": "schema"})), // wrong schema
            id: Some(serde_json::Value::Number(serde_json::Number::from(1))),
        };

        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HttpDecoderError::SchemaError));
    }

    #[test]
    fn test_zero_value_transaction() {
        let caller = Address::random();
        let to = Address::random();

        let transaction = create_test_transaction_with_to(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            caller,
            to,
            "0", // zero value
            "0x",
            21000,
            "20000000000",
            42,
            1,
        );

        let request = create_test_request("sendTransactions", vec![transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_ok());
        let transactions = result
            .unwrap()
            .into_iter()
            .filter_map(|queue_content| {
                match queue_content {
                    TxQueueContents::Tx(tx, _) => Some(tx),
                    _ => None,
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].tx_env.caller, caller);
        assert_eq!(transactions[0].tx_env.value, U256::ZERO);
    }

    #[test]
    fn test_empty_data_transaction() {
        let caller = Address::random();
        let to = Address::random();

        let transaction = create_test_transaction_with_to(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            caller,
            to,
            "1000000000000000000",
            "", // empty data
            21000,
            "20000000000",
            42,
            1,
        );

        let request = create_test_request("sendTransactions", vec![transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_ok());
        let transactions = result
            .unwrap()
            .into_iter()
            .filter_map(|queue_content| {
                match queue_content {
                    TxQueueContents::Tx(tx, _) => Some(tx),
                    _ => None,
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].tx_env.caller, caller);
        assert!(transactions[0].tx_env.data.is_empty());
    }

    #[test]
    fn test_large_values() {
        let caller = Address::random();
        let to = Address::random();

        let transaction = create_test_transaction_with_to(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            caller,
            to,
            &U256::MAX.to_string(),
            "0x",
            8000000,        // high gas limit
            "100000000000", // high gas price
            999999999,      // high nonce
            1,
        );

        let request = create_test_request("sendTransactions", vec![transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_ok());
        let transactions = result
            .unwrap()
            .into_iter()
            .filter_map(|queue_content| {
                match queue_content {
                    TxQueueContents::Tx(tx, _) => Some(tx),
                    _ => None,
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(transactions.len(), 1);

        let decoded_tx = &transactions[0];
        assert_eq!(decoded_tx.tx_env.caller, caller);
        assert_eq!(decoded_tx.tx_env.gas_limit, 8000000);
        assert_eq!(decoded_tx.tx_env.gas_price, 100000000000);
        assert_eq!(decoded_tx.tx_env.nonce, 999999999);
        assert_eq!(decoded_tx.tx_env.value, U256::MAX);
    }

    #[test]
    fn test_mixed_valid_invalid_transactions() {
        let caller1 = Address::random();
        let to1 = Address::random();
        let caller2 = Address::random();

        let valid_tx = create_test_transaction_with_to(
            "0x1111111111111111111111111111111111111111111111111111111111111111",
            caller1,
            to1,
            "1000000000000000000",
            "0x",
            21000,
            "20000000000",
            42,
            1,
        );

        let invalid_tx = create_test_transaction(
            "invalid_hash",
            caller2,
            &format!("{:?}", Address::random()),
            "1000000000000000000",
            "0x",
            21000,
            "20000000000",
            43,
            1,
        );

        let request = create_test_request("sendTransactions", vec![valid_tx, invalid_tx]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HttpDecoderError::InvalidHash(_)
        ));
    }

    #[test]
    fn test_long_hex_data() {
        let caller = Address::random();
        let to = Address::random();
        let long_data = "0x".to_string() + &"a".repeat(2000);

        let transaction = create_test_transaction_with_to(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            caller,
            to,
            "0",
            &long_data,
            2000000,
            "20000000000",
            42,
            1,
        );

        let request = create_test_request("sendTransactions", vec![transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_ok());
        let transactions = result
            .unwrap()
            .into_iter()
            .filter_map(|queue_content| {
                match queue_content {
                    TxQueueContents::Tx(tx, _) => Some(tx),
                    _ => None,
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].tx_env.caller, caller);
        assert_eq!(transactions[0].tx_env.data.len(), 1000);
    }

    #[test]
    fn test_malformed_hex_data() {
        let caller = Address::random();
        let to = Address::random();

        let transaction = create_test_transaction_with_to(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            caller,
            to,
            "0",
            "0xgggg",
            21000,
            "20000000000",
            42,
            1,
        );

        let request = create_test_request("sendTransactions", vec![transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_err());
        if let HttpDecoderError::InvalidHex(data) = result.unwrap_err() {
            assert_eq!(data, "0xgggg");
        } else {
            panic!("Expected InvalidHex error");
        }
    }

    #[test]
    fn test_calldata_without_0x_prefix() {
        let caller = Address::random();
        let to = Address::random();

        let transaction = create_test_transaction_with_to(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            caller,
            to,
            "0",
            "a9059cbb0000000000000000000000000000000000000000000000000000000000000001",
            100000,
            "20000000000",
            45,
            1,
        );

        let request = create_test_request("sendTransactions", vec![transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_ok());
        let transactions = result
            .unwrap()
            .into_iter()
            .filter_map(|queue_content| {
                match queue_content {
                    TxQueueContents::Tx(tx, _) => Some(tx),
                    _ => None,
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(transactions.len(), 1);

        let decoded_tx = &transactions[0];
        assert_eq!(decoded_tx.tx_env.caller, caller);

        let expected_bytes =
            bytes!("a9059cbb0000000000000000000000000000000000000000000000000000000000000001");
        assert_eq!(decoded_tx.tx_env.data, expected_bytes);

        if let TxKind::Call(to_addr) = decoded_tx.tx_env.kind {
            assert_eq!(to_addr, to);
        } else {
            panic!("Expected Call transaction kind");
        }
    }

    #[test]
    fn test_calldata_both_with_and_without_prefix() {
        let caller1 = Address::random();
        let to1 = Address::random();
        let caller2 = Address::random();
        let to2 = Address::random();

        // Transaction with 0x prefix
        let tx_with_prefix = create_test_transaction_with_to(
            "0x1111111111111111111111111111111111111111111111111111111111111111",
            caller1,
            to1,
            "0",
            "0x1234abcd", // with 0x prefix
            100000,
            "20000000000",
            45,
            1,
        );

        // Transaction without 0x prefix
        let tx_without_prefix = create_test_transaction_with_to(
            "0x2222222222222222222222222222222222222222222222222222222222222222",
            caller2,
            to2,
            "0",
            "1234abcd", // without 0x prefix
            100000,
            "20000000000",
            46,
            1,
        );

        let request =
            create_test_request("sendTransactions", vec![tx_with_prefix, tx_without_prefix]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_ok());
        let transactions = result
            .unwrap()
            .into_iter()
            .filter_map(|queue_content| {
                match queue_content {
                    TxQueueContents::Tx(tx, _) => Some(tx),
                    _ => None,
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(transactions.len(), 2);

        // Both transactions should decode to the same data
        let expected_bytes = bytes!("1234abcd");
        assert_eq!(transactions[0].tx_env.data, expected_bytes);
        assert_eq!(transactions[1].tx_env.data, expected_bytes);

        // Verify callers are different but data is the same
        assert_eq!(transactions[0].tx_env.caller, caller1);
        assert_eq!(transactions[1].tx_env.caller, caller2);
    }

    #[test]
    fn test_decode_valid_block_env() {
        let valid_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 1234567890u64,
                "gas_limit": 30000000u64,
                "basefee": 1000000000u64,
                "difficulty": "0x0",
                "prevrandao": "0x0000000000000000000000000000000000000000000000000000000000000000",
                "last_tx_hash": "0x2222222222222222222222222222222222222222222222222222222222222222",
                "n_transactions": 1000u64
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(valid_request).unwrap();

        // Test direct deserialization of BlockEnv from params
        let block_env_result = serde_json::from_value::<QueueBlockEnv>(request.params.unwrap());
        assert!(
            block_env_result.is_ok(),
            "Should successfully deserialize valid BlockEnv: {:?}",
            block_env_result.err()
        );

        let queue_block_env = block_env_result.unwrap();
        let block_env = queue_block_env.block_env;
        assert_eq!(block_env.number, 123456u64);
        assert_eq!(block_env.basefee, 1000000000u64);
        assert_eq!(block_env.gas_limit, 30000000u64);
        assert_eq!(block_env.timestamp, 1234567890u64);
        assert_eq!(
            queue_block_env.last_tx_hash,
            Some(
                "0x2222222222222222222222222222222222222222222222222222222222222222"
                    .parse()
                    .unwrap()
            )
        );
        assert_eq!(queue_block_env.n_transactions, 1000);
    }

    #[test]
    fn test_decode_minimal_block_env() {
        let minimal_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 1u64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 0u64,
                "gas_limit": 0u64,
                "basefee": 0u64,
                "difficulty": "0x0"
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(minimal_request).unwrap();

        // Test that minimal BlockEnv works
        let block_env_result = serde_json::from_value::<QueueBlockEnv>(request.params.unwrap());
        assert!(
            block_env_result.is_ok(),
            "Should successfully deserialize minimal BlockEnv"
        );

        let block_env = block_env_result.unwrap().block_env;
        assert_eq!(block_env.number, 1u64);
        assert_eq!(block_env.basefee, 0u64);
        assert_eq!(block_env.gas_limit, 0u64);
        assert_eq!(block_env.timestamp, 0u64);
    }

    #[test]
    fn test_decode_block_env_with_blob_excess_gas() {
        let with_blob_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64,
                "beneficiary": "0x1234567890123456789012345678901234567890",
                "timestamp": 1234567890u64,
                "gas_limit": 30000000u64,
                "basefee": 1000000000u64,
                "difficulty": "0x0",
                "blob_excess_gas_and_price": {
                    "excess_blob_gas": 1000u64,
                    "blob_gasprice": 2000u128
                }
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(with_blob_request).unwrap();

        // Test that blob excess gas is properly deserialized
        let block_env_result = serde_json::from_value::<QueueBlockEnv>(request.params.unwrap());
        assert!(
            block_env_result.is_ok(),
            "Should successfully deserialize BlockEnv with blob excess gas"
        );

        let queue_block_env = block_env_result.unwrap();
        let block_env = queue_block_env.block_env;
        assert!(
            block_env.blob_excess_gas_and_price.is_some(),
            "blob_excess_gas_and_price should be present"
        );
        let blob_data = block_env.blob_excess_gas_and_price.unwrap();
        assert_eq!(blob_data.excess_blob_gas, 1000u64);
        assert_eq!(blob_data.blob_gasprice, 2000u128);
        assert_eq!(queue_block_env.n_transactions, 0);
        assert_eq!(queue_block_env.last_tx_hash, None);
    }

    #[test]
    fn test_decode_block_env_missing_params() {
        let no_params_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(no_params_request).unwrap();

        // Should handle missing params gracefully
        assert!(request.params.is_none(), "Request should have no params");
    }

    #[test]
    fn test_decode_block_env_invalid_number_type() {
        let invalid_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": "invalid_number",  // String instead of u64
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 0u64,
                "gas_limit": 0u64,
                "basefee": 0u64,
                "difficulty": "0x0"
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(invalid_request).unwrap();

        // Test that invalid number type fails deserialization
        let block_env_result = serde_json::from_value::<QueueBlockEnv>(request.params.unwrap());
        assert!(
            block_env_result.is_err(),
            "Should fail to deserialize BlockEnv with invalid number type"
        );
    }

    #[test]
    fn test_decode_block_env_invalid_address() {
        let invalid_address_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64,
                "beneficiary": "invalid_address",
                "timestamp": 0u64,
                "gas_limit": 0u64,
                "basefee": 0u64,
                "difficulty": "0x0"
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(invalid_address_request).unwrap();

        // Test that invalid address format fails deserialization
        let block_env_result = serde_json::from_value::<QueueBlockEnv>(request.params.unwrap());
        assert!(
            block_env_result.is_err(),
            "Should fail to deserialize BlockEnv with invalid address format"
        );
    }

    #[test]
    fn test_decode_block_env_invalid_hash() {
        let invalid_hash_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 0u64,
                "gas_limit": 0u64,
                "basefee": 0u64,
                "difficulty": "0x0",
                "prevrandao": "invalid_hash"
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(invalid_hash_request).unwrap();

        // Test that invalid hash format fails deserialization
        let block_env_result = serde_json::from_value::<QueueBlockEnv>(request.params.unwrap());
        assert!(
            block_env_result.is_err(),
            "Should fail to deserialize BlockEnv with invalid hash format"
        );
    }

    #[test]
    fn test_decode_block_env_negative_values() {
        let negative_values_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": -1i64,  // Negative number
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 0u64,
                "gas_limit": 0u64,
                "basefee": 0u64,
                "difficulty": "0x0"
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(negative_values_request).unwrap();

        // Test that negative values fail deserialization for u64 fields
        let block_env_result = serde_json::from_value::<QueueBlockEnv>(request.params.unwrap());
        assert!(
            block_env_result.is_err(),
            "Should fail to deserialize BlockEnv with negative values for u64 fields"
        );
    }

    #[test]
    fn test_decode_block_env_extra_fields() {
        let extra_fields_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 1234567890u64,
                "gas_limit": 30000000u64,
                "basefee": 1000000000u64,
                "difficulty": "0x0",
                "extra_field": "should_be_ignored",
                "another_extra": 42
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(extra_fields_request).unwrap();

        // Test that extra fields are ignored during deserialization
        let block_env_result = serde_json::from_value::<QueueBlockEnv>(request.params.unwrap());
        assert!(
            block_env_result.is_ok(),
            "Should successfully deserialize BlockEnv ignoring extra fields"
        );

        let block_env = block_env_result.unwrap().block_env;
        assert_eq!(block_env.number, 123456u64);
        assert_eq!(block_env.basefee, 1000000000u64);
        assert_eq!(block_env.timestamp, 1234567890u64);
    }

    #[test]
    fn test_decode_block_env_hex_values() {
        let hex_values_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64,
                "beneficiary": "0x1234567890123456789012345678901234567890",
                "timestamp": 1234567890u64,
                "gas_limit": 30000000u64,
                "basefee": 1000000000u64,
                "difficulty": "0x1e240", // 123456 in hex for U256
                "prevrandao": "0x1234567890123456789012345678901234567890123456789012345678901234"
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(hex_values_request).unwrap();

        // Test that valid hex values are properly deserialized
        let block_env_result = serde_json::from_value::<QueueBlockEnv>(request.params.unwrap());
        assert!(
            block_env_result.is_ok(),
            "Should successfully deserialize BlockEnv with hex values"
        );

        let block_env = block_env_result.unwrap().block_env;
        assert_eq!(block_env.number, 123456u64);
        assert_eq!(block_env.timestamp, 1234567890u64);
        assert!(
            block_env.prevrandao.is_some(),
            "prevrandao should be present"
        );
        assert_eq!(block_env.difficulty, U256::from(123456u64));
    }

    #[test]
    fn test_decode_block_env_zero_values() {
        let zero_values_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 0u64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 0u64,
                "gas_limit": 0u64,
                "basefee": 0u64,
                "difficulty": "0x0"
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(zero_values_request).unwrap();

        // Test that zero values are valid
        let block_env_result = serde_json::from_value::<QueueBlockEnv>(request.params.unwrap());
        assert!(
            block_env_result.is_ok(),
            "Should successfully deserialize BlockEnv with zero values"
        );

        let block_env = block_env_result.unwrap().block_env;
        assert_eq!(block_env.number, 0u64);
        assert_eq!(block_env.basefee, 0u64);
        assert_eq!(block_env.gas_limit, 0u64);
        assert_eq!(block_env.timestamp, 0u64);
        assert_eq!(block_env.difficulty, U256::ZERO);
        assert_eq!(block_env.beneficiary, Address::ZERO);
    }

    #[test]
    fn test_decode_block_env_max_values() {
        let max_values_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": u64::MAX,
                "beneficiary": "0xffffffffffffffffffffffffffffffffffffffff",
                "timestamp": u64::MAX,
                "gas_limit": u64::MAX,
                "basefee": u64::MAX,
                "difficulty": "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff" // U256::MAX
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(max_values_request).unwrap();

        // Test that maximum values are handled correctly
        let block_env_result = serde_json::from_value::<QueueBlockEnv>(request.params.unwrap());
        assert!(
            block_env_result.is_ok(),
            "Should successfully deserialize BlockEnv with maximum values"
        );

        let block_env = block_env_result.unwrap().block_env;
        assert_eq!(block_env.number, u64::MAX);
        assert_eq!(block_env.basefee, u64::MAX);
        assert_eq!(block_env.gas_limit, u64::MAX);
        assert_eq!(block_env.timestamp, u64::MAX);
        assert_eq!(block_env.difficulty, U256::MAX);
    }

    #[test]
    fn test_decode_block_env_with_prevrandao() {
        let with_prevrandao_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64,
                "beneficiary": "0x1234567890123456789012345678901234567890",
                "timestamp": 1234567890u64,
                "gas_limit": 30000000u64,
                "basefee": 1000000000u64,
                "difficulty": "0x0",
                "prevrandao": "0x1234567890123456789012345678901234567890123456789012345678901234"
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(with_prevrandao_request).unwrap();

        // Test that prevrandao is properly deserialized
        let block_env_result = serde_json::from_value::<QueueBlockEnv>(request.params.unwrap());
        assert!(
            block_env_result.is_ok(),
            "Should successfully deserialize BlockEnv with prevrandao"
        );

        let block_env = block_env_result.unwrap().block_env;
        assert!(
            block_env.prevrandao.is_some(),
            "prevrandao should be present"
        );
        assert_eq!(block_env.number, 123456u64);
        assert_eq!(block_env.basefee, 1000000000u64);
    }

    #[test]
    fn test_decode_block_env_without_prevrandao() {
        let without_prevrandao_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64,
                "beneficiary": "0x1234567890123456789012345678901234567890",
                "timestamp": 1234567890u64,
                "gas_limit": 30000000u64,
                "basefee": 1000000000u64,
                "difficulty": "0x0"
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(without_prevrandao_request).unwrap();

        // Test that BlockEnv works without prevrandao (should be None)
        let block_env_result = serde_json::from_value::<QueueBlockEnv>(request.params.unwrap());
        assert!(
            block_env_result.is_ok(),
            "Should successfully deserialize BlockEnv without prevrandao"
        );

        let block_env = block_env_result.unwrap().block_env;
        assert!(
            block_env.prevrandao.is_none(),
            "prevrandao should be None when not provided"
        );
        assert_eq!(block_env.number, 123456u64);
    }

    #[test]
    fn test_decode_block_env_invalid_blob_excess_gas() {
        let invalid_blob_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 0u64,
                "gas_limit": 0u64,
                "basefee": 0u64,
                "difficulty": "0x0",
                "blob_excess_gas_and_price": {
                    "excess_blob_gas": "invalid", // Should be u64
                    "blob_gasprice": 2000u128
                }
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(invalid_blob_request).unwrap();

        // Test that invalid blob excess gas fails deserialization
        let block_env_result = serde_json::from_value::<QueueBlockEnv>(request.params.unwrap());
        assert!(
            block_env_result.is_err(),
            "Should fail to deserialize BlockEnv with invalid blob excess gas"
        );
    }

    #[test]
    fn test_json_rpc_request_structure() {
        let valid_json_rpc = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 0u64,
                "gas_limit": 0u64,
                "basefee": 0u64,
                "difficulty": "0x0"
            },
            "id": "test-id"
        });

        let request_result = serde_json::from_value::<JsonRpcRequest>(valid_json_rpc);
        assert!(
            request_result.is_ok(),
            "Should successfully deserialize valid JSON-RPC request"
        );

        let request = request_result.unwrap();
        assert_eq!(request.jsonrpc, "2.0");
        assert_eq!(request.method, "sendBlockEnv");
        assert!(request.params.is_some());
        assert!(request.id.is_some());
    }

    #[test]
    fn test_json_rpc_request_missing_fields() {
        let minimal_json_rpc = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv"
        });

        let request_result = serde_json::from_value::<JsonRpcRequest>(minimal_json_rpc);
        assert!(
            request_result.is_ok(),
            "Should successfully deserialize JSON-RPC request with optional fields missing"
        );

        let request = request_result.unwrap();
        assert_eq!(request.jsonrpc, "2.0");
        assert_eq!(request.method, "sendBlockEnv");
        assert!(request.params.is_none());
        assert!(request.id.is_none());
    }

    #[test]
    fn test_debug_block_env_serialization() {
        // Create a BlockEnv and serialize it to see the expected format
        let block_env = QueueBlockEnv {
            block_env: BlockEnv {
                number: 123456u64,
                beneficiary: Address::ZERO,
                timestamp: 1234567890u64,
                gas_limit: 30000000u64,
                basefee: 1000000000u64,
                difficulty: U256::ZERO,
                prevrandao: Some(B256::ZERO),
                blob_excess_gas_and_price: Some(BlobExcessGasAndPrice {
                    excess_blob_gas: 1000u64,
                    blob_gasprice: 2000u128,
                }),
            },
            last_tx_hash: Some(
                "0x2222222222222222222222222222222222222222222222222222222222222222"
                    .parse()
                    .unwrap(),
            ),
            n_transactions: 25,
        };

        // Serialize and then deserialize to ensure round-trip works
        let serialized = serde_json::to_value(&block_env).unwrap();
        let deserialized = serde_json::from_value::<BlockEnv>(serialized).unwrap();

        assert_eq!(block_env.block_env.number, deserialized.number);
        assert_eq!(block_env.block_env.beneficiary, deserialized.beneficiary);
        assert_eq!(block_env.block_env.timestamp, deserialized.timestamp);
        assert_eq!(block_env.block_env.gas_limit, deserialized.gas_limit);
        assert_eq!(block_env.block_env.basefee, deserialized.basefee);
        assert_eq!(block_env.block_env.difficulty, deserialized.difficulty);
        assert_eq!(block_env.block_env.prevrandao, deserialized.prevrandao);
        assert_eq!(
            block_env.block_env.blob_excess_gas_and_price,
            deserialized.blob_excess_gas_and_price
        );
    }

    #[test]
    fn test_decode_block_env_missing_required_fields() {
        let missing_fields_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64
                // Missing required fields like beneficiary, timestamp, etc.
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(missing_fields_request).unwrap();

        // Test that missing required fields cause deserialization to fail
        let block_env_result = serde_json::from_value::<BlockEnv>(request.params.unwrap());
        assert!(
            block_env_result.is_err(),
            "Should fail to deserialize BlockEnv with missing required fields"
        );
    }

    #[test]
    fn test_decode_block_env_partial_blob_data() {
        let partial_blob_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 0u64,
                "gas_limit": 0u64,
                "basefee": 0u64,
                "difficulty": "0x0",
                "blob_excess_gas_and_price": {
                    "excess_blob_gas": 1000u64
                    // Missing blob_gasprice
                }
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(partial_blob_request).unwrap();

        // Test that partial blob data fails deserialization
        let block_env_result = serde_json::from_value::<BlockEnv>(request.params.unwrap());
        assert!(
            block_env_result.is_err(),
            "Should fail to deserialize BlockEnv with partial blob excess gas data"
        );
    }

    #[test]
    fn test_decode_block_env_u256_difficulty_variants() {
        // Test small U256 value
        let small_difficulty_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 0u64,
                "gas_limit": 0u64,
                "basefee": 0u64,
                "difficulty": "0x64" // 100 in hex
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(small_difficulty_request).unwrap();
        let block_env_result = serde_json::from_value::<BlockEnv>(request.params.unwrap());
        assert!(
            block_env_result.is_ok(),
            "Should handle small U256 difficulty"
        );
        assert_eq!(block_env_result.unwrap().difficulty, U256::from(100u64));

        // Test large U256 value
        let large_difficulty_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 0u64,
                "gas_limit": 0u64,
                "basefee": 0u64,
                "difficulty": "0x1000000000000000000000000000000000000000000000000000000000000000"
            },
            "id": 1
        });

        let request2: JsonRpcRequest = serde_json::from_value(large_difficulty_request).unwrap();
        let block_env_result2 = serde_json::from_value::<BlockEnv>(request2.params.unwrap());
        assert!(
            block_env_result2.is_ok(),
            "Should handle large U256 difficulty"
        );
    }

    #[test]
    fn test_decode_block_env_with_only_last_tx_hash() {
        let request_with_last_tx = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 1234567890u64,
                "gas_limit": 30000000u64,
                "basefee": 1000000000u64,
                "difficulty": "0x0",
                "prevrandao": "0x0000000000000000000000000000000000000000000000000000000000000000",
                "last_tx_hash": "0x1111111111111111111111111111111111111111111111111111111111111111"
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(request_with_last_tx).unwrap();
        let result = serde_json::from_value::<QueueBlockEnv>(request.params.unwrap());

        assert!(
            result.is_err(),
            "Should fail validation when last_tx_hash is present but n_transactions = 0"
        );
        // Fix the error message check - it should contain the message for when n_transactions is 0 but last_tx_hash is present
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("last_tx_hash must be null, empty, or missing")
        );
    }

    #[test]
    fn test_decode_block_env_backward_compatibility() {
        // Test that old format (without new fields) still works
        let old_format_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 987654u64,
                "beneficiary": "0x2222222222222222222222222222222222222222",
                "timestamp": 1111111111u64,
                "gas_limit": 25000000u64,
                "basefee": 500000000u64,
                "difficulty": "0x0",
                "prevrandao": "0x2222222222222222222222222222222222222222222222222222222222222222"
            },
            "id": 3
        });

        let request: JsonRpcRequest = serde_json::from_value(old_format_request).unwrap();
        let queue_block_env: QueueBlockEnv =
            serde_json::from_value(request.params.unwrap()).unwrap();

        // Verify BlockEnv fields are correctly deserialized
        assert_eq!(queue_block_env.block_env.number, 987654u64);
        assert_eq!(queue_block_env.block_env.gas_limit, 25000000u64);
        assert_eq!(queue_block_env.block_env.basefee, 500000000u64);

        // Verify new fields have default values
        assert_eq!(queue_block_env.last_tx_hash, None);
        assert_eq!(queue_block_env.n_transactions, 0);
    }

    #[test]
    fn test_decode_block_env_validation_error_null_hash_with_transactions() {
        let invalid_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 111111u64,
                "beneficiary": "0x3333333333333333333333333333333333333333",
                "timestamp": 2222222222u64,
                "gas_limit": 35000000u64,
                "basefee": 1500000000u64,
                "difficulty": "0x0",
                "prevrandao": "0x3333333333333333333333333333333333333333333333333333333333333333",
                "last_tx_hash": null,
                "n_transactions": 10u64  // Invalid: n_transactions > 0 but last_tx_hash is null
            },
            "id": 4
        });

        let request: JsonRpcRequest = serde_json::from_value(invalid_request).unwrap();
        let result = serde_json::from_value::<QueueBlockEnv>(request.params.unwrap());

        assert!(
            result.is_err(),
            "Should fail validation when n_transactions > 0 but last_tx_hash is null"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("last_tx_hash must be provided and non-empty")
        );
    }

    #[test]
    fn test_decode_block_env_with_zero_n_transactions() {
        let request_with_zero = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 222222u64,
                "beneficiary": "0x4444444444444444444444444444444444444444",
                "timestamp": 3333333333u64,
                "gas_limit": 40000000u64,
                "basefee": 3000000000u64,
                "difficulty": "0x0",
                "prevrandao": "0x4444444444444444444444444444444444444444444444444444444444444444",
                "last_tx_hash": "0x5555555555555555555555555555555555555555555555555555555555555555",
                "n_transactions": 0u64
            },
            "id": 5
        });

        let request: JsonRpcRequest = serde_json::from_value(request_with_zero).unwrap();
        let result = serde_json::from_value::<QueueBlockEnv>(request.params.unwrap());

        assert!(
            result.is_err(),
            "Should fail validation when n_transactions > 0 but last_tx_hash is null"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("last_tx_hash must be null, empty, or missing")
        );
    }

    #[test]
    fn test_decode_block_env_with_invalid_tx_hash_format() {
        let request_with_invalid_hash = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 333333u64,
                "beneficiary": "0x5555555555555555555555555555555555555555",
                "timestamp": 4444444444u64,
                "gas_limit": 45000000u64,
                "basefee": 4000000000u64,
                "difficulty": "0x0",
                "prevrandao": "0x5555555555555555555555555555555555555555555555555555555555555555",
                "last_tx_hash": "invalid_hash_format",
                "n_transactions": 5u64
            },
            "id": 6
        });

        let request: JsonRpcRequest = serde_json::from_value(request_with_invalid_hash).unwrap();
        let result = serde_json::from_value::<QueueBlockEnv>(request.params.unwrap());

        // Should fail to deserialize due to invalid hash format
        assert!(
            result.is_err(),
            "Should fail to deserialize invalid tx hash format"
        );
    }

    #[test]
    fn test_decode_block_env_with_large_n_transactions() {
        let request_with_large_n = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 444444u64,
                "beneficiary": "0x6666666666666666666666666666666666666666",
                "timestamp": 5555555555u64,
                "gas_limit": 55000000u64,
                "basefee": 5000000000u64,
                "difficulty": "0x0",
                "prevrandao": "0x6666666666666666666666666666666666666666666666666666666666666666",
                "last_tx_hash": "0x7777777777777777777777777777777777777777777777777777777777777777",
                "n_transactions": 18446744073709551615u64 // u64::MAX
            },
            "id": 7
        });

        let request: JsonRpcRequest = serde_json::from_value(request_with_large_n).unwrap();
        let queue_block_env: QueueBlockEnv =
            serde_json::from_value(request.params.unwrap()).unwrap();

        assert_eq!(queue_block_env.block_env.number, 444444u64);
        assert_eq!(
            queue_block_env.last_tx_hash,
            Some(
                "0x7777777777777777777777777777777777777777777777777777777777777777"
                    .parse()
                    .unwrap()
            )
        );
        assert_eq!(queue_block_env.n_transactions, u64::MAX);
    }

    #[test]
    fn test_queue_block_env_serialization_round_trip() {
        // Test that serialization and deserialization work correctly
        let original_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 555555u64,
                "beneficiary": "0x7777777777777777777777777777777777777777",
                "timestamp": 6666666666u64,
                "gas_limit": 60000000u64,
                "basefee": 6000000000u64,
                "difficulty": "0x0",
                "prevrandao": "0x7777777777777777777777777777777777777777777777777777777777777777",
                "last_tx_hash": "0x8888888888888888888888888888888888888888888888888888888888888888",
                "n_transactions": 123u64
            },
            "id": 8
        });

        let request: JsonRpcRequest = serde_json::from_value(original_request).unwrap();
        let queue_block_env: QueueBlockEnv =
            serde_json::from_value(request.params.unwrap()).unwrap();

        // Serialize back to JSON
        let serialized = serde_json::to_value(&queue_block_env).unwrap();

        // Deserialize again
        let deserialized: QueueBlockEnv = serde_json::from_value(serialized).unwrap();

        // Verify round-trip consistency
        assert_eq!(
            deserialized.block_env.number,
            queue_block_env.block_env.number
        );
        assert_eq!(
            deserialized.block_env.basefee,
            queue_block_env.block_env.basefee
        );
        assert_eq!(deserialized.last_tx_hash, queue_block_env.last_tx_hash);
        assert_eq!(deserialized.n_transactions, queue_block_env.n_transactions);
    }
}
