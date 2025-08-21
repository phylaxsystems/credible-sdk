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
    context::{
        BlockEnv,
        TxEnv,
    },
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

    fn to_transaction(raw_event: Self::RawEvent) -> Result<Vec<QueueTransaction>, Self::Error>;
    fn to_blockenv(raw_event: Self::RawEvent) -> Result<BlockEnv, Self::Error>;
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

    fn to_transaction(req: Self::RawEvent) -> Result<Vec<QueueTransaction>, Self::Error> {
        if req.method != "sendTransactions" {
            return Err(HttpDecoderError::SchemaError);
        }

        let params = req.params.ok_or(HttpDecoderError::MissingParams)?;
        let send_params: SendTransactionsParams =
            serde_json::from_value(params).map_err(|_| HttpDecoderError::SchemaError)?;

        if send_params.transactions.is_empty() {
            return Err(HttpDecoderError::NoTransactions);
        }

        let mut queue_transactions = Vec::with_capacity(send_params.transactions.len());

        for transaction in send_params.transactions {
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

            let gas_price: u128 =
                transaction.tx_env.gas_price.parse().map_err(|_| {
                    HttpDecoderError::InvalidHex(transaction.tx_env.gas_price.clone())
                })?;

            let data =
                if transaction.tx_env.data.is_empty() {
                    Bytes::new()
                } else if transaction.tx_env.data.starts_with("0x") {
                    // we check beforehand that we have at least 2 chars before skipping below
                    let hex_data = &transaction.tx_env.data[2..];
                    Bytes::from(hex::decode(hex_data).map_err(|_| {
                        HttpDecoderError::InvalidHex(transaction.tx_env.data.clone())
                    })?)
                } else {
                    // Try to decode without 0x prefix
                    Bytes::from(hex::decode(&transaction.tx_env.data).map_err(|_| {
                        HttpDecoderError::InvalidHex(transaction.tx_env.data.clone())
                    })?)
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
                ..Default::default()
            };

            queue_transactions.push(QueueTransaction { tx_hash, tx_env });
        }

        Ok(queue_transactions)
    }

    fn to_blockenv(_req: Self::RawEvent) -> Result<BlockEnv, Self::Error> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::http::server::{
        JsonRpcRequest,
        SendTransactionsParams,
        Transaction,
        TransactionEnv,
    };
    use revm::primitives::{
        Address,
        TxKind,
        U256,
        bytes,
    };
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
                caller: format!("{:?}", caller),
                gas_limit,
                gas_price: gas_price.to_string(),
                transact_to: to.to_string(),
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
                caller: format!("{:?}", caller),
                gas_limit,
                gas_price: gas_price.to_string(),
                transact_to: format!("{:?}", to),
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
        let result = HttpTransactionDecoder::to_transaction(request);

        assert!(result.is_ok());
        let transactions = result.unwrap();
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
        let result = HttpTransactionDecoder::to_transaction(request);

        assert!(result.is_ok());
        let transactions = result.unwrap();
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
    fn test_contract_creation_transaction() {
        let caller = Address::random();

        let transaction = create_test_transaction(
            "0x3333333333333333333333333333333333333333333333333333333333333333",
            caller,
            "",
            "0",
            "0x608060405234801561001057600080fd5b50",
            2000000,
            "20000000000",
            44,
            1,
        );

        let request = create_test_request("sendTransactions", vec![transaction]);
        let result = HttpTransactionDecoder::to_transaction(request);

        assert!(result.is_ok());
        let transactions = result.unwrap();
        assert_eq!(transactions.len(), 1);

        let decoded_tx = &transactions[0];
        assert_eq!(decoded_tx.tx_env.caller, caller);
        assert!(matches!(decoded_tx.tx_env.kind, TxKind::Create));
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
        let result = HttpTransactionDecoder::to_transaction(request);

        assert!(result.is_ok());
        let transactions = result.unwrap();
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
        let result = HttpTransactionDecoder::to_transaction(request);

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

        let result = HttpTransactionDecoder::to_transaction(request);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HttpDecoderError::MissingParams
        ));
    }

    #[test]
    fn test_empty_transactions_error() {
        let request = create_test_request("sendTransactions", vec![]);
        let result = HttpTransactionDecoder::to_transaction(request);

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
        let result = HttpTransactionDecoder::to_transaction(request);

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
                transact_to: format!("{:?}", Address::random()),
                value: "1000000000000000000".to_string(),
                data: "0x".to_string(),
                nonce: 42,
                chain_id: 1,
                access_list: vec![],
            },
        };

        let request = create_test_request("sendTransactions", vec![transaction]);
        let result = HttpTransactionDecoder::to_transaction(request);

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
        let result = HttpTransactionDecoder::to_transaction(request);

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
        let result = HttpTransactionDecoder::to_transaction(request);

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
        let result = HttpTransactionDecoder::to_transaction(request);

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

        let result = HttpTransactionDecoder::to_transaction(request);
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
        let result = HttpTransactionDecoder::to_transaction(request);

        assert!(result.is_ok());
        let transactions = result.unwrap();
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
        let result = HttpTransactionDecoder::to_transaction(request);

        assert!(result.is_ok());
        let transactions = result.unwrap();
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].tx_env.caller, caller);
        assert!(transactions[0].tx_env.data.is_empty());
    }

    #[test]
    fn test_0x_prefix_contract_creation() {
        let caller = Address::random();

        let transaction = create_test_transaction(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            caller,
            "0x", // "0x" should also be treated as contract creation
            "0",
            "0x608060405234801561001057600080fd5b50",
            2000000,
            "20000000000",
            44,
            1,
        );

        let request = create_test_request("sendTransactions", vec![transaction]);
        let result = HttpTransactionDecoder::to_transaction(request);

        assert!(result.is_ok());
        let transactions = result.unwrap();
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].tx_env.caller, caller);
        assert!(matches!(transactions[0].tx_env.kind, TxKind::Create));
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
        let result = HttpTransactionDecoder::to_transaction(request);

        assert!(result.is_ok());
        let transactions = result.unwrap();
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
        let result = HttpTransactionDecoder::to_transaction(request);

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
        let result = HttpTransactionDecoder::to_transaction(request);

        assert!(result.is_ok());
        let transactions = result.unwrap();
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
        let result = HttpTransactionDecoder::to_transaction(request);

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
        let result = HttpTransactionDecoder::to_transaction(request);

        assert!(result.is_ok());
        let transactions = result.unwrap();
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
        let result = HttpTransactionDecoder::to_transaction(request);

        assert!(result.is_ok());
        let transactions = result.unwrap();
        assert_eq!(transactions.len(), 2);

        // Both transactions should decode to the same data
        let expected_bytes = bytes!("1234abcd");
        assert_eq!(transactions[0].tx_env.data, expected_bytes);
        assert_eq!(transactions[1].tx_env.data, expected_bytes);

        // Verify callers are different but data is the same
        assert_eq!(transactions[0].tx_env.caller, caller1);
        assert_eq!(transactions[1].tx_env.caller, caller2);
    }
}
