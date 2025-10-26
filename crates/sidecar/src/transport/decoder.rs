//! `decoder`
//!
//! This mod contains traits and implementations of transaction decoders.
//! Transactions arrive in various formats from transports, and its the job
//! of the decoders to convert them into events that can be passed down to
//! the core engine.

use crate::{
    engine::queue::{
        QueueBlockEnv,
        QueueTransaction,
        TxQueueContents,
    },
    transport::{
        common::HttpDecoderError,
        http::server::{
            JsonRpcRequest,
            METHOD_BLOCK_ENV,
            METHOD_REORG,
            METHOD_SEND_TRANSACTIONS,
            SendTransactionsParams,
            TxExecutionId,
        },
    },
};
use revm::primitives::B256;
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
            let tx_hash = transaction.tx_execution_id.tx_hash;

            let current_span = tracing::Span::current();
            queue_transactions.push(TxQueueContents::Tx(
                QueueTransaction {
                    tx_hash,
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
                Ok(vec![TxQueueContents::Reorg(
                    reorg.tx_hash,
                    current_span.clone(),
                )])
            }
            _ => {
                Err(HttpDecoderError::InvalidTransaction(
                    "unknown method".to_string(),
                ))
            }
        }
    }
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

#[cfg(test)]
mod tests {
    #![allow(clippy::match_wildcard_for_single_variants)]

    use super::*;
    use crate::transport::http::server::JsonRpcRequest;
    use alloy::primitives::{
        Address,
        U256,
    };
    use assertion_executor::primitives::BlockEnv;
    use revm::context_interface::block::BlobExcessGasAndPrice;
    use serde_json::json;

    #[allow(clippy::too_many_arguments)]
    fn create_test_transaction_json(
        hash: &str,
        caller: &str,
        to: Option<&str>,
        value: &str,
        data: &str,
        gas_limit: u64,
        gas_price: u128,
        nonce: u64,
        chain_id: u64,
    ) -> serde_json::Value {
        create_test_transaction_json_with_exec_id(
            hash, caller, to, value, data, gas_limit, gas_price, nonce, chain_id, 1u64, 1u64,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn create_test_transaction_json_with_exec_id(
        hash: &str,
        caller: &str,
        to: Option<&str>,
        value: &str,
        data: &str,
        gas_limit: u64,
        gas_price: u128,
        nonce: u64,
        chain_id: u64,
        block_number: u64,
        iteration_id: u64,
    ) -> serde_json::Value {
        let tx_env = json!({
            "tx_type": 0,
            "caller": caller,
            "gas_limit": gas_limit,
            "gas_price": gas_price,
            "kind": to,
            "value": value,
            "data": data,
            "nonce": nonce,
            "chain_id": chain_id,
            "access_list": [],
            "gas_priority_fee": null,
            "blob_hashes": [],
            "max_fee_per_blob_gas": 0,
            "authorization_list": []
        });

        json!({
            "tx_execution_id": {
                "block_number": block_number,
                "iteration_id": iteration_id,
                "tx_hash": hash
            },
            "tx_env": tx_env
        })
    }

    fn create_test_request_json(
        method: &str,
        transactions: &[serde_json::Value],
    ) -> JsonRpcRequest {
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: method.to_string(),
            params: Some(json!({
                "transactions": transactions
            })),
            id: Some(json!(1)),
        }
    }

    #[test]
    fn test_single_transaction_success() {
        let transaction = create_test_transaction_json(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "0x1234567890123456789012345678901234567890",
            Some("0x9876543210987654321098765432109876543210"),
            "1000000000000000000",
            "0x",
            21000,
            20000000000,
            0,
            1,
        );

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_ok());

        let contents = result.unwrap();
        assert_eq!(contents.len(), 1);
    }

    #[test]
    fn test_multiple_transactions_success() {
        let transactions = vec![
            create_test_transaction_json(
                "0x1111111111111111111111111111111111111111111111111111111111111111",
                "0x1111111111111111111111111111111111111111",
                Some("0x2222222222222222222222222222222222222222"),
                "1000000000000000000",
                "0x",
                21000,
                20000000000,
                0,
                1,
            ),
            create_test_transaction_json(
                "0x2222222222222222222222222222222222222222222222222222222222222222",
                "0x3333333333333333333333333333333333333333",
                Some("0x4444444444444444444444444444444444444444"),
                "2000000000000000000",
                "0xa9059cbb",
                25000,
                22000000000,
                1,
                1,
            ),
        ];

        let request = create_test_request_json("sendTransactions", &transactions);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(result.is_ok());

        let contents = result.unwrap();
        assert_eq!(contents.len(), 2);
    }

    #[test]
    fn test_transactions_with_different_execution_ids() {
        let transactions = vec![
            create_test_transaction_json_with_exec_id(
                "0x1111111111111111111111111111111111111111111111111111111111111111",
                "0x1111111111111111111111111111111111111111",
                Some("0x2222222222222222222222222222222222222222"),
                "1000000000000000000",
                "0x",
                21000,
                20000000000,
                0,
                1,
                100, // block_number
                1,   // iteration_id
            ),
            create_test_transaction_json_with_exec_id(
                "0x2222222222222222222222222222222222222222222222222222222222222222",
                "0x3333333333333333333333333333333333333333",
                Some("0x4444444444444444444444444444444444444444"),
                "2000000000000000000",
                "0xa9059cbb",
                25000,
                22000000000,
                1,
                1,
                100, // same block_number
                2,   // different iteration_id
            ),
        ];

        let request = create_test_request_json("sendTransactions", &transactions);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(result.is_ok());

        let contents = result.unwrap();
        assert_eq!(contents.len(), 2);
    }

    #[test]
    fn test_all_contract_creation_variations() {
        let transactions = vec![
            create_test_transaction_json(
                "0x1111111111111111111111111111111111111111111111111111111111111111",
                "0x1111111111111111111111111111111111111111",
                None, // Contract creation - no 'to' field
                "0",
                "0x608060405234801561001057600080fd5b50",
                1000000,
                20000000000,
                0,
                1,
            ),
            create_test_transaction_json(
                "0x2222222222222222222222222222222222222222222222222222222222222222",
                "0x2222222222222222222222222222222222222222",
                Some("0x0000000000000000000000000000000000000000"), // Zero address contract creation
                "0",
                "0x608060405234801561001057600080fd5b50",
                1000000,
                20000000000,
                1,
                1,
            ),
        ];

        let request = create_test_request_json("sendTransactions", &transactions);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(result.is_ok());

        let contents = result.unwrap();
        assert_eq!(contents.len(), 2);
    }

    #[test]
    fn test_contract_call_transaction() {
        let transaction = create_test_transaction_json(
            "0x2222222222222222222222222222222222222222222222222222222222222222",
            "0x1234567890123456789012345678901234567890",
            Some("0x9876543210987654321098765432109876543210"),
            "0",
            "0xa9059cbb00000000000000000000000012345678901234567890123456789012345678900000000000000000000000000000000000000000000000000de0b6b3a7640000",
            100000,
            25000000000,
            5,
            1,
        );

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(result.is_ok());

        let contents = result.unwrap();
        assert_eq!(contents.len(), 1);
    }

    #[test]
    fn test_wrong_method_error() {
        let transaction = create_test_transaction_json(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "0x1234567890123456789012345678901234567890",
            Some("0x9876543210987654321098765432109876543210"),
            "1000000000000000000",
            "0x",
            21000,
            20000000000,
            0,
            1,
        );

        let request = create_test_request_json("wrongMethod", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            HttpDecoderError::InvalidTransaction("unknown method".to_string()),
        );
    }

    #[test]
    fn test_missing_params_error() {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "sendTransactions".to_string(),
            params: None,
            id: Some(json!(1)),
        };

        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(matches!(result, Err(HttpDecoderError::MissingParams)));
    }

    #[test]
    fn test_empty_transactions_error() {
        let request = create_test_request_json("sendTransactions", &[]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(matches!(result, Err(HttpDecoderError::NoTransactions)));
    }

    #[test]
    fn test_invalid_hash_error() {
        let transaction = create_test_transaction_json(
            "invalid_hash",
            "0x1234567890123456789012345678901234567890",
            Some("0x9876543210987654321098765432109876543210"),
            "1000000000000000000",
            "0x",
            21000,
            20000000000,
            0,
            1,
        );

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(matches!(result, Err(HttpDecoderError::InvalidHash(_))));
    }

    #[test]
    fn test_invalid_address_error() {
        let transaction = json!({
            "tx_execution_id": {
                "block_number": 1,
                "iteration_id": 1,
                "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            },
            "tx_env": {
                "caller": "invalid_address",
                "to": "0x9876543210987654321098765432109876543210",
                "value": "1000000000000000000",
                "data": "0x",
                "gas_limit": 21000,
                "gas_price": "20000000000",
                "nonce": 0,
                "chain_id": 1
            }
        });

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(matches!(
            result,
            Err(HttpDecoderError::InvalidTransaction(_))
        ));
    }

    #[test]
    fn test_invalid_value_error() {
        let transaction = json!({
            "tx_execution_id": {
                "block_number": 1,
                "iteration_id": 1,
                "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            },
            "tx_env": {
                "caller": "0x1234567890123456789012345678901234567890",
                "to": "0x9876543210987654321098765432109876543210",
                "value": "invalid_value",
                "data": "0x",
                "gas_limit": 21000,
                "gas_price": "20000000000",
                "nonce": 0,
                "chain_id": 1
            }
        });

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert_eq!(
            result.unwrap_err(),
            HttpDecoderError::InvalidTransaction(
                "invalid transactions array: invalid tx_env: invalid value: string \"invalid_value\", expected a 32 byte hex string"
                    .to_string()
            )
        );
    }

    #[test]
    fn test_invalid_gas_price_error() {
        let transaction = json!({
            "tx_execution_id": {
                "block_number": 1,
                "iteration_id": 1,
                "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            },
            "tx_env": {
                "caller": "0x1234567890123456789012345678901234567890",
                "to": "0x9876543210987654321098765432109876543210",
                "value": "1000000000000000000",
                "data": "0x",
                "gas_limit": 21000,
                "gas_price": "invalid_gas_price",
                "nonce": 0,
                "chain_id": 1
            }
        });

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert_eq!(
            result.unwrap_err(),
            HttpDecoderError::InvalidTransaction("invalid transactions array: invalid tx_env: invalid type: string \"invalid_gas_price\", expected u128".to_string())
        );
    }

    #[test]
    fn test_invalid_data_error() {
        let transaction = json!({
            "tx_execution_id": {
                "block_number": 1,
                "iteration_id": 1,
                "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            },
            "tx_env": {
                "caller": "0x1234567890123456789012345678901234567890",
                "to": "0x9876543210987654321098765432109876543210",
                "value": "1000000000000000000",
                "data": "invalid_hex_data",
                "gas_limit": 21000,
                "gas_price": "20000000000",
                "nonce": 0,
                "chain_id": 1
            }
        });

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert_eq!(
            result.unwrap_err(),
            HttpDecoderError::InvalidTransaction("invalid transactions array: invalid tx_env: invalid value: string \"invalid_hex_data\", expected a valid hex string".to_string())
        );
    }

    #[test]
    fn test_missing_transactions_field() {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "sendTransactions".to_string(),
            params: Some(json!({
                "invalid_field": "invalid_value"
            })),
            id: Some(json!(1)),
        };

        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert_eq!(
            result.unwrap_err(),
            HttpDecoderError::InvalidTransaction(
                "unknown field `invalid_field`, expected `transactions`".to_string()
            )
        );
    }

    #[test]
    fn test_zero_value_transaction() {
        let transaction = create_test_transaction_json(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "0x1234567890123456789012345678901234567890",
            Some("0x9876543210987654321098765432109876543210"),
            "0",
            "0x",
            21000,
            20000000000,
            0,
            1,
        );

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(result.is_ok());

        let contents = result.unwrap();
        assert_eq!(contents.len(), 1);
    }

    #[test]
    fn test_empty_data_transaction() {
        let transaction = create_test_transaction_json(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "0x1234567890123456789012345678901234567890",
            Some("0x9876543210987654321098765432109876543210"),
            "1000000000000000000",
            "0x",
            21000,
            20000000000,
            0,
            1,
        );

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(result.is_ok());

        let contents = result.unwrap();
        assert_eq!(contents.len(), 1);
    }

    #[test]
    fn test_large_values() {
        let transaction = create_test_transaction_json(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "0x1234567890123456789012345678901234567890",
            Some("0x9876543210987654321098765432109876543210"),
            "115792089237316195423570985008687907853269984665640564039457584007913129639935", // Max U256
            "0x",
            30000000,
            1000000000000, // 1000 gwei
            u64::MAX,
            u64::MAX,
        );

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(result.is_ok());

        let contents = result.unwrap();
        assert_eq!(contents.len(), 1);
    }

    #[test]
    fn test_mixed_valid_invalid_transactions() {
        let transactions = vec![
            create_test_transaction_json(
                "0x1111111111111111111111111111111111111111111111111111111111111111",
                "0x1111111111111111111111111111111111111111",
                Some("0x2222222222222222222222222222222222222222"),
                "1000000000000000000",
                "0x",
                21000,
                20000000000,
                0,
                1,
            ),
            json!({
                "tx_execution_id": {
                    "block_number": 1,
                    "iteration_id": 1,
                    "tx_hash": "invalid_hash"
                },
                "tx_env": {
                    "caller": "0x3333333333333333333333333333333333333333",
                    "to": "0x4444444444444444444444444444444444444444",
                    "value": "2000000000000000000",
                    "data": "0x",
                    "gas_limit": 21000,
                    "gas_price": "20000000000",
                    "nonce": 1,
                    "chain_id": 1
                }
            }),
        ];

        let request = create_test_request_json("sendTransactions", &transactions);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert_eq!(
            result.unwrap_err(),
            HttpDecoderError::InvalidHash("invalid transactions array: invalid tx_execution_id: invalid hash: invalid string length".to_string())
        );
    }

    #[test]
    fn test_long_hex_data() {
        let long_data = format!("0x{}", "a".repeat(10000));
        let transaction = create_test_transaction_json(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "0x1234567890123456789012345678901234567890",
            Some("0x9876543210987654321098765432109876543210"),
            "0",
            &long_data,
            1000000,
            20000000000,
            0,
            1,
        );

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(result.is_ok());

        let contents = result.unwrap();
        assert_eq!(contents.len(), 1);
    }

    #[test]
    fn test_malformed_hex_data() {
        let transaction = json!({
            "tx_execution_id": {
                "block_number": 1,
                "iteration_id": 1,
                "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            },
            "tx_env": {
                "caller": "0x1234567890123456789012345678901234567890",
                "to": "0x9876543210987654321098765432109876543210",
                "value": "1000000000000000000",
                "data": "0xgg", // Invalid hex
                "gas_limit": 21000,
                "gas_price": "20000000000",
                "nonce": 0,
                "chain_id": 1
            }
        });

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert_eq!(
            result.unwrap_err(),
            HttpDecoderError::InvalidTransaction("invalid transactions array: invalid tx_env: invalid value: string \"0xgg\", expected a valid hex string".to_string())
        );
    }

    #[test]
    fn test_calldata_without_0x_prefix() {
        let transaction = create_test_transaction_json(
            "0x1111111111111111111111111111111111111111111111111111111111111111",
            "0x1111111111111111111111111111111111111111",
            Some("0x2222222222222222222222222222222222222222"),
            "1000000000000000000",
            "a9059cbb",
            21000,
            20000000000,
            0,
            1,
        );

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        // This should still work as the decoder should handle missing 0x prefix
        assert!(result.is_ok());

        let contents = result.unwrap();
        assert_eq!(contents.len(), 1);
    }

    #[test]
    fn test_calldata_both_with_and_without_prefix() {
        let transactions = vec![
            create_test_transaction_json(
                "0x1111111111111111111111111111111111111111111111111111111111111111",
                "0x1111111111111111111111111111111111111111",
                Some("0x2222222222222222222222222222222222222222"),
                "1000000000000000000",
                "a9059cbb",
                21000,
                20000000000,
                0,
                1,
            ),
            create_test_transaction_json(
                "0x1111111111111111111111111111111111111111111111111111111111111111",
                "0x1111111111111111111111111111111111111111",
                Some("0x2222222222222222222222222222222222222222"),
                "1000000000000000000",
                "0xa9059cbb",
                21000,
                20000000000,
                0,
                1,
            ),
        ];

        let request = create_test_request_json("sendTransactions", &transactions);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(result.is_ok());

        let contents = result.unwrap();
        assert_eq!(contents.len(), 2);
    }

    #[test]
    fn test_missing_tx_env_field() {
        let transaction = json!({
            "tx_execution_id": {
                "block_number": 1,
                "iteration_id": 1,
                "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            }
            // Missing tx_env field
        });

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(matches!(result, Err(HttpDecoderError::MissingTxEnv)));
    }

    #[test]
    fn test_missing_tx_execution_id_field() {
        let transaction = json!({
            "tx_env": {
                "caller": "0x1234567890123456789012345678901234567890",
                "kind": "0x9876543210987654321098765432109876543210",
                "value": "1000000000000000000",
                "data": "0x",
                "gas_limit": 21000,
                "gas_price": 20000000000_u64,
                "nonce": 0,
                "chain_id": 1,
                "tx_type": 0,
                "access_list": [],
                "gas_priority_fee": null,
                "blob_hashes": [],
                "max_fee_per_blob_gas": 0,
                "authorization_list": []
            }
            // Missing tx_execution_id field
        });

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(matches!(
            result,
            Err(HttpDecoderError::InvalidTransaction(_))
        ));
    }

    #[test]
    fn test_missing_hash_field() {
        let transaction = json!({
            "tx_execution_id": {
                "block_number": 1,
                // Missing hash field
                "iteration_id": 1
            },
            "tx_env": {
                "caller": "0x1234567890123456789012345678901234567890",
                "kind": "0x9876543210987654321098765432109876543210",
                "value": "1000000000000000000",
                "data": "0x",
                "gas_limit": 21000,
                "gas_price": 20000000000_u64,
                "nonce": 0,
                "chain_id": 1,
                "tx_type": 0,
                "access_list": [],
                "gas_priority_fee": null,
                "blob_hashes": [],
                "max_fee_per_blob_gas": 0,
                "authorization_list": []
            }
        });

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(matches!(
            result,
            Err(HttpDecoderError::InvalidTransaction(_))
        ));
    }

    #[test]
    fn test_invalid_nonce_error() {
        let transaction = json!({
            "tx_execution_id": {
                "block_number": 1,
                "iteration_id": 1,
                "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            },
            "tx_env": {
                "caller": "0x1234567890123456789012345678901234567890",
                "kind": "0x9876543210987654321098765432109876543210",
                "value": "1000000000000000000",
                "data": "0x",
                "gas_limit": 21000,
                "gas_price": 20000000000_u64,
                "nonce": "invalid_nonce",
                "chain_id": 1,
                "tx_type": 0,
                "access_list": [],
                "gas_priority_fee": null,
                "blob_hashes": [],
                "max_fee_per_blob_gas": 0,
                "authorization_list": []
            }
        });

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert_eq!(
            result.unwrap_err(),
            HttpDecoderError::InvalidTransaction("invalid transactions array: invalid tx_env: invalid type: string \"invalid_nonce\", expected u64".to_string())
        );
    }

    #[test]
    fn test_invalid_gas_limit_error() {
        let transaction = json!({
            "tx_execution_id": {
                "block_number": 1,
                "iteration_id": 1,
                "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            },
            "tx_env": {
                "caller": "0x1234567890123456789012345678901234567890",
                "kind": "0x9876543210987654321098765432109876543210",
                "value": "1000000000000000000",
                "data": "0x",
                "gas_limit": "invalid_gas_limit",
                "gas_price": 20000000000_u64,
                "nonce": 0,
                "chain_id": 1,
                "tx_type": 0,
                "access_list": [],
                "gas_priority_fee": null,
                "blob_hashes": [],
                "max_fee_per_blob_gas": 0,
                "authorization_list": []
            }
        });

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert_eq!(
            result.unwrap_err(),
            HttpDecoderError::InvalidTransaction("invalid transactions array: invalid tx_env: invalid type: string \"invalid_gas_limit\", expected u64".to_string())
        );
    }

    #[test]
    fn test_invalid_chain_id_error() {
        let transaction = json!({
            "tx_execution_id": {
                "block_number": 1,
                "iteration_id": 1,
                "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            },
            "tx_env": {
                "caller": "0x1234567890123456789012345678901234567890",
                "kind": "0x9876543210987654321098765432109876543210",
                "value": "1000000000000000000",
                "data": "0x",
                "gas_limit": 21000,
                "gas_price": 20000000000_u64,
                "nonce": 0,
                "chain_id": "invalid_chain_id",
                "tx_type": 0,
                "access_list": [],
                "gas_priority_fee": null,
                "blob_hashes": [],
                "max_fee_per_blob_gas": 0,
                "authorization_list": []
            }
        });

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert_eq!(
            result.unwrap_err(),
            HttpDecoderError::InvalidTransaction("invalid transactions array: invalid tx_env: invalid type: string \"invalid_chain_id\", expected u64".to_string())
        );
    }

    #[test]
    fn test_negative_nonce_error() {
        let transaction = json!({
            "tx_execution_id": {
                "block_number": 1,
                "iteration_id": 1,
                "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            },
            "tx_env": {
                "caller": "0x1234567890123456789012345678901234567890",
                "kind": "0x9876543210987654321098765432109876543210",
                "value": "1000000000000000000",
                "data": "0x",
                "gas_limit": 21000,
                "gas_price": "20000000000",
                "nonce": -1,
                "chain_id": 1,
                "tx_type": 0,
                "access_list": [],
                "gas_priority_fee": null,
                "blob_hashes": [],
                "max_fee_per_blob_gas": 0,
                "authorization_list": []
            }
        });

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(matches!(
            result,
            Err(HttpDecoderError::InvalidTransaction(_))
        ));
    }

    #[test]
    fn test_invalid_to_address_error() {
        let transaction = json!({
            "tx_execution_id": {
                "block_number": 1,
                "iteration_id": 1,
                "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            },
            "tx_env": {
                "caller": "0x1234567890123456789012345678901234567890",
                "kind": "invalid_to_address",
                "value": "1000000000000000000",
                "data": "0x",
                "gas_limit": 21000,
                "gas_price": 2000000000_u64,
                "nonce": 0,
                "chain_id": 1,
                "tx_type": 0,
                "access_list": [],
                "gas_priority_fee": null,
                "blob_hashes": [],
                "max_fee_per_blob_gas": 0,
                "authorization_list": []
            }
        });

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(matches!(
            result,
            Err(HttpDecoderError::InvalidTransaction(_))
        ));
    }

    #[test]
    fn test_invalid_hash_in_transactions_array() {
        let transaction = json!({
            "tx_execution_id": {
                "block_number": 1,
                "iteration_id": 1,
                "tx_hash": "not_64_chars"
            },
            "tx_env": {
                "caller": "0x1234567890123456789012345678901234567890",
                "kind": "0x9876543210987654321098765432109876543210",
                "value": "1000000000000000000",
                "data": "0x",
                "gas_limit": 21000,
                "gas_price": 20000000000_u64,
                "nonce": 0,
                "chain_id": 1,
                "tx_type": 0,
                "access_list": [],
                "gas_priority_fee": null,
                "blob_hashes": [],
                "max_fee_per_blob_gas": 0,
                "authorization_list": []
            }
        });

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert_eq!(
            result.unwrap_err(),
            HttpDecoderError::InvalidHash("invalid transactions array: invalid tx_execution_id: invalid hash: invalid string length".to_string())
        );
    }

    // BlockEnv Tests
    #[test]
    fn test_decode_valid_block_env() {
        let valid_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "block_env": {
                    "number": 123456u64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 1234567890u64,
                    "gas_limit": 30000000u64,
                    "basefee": 1000000000u64,
                    "difficulty": "0x0",
                    "prevrandao": "0x0000000000000000000000000000000000000000000000000000000000000000",
                },
                "last_tx_hash": "0x2222222222222222222222222222222222222222222222222222222222222222",
                "n_transactions": 1000u64,
                "selected_iteration_id": 42u64
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
        assert_eq!(queue_block_env.selected_iteration_id, Some(42));
    }

    #[test]
    fn test_decode_minimal_block_env() {
        let minimal_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "block_env": {
                    "number": 1u64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 0u64,
                    "gas_limit": 0u64,
                    "basefee": 0u64,
                    "difficulty": "0x0"
                }
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

        let queue_block_env = block_env_result.unwrap();
        let block_env = queue_block_env.block_env;
        assert_eq!(block_env.number, 1u64);
        assert_eq!(block_env.basefee, 0u64);
        assert_eq!(block_env.gas_limit, 0u64);
        assert_eq!(block_env.timestamp, 0u64);
        assert_eq!(queue_block_env.selected_iteration_id, None);
    }

    #[test]
    fn test_decode_block_env_with_blob_excess_gas() {
        let with_blob_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "block_env": {
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
                "selected_iteration_id": 7u64
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
        assert_eq!(queue_block_env.selected_iteration_id, Some(7));
    }

    #[test]
    fn test_decode_block_env_missing_params() {
        let no_params_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(no_params_request).unwrap();

        assert!(request.params.is_none(), "Request should have no params");
    }

    #[test]
    fn test_decode_block_env_invalid_number_type() {
        let invalid_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": "invalid_number",
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 0u64,
                "gas_limit": 0u64,
                "basefee": 0u64,
                "difficulty": "0x0"
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(invalid_request).unwrap();

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
                "number": -1i64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 0u64,
                "gas_limit": 0u64,
                "basefee": 0u64,
                "difficulty": "0x0"
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(negative_values_request).unwrap();

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
                "block_env": {
                    "number": 123456u64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 1234567890u64,
                    "gas_limit": 30000000u64,
                    "basefee": 1000000000u64,
                    "difficulty": "0x0",
                    "extra_field": "should_be_ignored",
                },
                "another_extra": 42
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(extra_fields_request).unwrap();

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
                "block_env": {
                    "number": 123456u64,
                    "beneficiary": "0x1234567890123456789012345678901234567890",
                    "timestamp": 1234567890u64,
                    "gas_limit": 30000000u64,
                    "basefee": 1000000000u64,
                    "difficulty": "0x1e240",
                    "prevrandao": "0x1234567890123456789012345678901234567890123456789012345678901234",
                },
                "selected_iteration_id": 15u64
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(hex_values_request).unwrap();

        let block_env_result = serde_json::from_value::<QueueBlockEnv>(request.params.unwrap());
        assert!(
            block_env_result.is_ok(),
            "Should successfully deserialize BlockEnv with hex values"
        );

        let queue_block_env = block_env_result.unwrap();
        let block_env = queue_block_env.block_env;
        assert_eq!(block_env.number, 123456u64);
        assert_eq!(block_env.timestamp, 1234567890u64);
        assert!(
            block_env.prevrandao.is_some(),
            "prevrandao should be present"
        );
        assert_eq!(block_env.difficulty, U256::from(123456u64));
        assert_eq!(queue_block_env.selected_iteration_id, Some(15));
    }

    #[test]
    fn test_decode_block_env_zero_values() {
        let zero_values_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "block_env": {
                    "number": 0u64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 0u64,
                    "gas_limit": 0u64,
                    "basefee": 0u64,
                    "difficulty": "0x0"
                }
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(zero_values_request).unwrap();

        let block_env_result = serde_json::from_value::<QueueBlockEnv>(request.params.unwrap());
        assert!(
            block_env_result.is_ok(),
            "Should successfully deserialize BlockEnv with zero values"
        );

        let queue_block_env = block_env_result.unwrap();
        let block_env = queue_block_env.block_env;
        assert_eq!(block_env.number, 0u64);
        assert_eq!(block_env.basefee, 0u64);
        assert_eq!(block_env.gas_limit, 0u64);
        assert_eq!(block_env.timestamp, 0u64);
        assert_eq!(block_env.difficulty, U256::ZERO);
        assert_eq!(block_env.beneficiary, Address::ZERO);
        assert_eq!(queue_block_env.selected_iteration_id, None);
    }

    #[test]
    fn test_decode_block_env_max_values() {
        let max_values_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "block_env": {
                    "number": u64::MAX,
                    "beneficiary": "0xffffffffffffffffffffffffffffffffffffffff",
                    "timestamp": u64::MAX,
                    "gas_limit": u64::MAX,
                    "basefee": u64::MAX,
                    "difficulty": "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                },
                "selected_iteration_id": u64::MAX
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(max_values_request).unwrap();

        let block_env_result = serde_json::from_value::<QueueBlockEnv>(request.params.unwrap());
        assert!(
            block_env_result.is_ok(),
            "Should successfully deserialize BlockEnv with maximum values"
        );

        let queue_block_env = block_env_result.unwrap();
        let block_env = queue_block_env.block_env;
        assert_eq!(block_env.number, u64::MAX);
        assert_eq!(block_env.basefee, u64::MAX);
        assert_eq!(block_env.gas_limit, u64::MAX);
        assert_eq!(block_env.timestamp, u64::MAX);
        assert_eq!(block_env.difficulty, U256::MAX);
        assert_eq!(queue_block_env.selected_iteration_id, Some(u64::MAX));
    }

    #[test]
    fn test_decode_block_env_with_prevrandao() {
        let with_prevrandao_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "block_env": {
                    "number": 123456u64,
                    "beneficiary": "0x1234567890123456789012345678901234567890",
                    "timestamp": 1234567890u64,
                    "gas_limit": 30000000u64,
                    "basefee": 1000000000u64,
                    "difficulty": "0x0",
                    "prevrandao": "0x1234567890123456789012345678901234567890123456789012345678901234",
                 },
                "selected_iteration_id": 99u64
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

        let queue_block_env = block_env_result.unwrap();
        let block_env = queue_block_env.block_env;
        assert!(
            block_env.prevrandao.is_some(),
            "prevrandao should be present"
        );
        assert_eq!(block_env.number, 123456u64);
        assert_eq!(block_env.basefee, 1000000000u64);
        assert_eq!(queue_block_env.selected_iteration_id, Some(99));
    }

    #[test]
    fn test_decode_block_env_without_prevrandao() {
        let without_prevrandao_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "block_env": {
                    "number": 123456u64,
                    "beneficiary": "0x1234567890123456789012345678901234567890",
                    "timestamp": 1234567890u64,
                    "gas_limit": 30000000u64,
                    "basefee": 1000000000u64,
                    "difficulty": "0x0"
                }
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(without_prevrandao_request).unwrap();

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
                    "excess_blob_gas": "invalid",
                    "blob_gasprice": 2000u128
                }
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(invalid_blob_request).unwrap();

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
            selected_iteration_id: Some(88),
        };

        let serialized = serde_json::to_value(&block_env).unwrap();
        let deserialized = serde_json::from_value::<QueueBlockEnv>(serialized).unwrap();

        assert_eq!(block_env.block_env.number, deserialized.block_env.number);
        assert_eq!(
            block_env.block_env.beneficiary,
            deserialized.block_env.beneficiary
        );
        assert_eq!(
            block_env.block_env.timestamp,
            deserialized.block_env.timestamp
        );
        assert_eq!(
            block_env.block_env.gas_limit,
            deserialized.block_env.gas_limit
        );
        assert_eq!(block_env.block_env.basefee, deserialized.block_env.basefee);
        assert_eq!(
            block_env.block_env.difficulty,
            deserialized.block_env.difficulty
        );
        assert_eq!(
            block_env.block_env.prevrandao,
            deserialized.block_env.prevrandao
        );
        assert_eq!(
            block_env.block_env.blob_excess_gas_and_price,
            deserialized.block_env.blob_excess_gas_and_price
        );
    }

    #[test]
    fn test_decode_block_env_missing_required_fields() {
        let missing_fields_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(missing_fields_request).unwrap();

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
                }
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(partial_blob_request).unwrap();

        let block_env_result = serde_json::from_value::<BlockEnv>(request.params.unwrap());
        assert!(
            block_env_result.is_err(),
            "Should fail to deserialize BlockEnv with partial blob excess gas data"
        );
    }

    #[test]
    fn test_decode_block_env_u256_difficulty_variants() {
        let small_difficulty_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 0u64,
                "gas_limit": 0u64,
                "basefee": 0u64,
                "difficulty": "0x64"
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
                "block_env": {
                    "number": 123456u64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 1234567890u64,
                    "gas_limit": 30000000u64,
                    "basefee": 1000000000u64,
                    "difficulty": "0x0",
                    "prevrandao": "0x0000000000000000000000000000000000000000000000000000000000000000",
                },
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
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("last_tx_hash must be null, empty, or missing")
        );
    }

    #[test]
    fn test_decode_block_env_validation_error_null_hash_with_transactions() {
        let invalid_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "block_env": {
                    "number": 111111u64,
                    "beneficiary": "0x3333333333333333333333333333333333333333",
                    "timestamp": 2222222222u64,
                    "gas_limit": 35000000u64,
                    "basefee": 1500000000u64,
                    "difficulty": "0x0",
                    "prevrandao": "0x3333333333333333333333333333333333333333333333333333333333333333",
                },
                "last_tx_hash": null,
                "n_transactions": 10u64,
                "selected_iteration_id": 5u64
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
                "block_env": {
                    "number": 222222u64,
                    "beneficiary": "0x4444444444444444444444444444444444444444",
                    "timestamp": 3333333333u64,
                    "gas_limit": 40000000u64,
                    "basefee": 3000000000u64,
                    "difficulty": "0x0",
                    "prevrandao": "0x4444444444444444444444444444444444444444444444444444444444444444",
                },
                "last_tx_hash": "0x5555555555555555555555555555555555555555555555555555555555555555",
                "n_transactions": 0u64
            },
            "id": 5
        });

        let request: JsonRpcRequest = serde_json::from_value(request_with_zero).unwrap();
        let result = serde_json::from_value::<QueueBlockEnv>(request.params.unwrap());

        assert!(
            result.is_err(),
            "Should fail validation when n_transactions = 0 but last_tx_hash is present"
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
                "block_env":{
                    "number": 444444u64,
                    "beneficiary": "0x6666666666666666666666666666666666666666",
                    "timestamp": 5555555555u64,
                    "gas_limit": 55000000u64,
                    "basefee": 5000000000u64,
                    "difficulty": "0x0",
                    "prevrandao": "0x6666666666666666666666666666666666666666666666666666666666666666",
                },
                "last_tx_hash": "0x7777777777777777777777777777777777777777777777777777777777777777",
                "n_transactions": 18446744073709551615u64,
                "selected_iteration_id": 12345u64
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
        assert_eq!(queue_block_env.selected_iteration_id, Some(12345));
    }

    #[test]
    fn test_queue_block_env_serialization_round_trip() {
        let original_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "block_env": {
                    "number": 555555u64,
                    "beneficiary": "0x7777777777777777777777777777777777777777",
                    "timestamp": 6666666666u64,
                    "gas_limit": 60000000u64,
                    "basefee": 6000000000u64,
                    "difficulty": "0x0",
                    "prevrandao": "0x7777777777777777777777777777777777777777777777777777777777777777",
                },
                "last_tx_hash": "0x8888888888888888888888888888888888888888888888888888888888888888",
                "n_transactions": 123u64,
                "selected_iteration_id": 777u64
            },
            "id": 8
        });

        let request: JsonRpcRequest = serde_json::from_value(original_request).unwrap();
        let queue_block_env: QueueBlockEnv =
            serde_json::from_value(request.params.unwrap()).unwrap();

        let serialized = serde_json::to_value(&queue_block_env).unwrap();

        let deserialized: QueueBlockEnv = serde_json::from_value(serialized).unwrap();

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
        assert_eq!(
            deserialized.selected_iteration_id,
            queue_block_env.selected_iteration_id
        );
    }

    #[test]
    fn test_block_env_missing_number() {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "block_env": {
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 1234567890u64,
                    "gas_limit": 30000000u64,
                    "basefee": 1000000000u64,
                    "difficulty": "0x0"
                }
            },
            "id": 1
        });

        let json_request: JsonRpcRequest = serde_json::from_value(request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&json_request);
        assert!(matches!(result, Err(HttpDecoderError::MissingBlockNumber)));
    }

    #[test]
    fn test_block_env_missing_beneficiary() {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "block_env": {
                    "number": 123456u64,
                    "timestamp": 1234567890u64,
                    "gas_limit": 30000000u64,
                    "basefee": 1000000000u64,
                    "difficulty": "0x0"
                }
            },
            "id": 1
        });

        let json_request: JsonRpcRequest = serde_json::from_value(request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&json_request);
        assert!(matches!(result, Err(HttpDecoderError::MissingBeneficiary)));
    }

    #[test]
    fn test_block_env_invalid_beneficiary() {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64,
                "beneficiary": "invalid_address",
                "timestamp": 1234567890u64,
                "gas_limit": 30000000u64,
                "basefee": 1000000000u64,
                "difficulty": "0x0"
            },
            "id": 1
        });

        let json_request: JsonRpcRequest = serde_json::from_value(request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&json_request);
        assert!(matches!(
            result,
            Err(HttpDecoderError::BlockEnvValidation(_))
        ));
    }

    #[test]
    fn test_block_env_missing_timestamp() {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "block_env": {
                    "number": 123456u64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "gas_limit": 30000000u64,
                    "basefee": 1000000000u64,
                    "difficulty": "0x0"
                }
            },
            "id": 1
        });

        let json_request: JsonRpcRequest = serde_json::from_value(request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&json_request);
        assert!(matches!(result, Err(HttpDecoderError::MissingTimestamp)));
    }

    #[test]
    fn test_block_env_invalid_timestamp() {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": "invalid_timestamp",
                "gas_limit": 30000000u64,
                "basefee": 1000000000u64,
                "difficulty": "0x0"
            },
            "id": 1
        });

        let json_request: JsonRpcRequest = serde_json::from_value(request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&json_request);
        assert!(matches!(
            result,
            Err(HttpDecoderError::BlockEnvValidation(_))
        ));
    }

    #[test]
    fn test_block_env_missing_gas_limit() {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "block_env": {
                    "number": 123456u64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 1234567890u64,
                    "basefee": 1000000000u64,
                    "difficulty": "0x0"
                }
            },
            "id": 1
        });

        let json_request: JsonRpcRequest = serde_json::from_value(request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&json_request);
        assert!(matches!(
            result,
            Err(HttpDecoderError::MissingBlockGasLimit)
        ));
    }

    #[test]
    fn test_block_env_invalid_gas_limit() {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 1234567890u64,
                "gas_limit": "invalid_gas_limit",
                "basefee": 1000000000u64,
                "difficulty": "0x0"
            },
            "id": 1
        });

        let json_request: JsonRpcRequest = serde_json::from_value(request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&json_request);
        assert!(matches!(
            result,
            Err(HttpDecoderError::BlockEnvValidation(_))
        ));
    }

    #[test]
    fn test_block_env_missing_basefee() {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "block_env": {
                    "number": 123456u64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 1234567890u64,
                    "gas_limit": 30000000u64,
                    "difficulty": "0x0"
                }
            },
            "id": 1
        });

        let json_request: JsonRpcRequest = serde_json::from_value(request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&json_request);
        assert!(matches!(result, Err(HttpDecoderError::MissingBasefee)));
    }

    #[test]
    fn test_block_env_invalid_basefee() {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 1234567890u64,
                "gas_limit": 30000000u64,
                "basefee": "invalid_basefee",
                "difficulty": "0x0"
            },
            "id": 1
        });

        let json_request: JsonRpcRequest = serde_json::from_value(request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&json_request);
        assert!(matches!(
            result,
            Err(HttpDecoderError::BlockEnvValidation(_))
        ));
    }

    #[test]
    fn test_block_env_missing_difficulty() {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "block_env": {
                    "number": 123456u64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 1234567890u64,
                    "gas_limit": 30000000u64,
                    "basefee": 1000000000u64
                }
            },
            "id": 1
        });

        let json_request: JsonRpcRequest = serde_json::from_value(request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&json_request);
        assert!(matches!(result, Err(HttpDecoderError::MissingDifficulty)));
    }

    #[test]
    fn test_block_env_invalid_difficulty() {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 1234567890u64,
                "gas_limit": 30000000u64,
                "basefee": 1000000000u64,
                "difficulty": "invalid_difficulty"
            },
            "id": 1
        });

        let json_request: JsonRpcRequest = serde_json::from_value(request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&json_request);
        assert!(matches!(
            result,
            Err(HttpDecoderError::BlockEnvValidation(_))
        ));
    }

    #[test]
    fn test_block_env_invalid_prevrandao() {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 1234567890u64,
                "gas_limit": 30000000u64,
                "basefee": 1000000000u64,
                "difficulty": "0x0",
                "prevrandao": "invalid_hash"
            },
            "id": 1
        });

        let json_request: JsonRpcRequest = serde_json::from_value(request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&json_request);
        assert!(matches!(
            result,
            Err(HttpDecoderError::BlockEnvValidation(_))
        ));
    }

    #[test]
    fn test_block_env_invalid_blob_excess_gas_and_price() {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 1234567890u64,
                "gas_limit": 30000000u64,
                "basefee": 1000000000u64,
                "difficulty": "0x0",
                "blob_excess_gas_and_price": "invalid"
            },
            "id": 1
        });

        let json_request: JsonRpcRequest = serde_json::from_value(request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&json_request);
        assert!(matches!(
            result,
            Err(HttpDecoderError::BlockEnvValidation(_))
        ));
    }

    #[test]
    fn test_block_env_invalid_last_tx_hash() {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "block_env": {
                    "number": 123456u64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 1234567890u64,
                    "gas_limit": 30000000u64,
                    "basefee": 1000000000u64,
                    "difficulty": "0x0",
                },
                "last_tx_hash": "invalid_hash",
                "n_transactions": 5
            },
            "id": 1
        });

        let json_request: JsonRpcRequest = serde_json::from_value(request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&json_request);
        assert!(matches!(
            result,
            Err(HttpDecoderError::InvalidLastTxHash(_))
        ));
    }

    #[test]
    fn test_block_env_invalid_n_transactions() {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "block_env": {
                    "number": 123456u64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 1234567890u64,
                    "gas_limit": 30000000u64,
                    "basefee": 1000000000u64,
                    "difficulty": "0x0",
                },
                "n_transactions": "invalid"
            },
            "id": 1
        });

        let json_request: JsonRpcRequest = serde_json::from_value(request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&json_request);
        assert!(matches!(
            result,
            Err(HttpDecoderError::InvalidNTransactions(_))
        ));
    }

    #[test]
    fn test_block_env_validation_error() {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": 123456u64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 1234567890u64,
                "gas_limit": 30000000u64,
                "basefee": 1000000000u64,
                "difficulty": "0x0",
                "last_tx_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                "n_transactions": 0
            },
            "id": 1
        });

        let json_request: JsonRpcRequest = serde_json::from_value(request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&json_request);
        assert!(matches!(
            result,
            Err(HttpDecoderError::BlockEnvValidation(_))
        ));
    }

    #[test]
    fn test_block_env_invalid_block_number() {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": "not_a_number",
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 1234567890u64,
                "gas_limit": 30000000u64,
                "basefee": 1000000000u64,
                "difficulty": "0x0"
            },
            "id": 1
        });

        let json_request: JsonRpcRequest = serde_json::from_value(request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&json_request);
        assert!(matches!(
            result,
            Err(HttpDecoderError::BlockEnvValidation(_))
        ));
    }

    // Reorg Tests
    #[test]
    fn test_decode_reorg_valid() {
        let valid_reorg_request = json!({
            "jsonrpc": "2.0",
            "method": "reorg",
            "params": {
                "block_number": 100u64,
                "iteration_id": 1u64,
                "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(valid_reorg_request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(
            result.is_ok(),
            "Should successfully decode valid reorg request"
        );
        let contents = result.unwrap();
        assert_eq!(contents.len(), 1, "Should return exactly one queue content");

        match &contents[0] {
            TxQueueContents::Reorg(hash, _) => {
                assert_eq!(
                    *hash,
                    B256::from_str(
                        "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                    )
                    .unwrap(),
                    "Should extract correct transaction hash"
                );
            }
            _ => panic!("Expected Reorg variant, got {:?}", contents[0]),
        }
    }

    #[test]
    fn test_decode_reorg_without_0x_prefix() {
        let reorg_request_no_prefix = json!({
            "jsonrpc": "2.0",
            "method": "reorg",
            "params": {
                "block_number": 100u64,
                "iteration_id": 1u64,
                "tx_hash": "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(reorg_request_no_prefix).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(
            result.is_ok(),
            "Should successfully decode reorg request without 0x prefix"
        );
        let contents = result.unwrap();

        match &contents[0] {
            TxQueueContents::Reorg(hash, _) => {
                assert_eq!(
                    *hash,
                    B256::from_str(
                        "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                    )
                    .unwrap(),
                    "Should handle hash without 0x prefix"
                );
            }
            _ => panic!("Expected Reorg variant"),
        }
    }

    #[test]
    fn test_decode_reorg_missing_params() {
        let no_params_request = json!({
            "jsonrpc": "2.0",
            "method": "reorg",
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(no_params_request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_err(), "Should fail when params are missing");
        assert!(
            matches!(result.unwrap_err(), HttpDecoderError::MissingParams),
            "Should return MissingParams error"
        );
    }

    #[test]
    fn test_decode_reorg_missing_tx_execution_ids_field() {
        let missing_field_request = json!({
            "jsonrpc": "2.0",
            "method": "reorg",
            "params": {
                "someOtherField": "value"
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(missing_field_request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(
            result.is_err(),
            "Should fail when tx_execution_id field is missing"
        );
        assert!(
            matches!(result.unwrap_err(), HttpDecoderError::ReorgValidation(_)),
            "Should return ReorgValidation error"
        );
    }

    #[test]
    fn test_decode_reorg_missing_hash_in_tx() {
        let missing_hash_request = json!({
            "jsonrpc": "2.0",
            "method": "reorg",
            "params": {
                "block_number": 100u64,
                "iteration_id": 1u64
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(missing_hash_request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_err(), "Should fail when hash is missing");
        assert!(
            matches!(result.unwrap_err(), HttpDecoderError::ReorgValidation(_)),
            "Should return ReorgValidation error"
        );
    }

    #[test]
    fn test_decode_reorg_invalid_hash_format() {
        let invalid_hash_request = json!({
            "jsonrpc": "2.0",
            "method": "reorg",
            "params": {
                "block_number": 100u64,
                "iteration_id": 1u64,
                "tx_hash": "not_a_valid_hash"
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(invalid_hash_request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_err(), "Should fail with invalid hash format");
        assert!(
            matches!(result.unwrap_err(), HttpDecoderError::ReorgValidation(_)),
            "Should return ReorgValidation error"
        );
    }

    #[test]
    fn test_decode_reorg_hash_wrong_length() {
        let wrong_length_request = json!({
            "jsonrpc": "2.0",
            "method": "reorg",
            "params": {
                "block_number": 100u64,
                "iteration_id": 1u64,
                "tx_hash": "0x1234"
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(wrong_length_request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_err(), "Should fail with wrong hash length");
        assert!(
            matches!(result.unwrap_err(), HttpDecoderError::ReorgValidation(_)),
            "Should return ReorgValidation error"
        );
    }

    #[test]
    fn test_decode_reorg_hash_not_string() {
        let non_string_hash_request = json!({
            "jsonrpc": "2.0",
            "method": "reorg",
            "params": {
                "block_number": 100u64,
                "iteration_id": 1u64,
                "tx_hash": 12345
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(non_string_hash_request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_err(), "Should fail when hash is not a string");
        assert!(
            matches!(result.unwrap_err(), HttpDecoderError::ReorgValidation(_)),
            "Should return ReorgValidation error"
        );
    }

    #[test]
    fn test_decode_reorg_null_hash() {
        let null_hash_request = json!({
            "jsonrpc": "2.0",
            "method": "reorg",
            "params": {
                "block_number": 100u64,
                "iteration_id": 1u64,
                "tx_hash": null
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(null_hash_request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_err(), "Should fail when hash is null");
        assert!(
            matches!(result.unwrap_err(), HttpDecoderError::ReorgValidation(_)),
            "Should return ReorgValidation error"
        );
    }

    #[test]
    fn test_decode_reorg_uppercase_hex() {
        let uppercase_request = json!({
            "jsonrpc": "2.0",
            "method": "reorg",
            "params": {
                "block_number": 100u64,
                "iteration_id": 1u64,
                "tx_hash": "0x1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF"
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(uppercase_request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_ok(), "Should handle uppercase hex characters");
    }

    #[test]
    fn test_decode_reorg_mixed_case_hex() {
        let mixed_case_request = json!({
            "jsonrpc": "2.0",
            "method": "reorg",
            "params": {
                "block_number": 100u64,
                "iteration_id": 1u64,
                "tx_hash": "0x1234567890AbCdEf1234567890aBcDeF1234567890AbCdEf1234567890aBcDeF"
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(mixed_case_request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_ok(), "Should handle mixed case hex characters");
    }

    #[test]
    fn test_decode_reorg_empty_string_hash() {
        let empty_hash_request = json!({
            "jsonrpc": "2.0",
            "method": "reorg",
            "params": {
                "block_number": 100u64,
                "iteration_id": 1u64,
                "tx_hash": ""
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(empty_hash_request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_err(), "Should fail with empty hash string");
        assert!(matches!(
            result.unwrap_err(),
            HttpDecoderError::ReorgValidation(_)
        ));
    }

    #[test]
    fn test_decode_reorg_0x_only_hash() {
        let ox_only_request = json!({
            "jsonrpc": "2.0",
            "method": "reorg",
            "params": {
                "block_number": 100u64,
                "iteration_id": 1u64,
                "tx_hash": "0x"
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(ox_only_request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_err(), "Should fail with 0x only hash");
        assert!(matches!(
            result.unwrap_err(),
            HttpDecoderError::ReorgValidation(_)
        ));
    }

    #[test]
    fn test_decode_reorg_missing_block_number() {
        let missing_block_number_request = json!({
            "jsonrpc": "2.0",
            "method": "reorg",
            "params": {
                "iteration_id": 1u64,
                "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(missing_block_number_request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_err(), "Should fail when block_number is missing");
        assert!(
            matches!(result.unwrap_err(), HttpDecoderError::ReorgValidation(_)),
            "Should return ReorgValidation error"
        );
    }

    #[test]
    fn test_decode_reorg_missing_iteration_id() {
        let missing_iteration_id_request = json!({
            "jsonrpc": "2.0",
            "method": "reorg",
            "params": {
                "tx_execution_id": {
                    "block_number": 100u64,
                    "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                }
            },
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(missing_iteration_id_request).unwrap();
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_err(), "Should fail when iteration_id is missing");
        assert!(
            matches!(result.unwrap_err(), HttpDecoderError::ReorgValidation(_)),
            "Should return ReorgValidation error"
        );
    }

    #[test]
    fn test_decode_reorg_with_various_block_numbers() {
        let test_cases = vec![
            (0u64, "zero block number"),
            (1u64, "block number 1"),
            (999999u64, "large block number"),
            (u64::MAX, "max block number"),
        ];

        for (block_number, description) in test_cases {
            let request = json!({
                "jsonrpc": "2.0",
                "method": "reorg",
                "params": {
                    "block_number": block_number,
                    "iteration_id": 1u64,
                    "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                },
                "id": 1
            });

            let json_request: JsonRpcRequest = serde_json::from_value(request).unwrap();
            let result = HttpTransactionDecoder::to_tx_queue_contents(&json_request);

            assert!(result.is_ok(), "Should succeed with {description}");
        }
    }
}
