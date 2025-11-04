#![allow(clippy::match_wildcard_for_single_variants)]
use crate::{
    engine::queue::NewIteration,
    transport::{
        decoder::{
            Decoder,
            HttpDecoderError,
            HttpTransactionDecoder,
            TxQueueContents,
        },
        http::server::JsonRpcRequest,
    },
};
use alloy::primitives::{
    Address,
    U256,
};
use assertion_executor::primitives::{
    B256,
    BlockEnv,
};
use revm::context_interface::block::BlobExcessGasAndPrice;
use serde_json::json;
use std::str::FromStr;

// ============================================================================
// Test Helpers
// ============================================================================

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

fn create_test_request_json(method: &str, transactions: &[serde_json::Value]) -> JsonRpcRequest {
    JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: method.to_string(),
        params: Some(json!({
            "transactions": transactions
        })),
        id: Some(json!(1)),
    }
}

// ============================================================================
// Transaction Tests (sendTransactions method)
// ============================================================================

#[test]
fn test_transaction_decoding_happy_path_variants() {
    let test_cases = vec![
        ("basic_transfer", "1000000000000000000", "0x"),
        ("zero_value", "0", "0x"),
        ("with_calldata", "500000000000000000", "0xa9059cbb"),
    ];

    for (name, value, data) in test_cases {
        let transaction = create_test_transaction_json(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "0x1234567890123456789012345678901234567890",
            Some("0x9876543210987654321098765432109876543210"),
            value,
            data,
            21000,
            20000000000,
            0,
            1,
        );

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_ok(), "Failed for case: {name}");
        assert_eq!(result.unwrap().len(), 1);
    }
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
    assert_eq!(result.unwrap().len(), 2);
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
            100,
            1,
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
            100,
            2,
        ),
    ];

    let request = create_test_request_json("sendTransactions", &transactions);
    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 2);
}

#[test]
fn test_contract_creation_transactions() {
    let transactions = vec![
        // Contract creation with None as 'to'
        create_test_transaction_json(
            "0x1111111111111111111111111111111111111111111111111111111111111111",
            "0x1111111111111111111111111111111111111111",
            None,
            "0",
            "0x608060405234801561001057600080fd5b50",
            1000000,
            20000000000,
            0,
            1,
        ),
        // Contract creation with zero address
        create_test_transaction_json(
            "0x2222222222222222222222222222222222222222222222222222222222222222",
            "0x2222222222222222222222222222222222222222",
            Some("0x0000000000000000000000000000000000000000"),
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
    assert_eq!(result.unwrap().len(), 2);
}

#[test]
fn test_contract_call_with_full_calldata() {
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
    assert_eq!(result.unwrap().len(), 1);
}

#[test]
fn test_large_values_and_limits() {
    let transaction = create_test_transaction_json(
        "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        "0x1234567890123456789012345678901234567890",
        Some("0x9876543210987654321098765432109876543210"),
        "115792089237316195423570985008687907853269984665640564039457584007913129639935", // Max U256
        "0x",
        30000000,
        1000000000000,
        u64::MAX,
        u64::MAX,
    );

    let request = create_test_request_json("sendTransactions", &[transaction]);
    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 1);
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
    assert_eq!(result.unwrap().len(), 1);
}

#[test]
fn test_calldata_format_handling() {
    let test_cases = vec![
        ("with_prefix", "0xa9059cbb"),
        ("without_prefix", "a9059cbb"),
    ];

    for (name, data) in test_cases {
        let transaction = create_test_transaction_json(
            "0x1111111111111111111111111111111111111111111111111111111111111111",
            "0x1111111111111111111111111111111111111111",
            Some("0x2222222222222222222222222222222222222222"),
            "1000000000000000000",
            data,
            21000,
            20000000000,
            0,
            1,
        );

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_ok(), "Failed for case: {name}");
    }
}

// ============================================================================
// Transaction Error Tests
// ============================================================================

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
fn test_invalid_transaction_hash() {
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
fn test_invalid_transaction_fields() {
    let test_cases = vec![
        (
            "invalid_address",
            json!({
                "tx_execution_id": {
                    "block_number": 1,
                    "iteration_id": 1,
                    "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                },
                "tx_env": {
                    "caller": "invalid_address",
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
            }),
        ),
        (
            "invalid_value",
            json!({
                "tx_execution_id": {
                    "block_number": 1,
                    "iteration_id": 1,
                    "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                },
                "tx_env": {
                    "caller": "0x1234567890123456789012345678901234567890",
                    "kind": "0x9876543210987654321098765432109876543210",
                    "value": "invalid_value",
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
            }),
        ),
        (
            "invalid_gas_price",
            json!({
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
                    "gas_price": "invalid_gas_price",
                    "nonce": 0,
                    "chain_id": 1,
                    "tx_type": 0,
                    "access_list": [],
                    "gas_priority_fee": null,
                    "blob_hashes": [],
                    "max_fee_per_blob_gas": 0,
                    "authorization_list": []
                }
            }),
        ),
        (
            "invalid_data",
            json!({
                "tx_execution_id": {
                    "block_number": 1,
                    "iteration_id": 1,
                    "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                },
                "tx_env": {
                    "caller": "0x1234567890123456789012345678901234567890",
                    "kind": "0x9876543210987654321098765432109876543210",
                    "value": "1000000000000000000",
                    "data": "invalid_hex_data",
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
            }),
        ),
        (
            "malformed_hex",
            json!({
                "tx_execution_id": {
                    "block_number": 1,
                    "iteration_id": 1,
                    "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                },
                "tx_env": {
                    "caller": "0x1234567890123456789012345678901234567890",
                    "kind": "0x9876543210987654321098765432109876543210",
                    "value": "1000000000000000000",
                    "data": "0xgg",
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
            }),
        ),
        (
            "invalid_nonce",
            json!({
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
            }),
        ),
        (
            "invalid_gas_limit",
            json!({
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
            }),
        ),
        (
            "invalid_chain_id",
            json!({
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
            }),
        ),
        (
            "negative_nonce",
            json!({
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
                    "nonce": -1,
                    "chain_id": 1,
                    "tx_type": 0,
                    "access_list": [],
                    "gas_priority_fee": null,
                    "blob_hashes": [],
                    "max_fee_per_blob_gas": 0,
                    "authorization_list": []
                }
            }),
        ),
        (
            "invalid_to_address",
            json!({
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
            }),
        ),
    ];

    for (name, transaction) in test_cases {
        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(result.is_err(), "Should fail for: {name}");
    }
}

#[test]
fn test_missing_transaction_structure_fields() {
    let test_cases = vec![
        (
            "missing_tx_env",
            json!({
                "tx_execution_id": {
                    "block_number": 1,
                    "iteration_id": 1,
                    "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                }
            }),
            HttpDecoderError::MissingTxEnv,
        ),
        (
            "missing_tx_execution_id",
            json!({
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
            }),
            HttpDecoderError::InvalidTransaction(
                "invalid transactions array: missing field `tx_execution_id`".to_string(),
            ),
        ),
        (
            "missing_hash",
            json!({
                "tx_execution_id": {
                    "block_number": 1,
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
            }),
            HttpDecoderError::InvalidTransaction(
                "invalid transactions array: invalid tx_execution_id: missing field `tx_hash`"
                    .to_string(),
            ),
        ),
    ];

    for (name, transaction, expected_error) in test_cases {
        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert_eq!(result.unwrap_err(), expected_error, "Failed for: {name}");
    }
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
        HttpDecoderError::InvalidHash(
            "invalid transactions array: invalid tx_execution_id: invalid tx_hash: invalid string length"
                .to_string()
        )
    );
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
                "kind": "0x4444444444444444444444444444444444444444",
                "value": "2000000000000000000",
                "data": "0x",
                "gas_limit": 21000,
                "gas_price": 20000000000_u64,
                "nonce": 1,
                "chain_id": 1,
                "tx_type": 0,
                "access_list": [],
                "gas_priority_fee": null,
                "blob_hashes": [],
                "max_fee_per_blob_gas": 0,
                "authorization_list": []
            }
        }),
    ];

    let request = create_test_request_json("sendTransactions", &transactions);
    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
    assert!(matches!(result, Err(HttpDecoderError::InvalidHash(_))));
}

fn decode_block_env_request(
    params: serde_json::Value,
) -> Result<Vec<TxQueueContents>, HttpDecoderError> {
    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "sendBlockEnv".to_string(),
        params: Some(params),
        id: Some(json!(1)),
    };

    HttpTransactionDecoder::to_tx_queue_contents(&request)
}

// ============================================================================
// BlockEnv Tests - Direct Deserialization
// ============================================================================

#[test]
fn test_block_env_deserialization_valid() {
    let params = json!({
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
    });

    let result = decode_block_env_request(params).expect("block env decoding should succeed");
    assert_eq!(
        result.len(),
        2,
        "Expected commit head + new iteration events"
    );

    let commit_head = match &result[0] {
        TxQueueContents::CommitHead(commit_head, _) => commit_head,
        other => panic!("Expected CommitHead event, got {other:?}"),
    };
    assert_eq!(commit_head.block_number, 123456);
    assert_eq!(commit_head.n_transactions, 1000);
    assert_eq!(commit_head.iteration_id(), 42);
    assert_eq!(
        commit_head.last_tx_hash,
        Some(
            B256::from_str("0x2222222222222222222222222222222222222222222222222222222222222222")
                .unwrap()
        )
    );

    let new_iteration = match &result[1] {
        TxQueueContents::NewIteration(iteration, _) => iteration,
        other => panic!("Expected NewIteration event, got {other:?}"),
    };
    assert_eq!(new_iteration.iteration_id, 42);
    assert_eq!(new_iteration.block_env.number, 123456);
    assert_eq!(new_iteration.block_env.basefee, 1_000_000_000);
    assert_eq!(new_iteration.block_env.gas_limit, 30_000_000);
    assert_eq!(new_iteration.block_env.timestamp, 1_234_567_890);
    assert!(new_iteration.block_env.prevrandao.is_some());
}

#[test]
fn test_block_env_with_optional_fields() {
    let params = json!({
        "block_env": {
            "number": 123456u64,
            "beneficiary": "0x1234567890123456789012345678901234567890",
            "timestamp": 1234567890u64,
            "gas_limit": 30000000u64,
            "basefee": 1000000000u64,
            "difficulty": "0x0",
            "prevrandao": "0x1234567890123456789012345678901234567890123456789012345678901234",
            "blob_excess_gas_and_price": {
                "excess_blob_gas": 1000u64,
                "blob_gasprice": 2000u128
            }
        },
        "selected_iteration_id": 7u64,
        "n_transactions": 0u64
    });

    let result = decode_block_env_request(params).expect("optional fields should decode");
    assert_eq!(result.len(), 2);

    let new_iteration = match &result[1] {
        TxQueueContents::NewIteration(iteration, _) => iteration,
        other => panic!("Expected NewIteration event, got {other:?}"),
    };
    let block_env = &new_iteration.block_env;
    assert!(block_env.prevrandao.is_some());
    assert!(block_env.blob_excess_gas_and_price.is_some());
    assert_eq!(new_iteration.iteration_id, 7);
}

#[test]
fn test_block_env_boundary_values() {
    let test_cases = vec![
        (
            "zero_values",
            json!({
                "block_env": {
                    "number": 0u64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 0u64,
                    "gas_limit": 0u64,
                    "basefee": 0u64,
                    "difficulty": "0x0"
                },
                "selected_iteration_id": 1u64
            }),
        ),
        (
            "max_values",
            json!({
                "block_env": {
                    "number": u64::MAX,
                    "beneficiary": "0xffffffffffffffffffffffffffffffffffffffff",
                    "timestamp": u64::MAX,
                    "gas_limit": u64::MAX,
                    "basefee": u64::MAX,
                    "difficulty": "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                },
                "selected_iteration_id": u64::MAX
            }),
        ),
    ];

    for (name, request) in test_cases {
        let result = decode_block_env_request(request);
        assert!(result.is_ok(), "Failed for case: {name}");
    }
}

#[test]
fn test_block_env_invalid_field_types() {
    let test_cases = vec![
        (
            "invalid_number",
            json!({
                "block_env": {
                    "number": "not_a_number",
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 0u64,
                    "gas_limit": 0u64,
                    "basefee": 0u64,
                    "difficulty": "0x0"
                }
            }),
        ),
        (
            "invalid_beneficiary",
            json!({
                "block_env": {
                    "number": 1u64,
                    "beneficiary": "invalid_address",
                    "timestamp": 0u64,
                    "gas_limit": 0u64,
                    "basefee": 0u64,
                    "difficulty": "0x0"
                }
            }),
        ),
        (
            "invalid_timestamp",
            json!({
                "block_env": {
                    "number": 1u64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": "invalid",
                    "gas_limit": 0u64,
                    "basefee": 0u64,
                    "difficulty": "0x0"
                }
            }),
        ),
        (
            "invalid_gas_limit",
            json!({
                "block_env": {
                    "number": 1u64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 0u64,
                    "gas_limit": "invalid",
                    "basefee": 0u64,
                    "difficulty": "0x0"
                }
            }),
        ),
        (
            "invalid_basefee",
            json!({
                "block_env": {
                    "number": 1u64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 0u64,
                    "gas_limit": 0u64,
                    "basefee": "invalid",
                    "difficulty": "0x0"
                }
            }),
        ),
        (
            "invalid_difficulty",
            json!({
                "block_env": {
                    "number": 1u64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 0u64,
                    "gas_limit": 0u64,
                    "basefee": 0u64,
                    "difficulty": "invalid"
                }
            }),
        ),
        (
            "invalid_prevrandao",
            json!({
                "block_env": {
                    "number": 1u64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 0u64,
                    "gas_limit": 0u64,
                    "basefee": 0u64,
                    "difficulty": "0x0",
                    "prevrandao": "invalid_hash"
                }
            }),
        ),
        (
            "invalid_blob_data",
            json!({
                "block_env": {
                    "number": 1u64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 0u64,
                    "gas_limit": 0u64,
                    "basefee": 0u64,
                    "difficulty": "0x0",
                    "blob_excess_gas_and_price": {
                        "excess_blob_gas": "invalid"
                    }
                }
            }),
        ),
        (
            "negative_number",
            json!({
                "block_env": {
                    "number": -1i64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 0u64,
                    "gas_limit": 0u64,
                    "basefee": 0u64,
                    "difficulty": "0x0"
                }
            }),
        ),
    ];

    for (name, params) in test_cases {
        let result = decode_block_env_request(params);
        assert!(result.is_err(), "Should fail for: {name}");
    }
}

#[test]
fn test_block_env_validation_rules() {
    let test_cases = vec![
        (
            "last_tx_hash_without_n_transactions",
            json!({
                "block_env": {
                    "number": 123456u64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 1234567890u64,
                    "gas_limit": 30000000u64,
                    "basefee": 1000000000u64,
                    "difficulty": "0x0",
                },
                "last_tx_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                "selected_iteration_id": 1u64
            }),
        ),
        (
            "n_transactions_without_last_tx_hash",
            json!({
                "block_env": {
                    "number": 123456u64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 1234567890u64,
                    "gas_limit": 30000000u64,
                    "basefee": 1000000000u64,
                    "difficulty": "0x0",
                },
                "last_tx_hash": null,
                "n_transactions": 10u64,
                "selected_iteration_id": 2u64
            }),
        ),
        (
            "zero_n_transactions_with_hash",
            json!({
                "block_env": {
                    "number": 123456u64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 1234567890u64,
                    "gas_limit": 30000000u64,
                    "basefee": 1000000000u64,
                    "difficulty": "0x0",
                },
                "last_tx_hash": "0x5555555555555555555555555555555555555555555555555555555555555555",
                "n_transactions": 0u64,
                "selected_iteration_id": 3u64
            }),
        ),
    ];

    for (name, request) in test_cases {
        let result = decode_block_env_request(request);
        assert!(
            matches!(result, Err(HttpDecoderError::BlockEnvValidation(_))),
            "Should fail validation for: {name}"
        );
    }
}

#[test]
fn test_block_env_serialization_round_trip() {
    let original = NewIteration {
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
        iteration_id: 88,
    };

    let serialized = serde_json::to_value(&original).unwrap();
    let deserialized = serde_json::from_value::<NewIteration>(serialized).unwrap();

    assert_eq!(original.block_env.number, deserialized.block_env.number);
    assert_eq!(
        original.block_env.beneficiary,
        deserialized.block_env.beneficiary
    );
    assert_eq!(
        original.block_env.timestamp,
        deserialized.block_env.timestamp
    );
    assert_eq!(original.iteration_id, deserialized.iteration_id);
}

// ============================================================================
// BlockEnv Tests - Full Decoder Pipeline
// ============================================================================

#[test]
fn test_decoder_block_env_success() {
    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "sendBlockEnv".to_string(),
        params: Some(json!({
            "block_env": {
                "number": 123456u64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 1234567890u64,
                "gas_limit": 30000000u64,
                "basefee": 1000000000u64,
                "difficulty": "0x0",
            },
            "last_tx_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
            "n_transactions": 5u64,
            "selected_iteration_id": 9u64
        })),
        id: Some(json!(1)),
    };

    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
    assert!(result.is_ok());
    let events = result.unwrap();
    assert_eq!(events.len(), 2);
    assert!(matches!(events[0], TxQueueContents::CommitHead(_, _)));
    assert!(matches!(events[1], TxQueueContents::NewIteration(_, _)));
}

#[test]
fn test_decoder_block_env_missing_params() {
    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "sendBlockEnv".to_string(),
        params: None,
        id: Some(json!(1)),
    };

    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
    assert!(matches!(result, Err(HttpDecoderError::MissingParams)));
}

// ============================================================================
// Reorg Tests
// ============================================================================

#[test]
fn test_reorg_valid() {
    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "reorg".to_string(),
        params: Some(json!({
            "block_number": 100u64,
            "iteration_id": 1u64,
            "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        })),
        id: Some(json!(1)),
    };

    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
    assert!(result.is_ok());

    let contents = result.unwrap();
    assert_eq!(contents.len(), 1);

    match &contents[0] {
        TxQueueContents::Reorg(tx_execution_id, _) => {
            assert_eq!(
                tx_execution_id.tx_hash,
                B256::from_str(
                    "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                )
                .unwrap()
            );
            assert_eq!(tx_execution_id.block_number, 100);
            assert_eq!(tx_execution_id.iteration_id, 1);
        }
        _ => panic!("Expected Reorg variant"),
    }
}

#[test]
fn test_reorg_hash_format_variations() {
    let test_cases = vec![
        (
            "without_prefix",
            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        ),
        (
            "with_prefix",
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        ),
        (
            "uppercase",
            "0x1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF",
        ),
        (
            "mixed_case",
            "0x1234567890AbCdEf1234567890aBcDeF1234567890AbCdEf1234567890aBcDeF",
        ),
    ];

    for (name, hash) in test_cases {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "reorg".to_string(),
            params: Some(json!({
                "block_number": 100u64,
                "iteration_id": 1u64,
                "tx_hash": hash
            })),
            id: Some(json!(1)),
        };

        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(result.is_ok(), "Failed for case: {name}");
    }
}

#[test]
fn test_reorg_missing_params() {
    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "reorg".to_string(),
        params: None,
        id: Some(json!(1)),
    };

    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
    assert!(matches!(result, Err(HttpDecoderError::MissingParams)));
}

#[test]
fn test_reorg_missing_fields() {
    let test_cases = vec![
        ("missing_all", json!({"someOtherField": "value"})),
        (
            "missing_hash",
            json!({
                "block_number": 100u64,
                "iteration_id": 1u64
            }),
        ),
        (
            "missing_block_number",
            json!({
                "iteration_id": 1u64,
                "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            }),
        ),
        (
            "missing_iteration_id",
            json!({
                "block_number": 100u64,
                "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            }),
        ),
    ];

    for (name, params) in test_cases {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "reorg".to_string(),
            params: Some(params),
            id: Some(json!(1)),
        };

        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(
            matches!(result, Err(HttpDecoderError::ReorgValidation(_))),
            "Should fail for: {name}",
        );
    }
}

#[test]
fn test_reorg_invalid_hash_formats() {
    let test_cases = vec![
        ("invalid_chars", "not_a_valid_hash"),
        ("wrong_length", "0x1234"),
        ("empty_string", ""),
        ("only_0x", "0x"),
    ];

    for (name, hash) in test_cases {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "reorg".to_string(),
            params: Some(json!({
                "block_number": 100u64,
                "iteration_id": 1u64,
                "tx_hash": hash
            })),
            id: Some(json!(1)),
        };

        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(
            matches!(result, Err(HttpDecoderError::ReorgValidation(_))),
            "Should fail for: {name}",
        );
    }
}

#[test]
fn test_reorg_invalid_hash_types() {
    let test_cases = vec![
        ("number", json!(12345)),
        ("null", json!(null)),
        ("object", json!({"hash": "value"})),
        ("array", json!(["0x1234"])),
    ];

    for (name, hash_value) in test_cases {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "reorg".to_string(),
            params: Some(json!({
                "block_number": 100u64,
                "iteration_id": 1u64,
                "tx_hash": hash_value
            })),
            id: Some(json!(1)),
        };

        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(
            matches!(result, Err(HttpDecoderError::ReorgValidation(_))),
            "Should fail for: {name}",
        );
    }
}

#[test]
fn test_reorg_with_various_block_numbers() {
    let test_cases = vec![
        (0u64, "zero"),
        (1u64, "one"),
        (999999u64, "large"),
        (u64::MAX, "max"),
    ];

    for (block_number, description) in test_cases {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "reorg".to_string(),
            params: Some(json!({
                "block_number": block_number,
                "iteration_id": 1u64,
                "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            })),
            id: Some(json!(1)),
        };

        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(
            result.is_ok(),
            "Should succeed with {description} block number",
        );
    }
}
