#![allow(clippy::match_wildcard_for_single_variants)]
use crate::{
    engine::queue::NewIteration,
    transport::{
        decoder::{
            Decoder,
            HttpDecoderError,
            HttpTransactionDecoder,
            NewIterationEvent,
            SendEvent,
            SendEventsParams,
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
            "tx_hash": hash,
            "index": 0
        },
        "tx_env": tx_env
    })
}

#[allow(clippy::too_many_arguments)]
fn create_test_transaction_json_full(
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
    index: u64,
    prev_tx_hash: Option<&str>,
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

    let mut transaction = json!({
        "tx_execution_id": {
            "block_number": block_number,
            "iteration_id": iteration_id,
            "tx_hash": hash,
            "index": index
        },
        "tx_env": tx_env
    });

    if let Some(prev_hash) = prev_tx_hash {
        transaction["prev_tx_hash"] = json!(prev_hash);
    }

    transaction
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

#[test]
fn test_transaction_with_custom_index() {
    let transaction = create_test_transaction_json_full(
        "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        "0x1234567890123456789012345678901234567890",
        Some("0x9876543210987654321098765432109876543210"),
        "1000000000000000000",
        "0x",
        21000,
        20000000000,
        0,
        1,
        1,
        1,
        5,
        None,
    );

    let request = create_test_request_json("sendTransactions", &[transaction]);
    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 1);
}

#[test]
fn test_transaction_with_prev_tx_hash() {
    let transaction = create_test_transaction_json_full(
        "0x2222222222222222222222222222222222222222222222222222222222222222",
        "0x1234567890123456789012345678901234567890",
        Some("0x9876543210987654321098765432109876543210"),
        "1000000000000000000",
        "0x",
        21000,
        20000000000,
        0,
        1,
        1,
        1,
        0,
        Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
    );

    let request = create_test_request_json("sendTransactions", &[transaction]);
    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 1);
}

#[test]
fn test_transaction_with_both_index_and_prev_tx_hash() {
    let transaction = create_test_transaction_json_full(
        "0x3333333333333333333333333333333333333333333333333333333333333333",
        "0x1234567890123456789012345678901234567890",
        Some("0x9876543210987654321098765432109876543210"),
        "1000000000000000000",
        "0x",
        21000,
        20000000000,
        0,
        1,
        1,
        1,
        5,
        Some("0x2222222222222222222222222222222222222222222222222222222222222222"),
    );

    let request = create_test_request_json("sendTransactions", &[transaction]);
    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 1);
}

#[test]
fn test_multiple_transactions_with_index_sequence() {
    let transactions = vec![
        create_test_transaction_json_full(
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
            0,
            None,
        ),
        create_test_transaction_json_full(
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
            1,
            1,
            Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
        ),
        create_test_transaction_json_full(
            "0x3333333333333333333333333333333333333333333333333333333333333333",
            "0x5555555555555555555555555555555555555555",
            Some("0x6666666666666666666666666666666666666666"),
            "3000000000000000000",
            "0x",
            21000,
            23000000000,
            2,
            1,
            100,
            1,
            2,
            Some("0x2222222222222222222222222222222222222222222222222222222222222222"),
        ),
    ];

    let request = create_test_request_json("sendTransactions", &transactions);
    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 3);
}

#[test]
fn test_index_boundary_values() {
    let test_cases = vec![(0u64, "zero"), (1u64, "one"), (u64::MAX, "max")];

    for (index, name) in test_cases {
        let transaction = create_test_transaction_json_full(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "0x1234567890123456789012345678901234567890",
            Some("0x9876543210987654321098765432109876543210"),
            "1000000000000000000",
            "0x",
            21000,
            20000000000,
            0,
            1,
            1,
            1,
            index,
            None,
        );

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_ok(), "Failed for index value: {name}");
        assert_eq!(result.unwrap().len(), 1);
    }
}

#[test]
fn test_prev_tx_hash_format_variations() {
    let test_cases = vec![
        (
            "with_prefix",
            "0x1111111111111111111111111111111111111111111111111111111111111111",
        ),
        (
            "without_prefix",
            "1111111111111111111111111111111111111111111111111111111111111111",
        ),
        (
            "uppercase",
            "0x1111111111111111111111111111111111111111111111111111111111111111",
        ),
    ];

    for (name, hash) in test_cases {
        let transaction = create_test_transaction_json_full(
            "0x2222222222222222222222222222222222222222222222222222222222222222",
            "0x1234567890123456789012345678901234567890",
            Some("0x9876543210987654321098765432109876543210"),
            "1000000000000000000",
            "0x",
            21000,
            20000000000,
            0,
            1,
            1,
            1,
            0,
            Some(hash),
        );

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_ok(), "Failed for case: {name}");
    }
}

#[test]
fn test_invalid_prev_tx_hash_format() {
    let test_cases = vec![
        ("invalid_chars", "not_a_valid_hash"),
        ("wrong_length", "0x1234"),
        (
            "malformed_hex",
            "0xgg11111111111111111111111111111111111111111111111111111111111111",
        ),
    ];

    for (name, invalid_hash) in test_cases {
        let transaction = create_test_transaction_json_full(
            "0x2222222222222222222222222222222222222222222222222222222222222222",
            "0x1234567890123456789012345678901234567890",
            Some("0x9876543210987654321098765432109876543210"),
            "1000000000000000000",
            "0x",
            21000,
            20000000000,
            0,
            1,
            1,
            1,
            0,
            Some(invalid_hash),
        );

        let request = create_test_request_json("sendTransactions", &[transaction]);
        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

        assert!(result.is_err(), "Should fail for case: {name}");
    }
}

#[test]
fn test_invalid_index_type() {
    let transaction = json!({
        "tx_execution_id": {
            "block_number": 1,
            "iteration_id": 1,
            "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "index": "not_a_number"
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

    assert!(result.is_err());
}

#[test]
fn test_negative_index() {
    let transaction = json!({
        "tx_execution_id": {
            "block_number": 1,
            "iteration_id": 1,
            "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "index": -1
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

    assert!(result.is_err());
}

#[test]
fn test_prev_tx_hash_empty_string() {
    let transaction = create_test_transaction_json_full(
        "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        "0x1234567890123456789012345678901234567890",
        Some("0x9876543210987654321098765432109876543210"),
        "1000000000000000000",
        "0x",
        21000,
        20000000000,
        0,
        1,
        1,
        1,
        0,
        Some(""),
    );

    let request = create_test_request_json("sendTransactions", &[transaction]);
    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);

    assert!(result.is_err());
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
                    "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                    "index": 0
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
                    "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                    "index": 0
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
                    "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                    "index": 0
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
                    "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                    "index": 0
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
                    "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                    "index": 0
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
                    "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                    "index": 0
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
                    "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                    "index": 0
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
                    "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                    "index": 0
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
                    "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                    "index": 0
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
                    "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                    "index": 0
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
                    "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                    "index": 0
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
                    "iteration_id": 1,
                    "index": 0
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
            "tx_hash": "not_64_chars",
            "index": 0
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
                "tx_hash": "invalid_hash",
                "index": 0
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

#[test]
fn test_block_env_serialization_round_trip() {
    let original = NewIteration {
        block_env: BlockEnv {
            number: U256::from(123456u64),
            beneficiary: Address::ZERO,
            timestamp: U256::from(1234567890u64),
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
            "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "index": 0,
            "depth": 1,
            "tx_hashes": [
                "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            ]
        })),
        id: Some(json!(1)),
    };

    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
    assert!(result.is_ok());

    let contents = result.unwrap();
    assert_eq!(contents.len(), 1);

    match &contents[0] {
        TxQueueContents::Reorg(reorg) => {
            assert_eq!(
                reorg.tx_execution_id.tx_hash,
                B256::from_str(
                    "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                )
                .unwrap()
            );
            assert_eq!(reorg.tx_execution_id.block_number, 100);
            assert_eq!(reorg.tx_execution_id.iteration_id, 1);
            assert_eq!(reorg.depth(), 1);
            assert_eq!(reorg.tx_hashes.len(), 1);
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
                "tx_hash": hash,
                "index": 0,
                "depth": 1,
                "tx_hashes": [hash]
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
                "iteration_id": 1u64,
                "index": 0,
                "depth": 1,
                "tx_hashes": [
                    "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                ]
            }),
        ),
        (
            "missing_block_number",
            json!({
                "iteration_id": 1u64,
                "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                "index": 0,
                "depth": 1,
                "tx_hashes": [
                    "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                ]
            }),
        ),
        (
            "missing_iteration_id",
            json!({
                "block_number": 100u64,
                "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                "index": 0,
                "depth": 1,
                "tx_hashes": [
                    "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                ]
            }),
        ),
        (
            "missing_tx_hashes",
            json!({
                "block_number": 100u64,
                "iteration_id": 1u64,
                "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                "index": 0,
                "depth": 1
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
                "tx_hash": hash,
                "index": 0,
                "depth": 1,
                "tx_hashes": [hash]
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
                "tx_hash": hash_value,
                "index": 0,
                "depth": 1,
                "tx_hashes": [
                    "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                ]
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
                "tx_hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                "index": 0,
                "depth": 1,
                "tx_hashes": [
                    "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                ]
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

// ============================================================================
// SendEvents Tests - Valid Cases
// ============================================================================

#[test]
fn test_send_events_single_commit_head() {
    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "sendEvents".to_string(),
        params: Some(json!({
            "events": [
                {
                    "commit_head": {
                        "last_tx_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                        "n_transactions": 10,
                        "block_number": 100,
                        "timestamp": 0,
                        "selected_iteration_id": 5,
                        "block_hash": "0x0000000000000000000000000000000000000000000000000000000000000000"
                    }
                }
            ]
        })),
        id: Some(json!(1)),
    };

    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
    assert!(result.is_ok());
    let events = result.unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], TxQueueContents::CommitHead(_)));
}

#[test]
fn test_send_events_single_new_iteration() {
    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "sendEvents".to_string(),
        params: Some(json!({
            "events": [
                {
                    "new_iteration": {
                        "iteration_id": 42,
                        "block_env": {
                            "number": 123456u64,
                            "beneficiary": "0x0000000000000000000000000000000000000000",
                            "timestamp": 1234567890u64,
                            "gas_limit": 30000000u64,
                            "basefee": 1000000000u64,
                            "difficulty": "0x0"
                        }
                    }
                }
            ]
        })),
        id: Some(json!(1)),
    };

    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
    assert!(result.is_ok());
    let events = result.unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], TxQueueContents::NewIteration(_)));
}

#[test]
fn test_send_events_commit_head_with_zero_transactions() {
    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "sendEvents".to_string(),
        params: Some(json!({
            "events": [
                {
                    "commit_head": {
                        "n_transactions": 0,
                        "block_number": 100,
                        "timestamp": 100,
                        "selected_iteration_id": 1,
                        "block_hash": "0x0000000000000000000000000000000000000000000000000000000000000000"
                    }
                }
            ]
        })),
        id: Some(json!(1)),
    };

    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
    assert!(result.is_ok());
}

#[test]
fn test_send_events_commit_head_last_tx_hash_variations() {
    let test_cases = vec![
        ("null_hash", json!(null)),
        ("empty_string", json!("")),
        ("omitted", serde_json::Value::Null), // Will be omitted in construction
    ];

    for (name, hash_value) in test_cases {
        let mut commit_head = json!({
            "n_transactions": 0,
            "block_number": 100,
            "timestamp": 100,
            "selected_iteration_id": 1,
            "block_hash": "0x0000000000000000000000000000000000000000000000000000000000000000"
        });

        if !hash_value.is_null() || name == "null_hash" || name == "empty_string" {
            commit_head["last_tx_hash"] = hash_value;
        }

        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "sendEvents".to_string(),
            params: Some(json!({
                "events": [{"commit_head": commit_head}]
            })),
            id: Some(json!(1)),
        };

        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(result.is_ok(), "Failed for case: {name}");
    }
}

#[test]
fn test_send_events_new_iteration_with_optional_block_fields() {
    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "sendEvents".to_string(),
        params: Some(json!({
            "events": [
                {
                    "new_iteration": {
                        "iteration_id": 42,
                        "block_env": {
                            "number": 123456u64,
                            "beneficiary": "0x1234567890123456789012345678901234567890",
                            "timestamp": 1234567890u64,
                            "gas_limit": 30000000u64,
                            "basefee": 1000000000u64,
                            "difficulty": "0x0",
                            "prevrandao": "0x1234567890123456789012345678901234567890123456789012345678901234",
                            "blob_excess_gas_and_price": {
                                "excess_blob_gas": 1000,
                                "blob_gasprice": 2000
                            }
                        }
                    }
                }
            ]
        })),
        id: Some(json!(1)),
    };

    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
    assert!(result.is_ok());
}

#[test]
fn test_send_events_new_iteration_iteration_id_boundaries() {
    let test_cases = vec![(0u64, "zero"), (u64::MAX, "max")];

    for (iteration_id, name) in test_cases {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "sendEvents".to_string(),
            params: Some(json!({
                "events": [{
                    "new_iteration": {
                        "iteration_id": iteration_id,
                        "block_env": {
                            "number": 1u64,
                            "beneficiary": "0x0000000000000000000000000000000000000000",
                            "timestamp": 1u64,
                            "gas_limit": 30000000u64,
                            "basefee": 1u64,
                            "difficulty": "0x0"
                        }
                    }
                }]
            })),
            id: Some(json!(1)),
        };

        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(result.is_ok(), "Failed for case: {name}");
        let events = result.unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            TxQueueContents::NewIteration(new_iteration) => {
                assert_eq!(new_iteration.iteration_id, iteration_id);
            }
            _ => panic!("Expected NewIteration event for case: {name}"),
        }
    }
}

// ============================================================================
// SendEvents Tests - Error Cases
// ============================================================================

#[test]
fn test_send_events_missing_params() {
    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "sendEvents".to_string(),
        params: None,
        id: Some(json!(1)),
    };

    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
    assert!(matches!(result, Err(HttpDecoderError::MissingParams)));
}

#[test]
fn test_send_events_empty_events_array() {
    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "sendEvents".to_string(),
        params: Some(json!({
            "events": []
        })),
        id: Some(json!(1)),
    };

    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
    assert!(matches!(result, Err(HttpDecoderError::NoEvents)));
}

#[test]
fn test_send_events_missing_events_field() {
    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "sendEvents".to_string(),
        params: Some(json!({
            "invalid_field": "value"
        })),
        id: Some(json!(1)),
    };

    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
    assert!(matches!(result, Err(HttpDecoderError::MissingEventsField)));
}

#[test]
fn test_send_events_commit_head_missing_required_fields() {
    let test_cases = vec![
        (
            "missing_n_transactions",
            json!({
                "last_tx_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                "block_number": 100,
                "selected_iteration_id": 1
            }),
        ),
        (
            "missing_block_number",
            json!({
                "last_tx_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                "n_transactions": 10,
                "selected_iteration_id": 1
            }),
        ),
        (
            "missing_selected_iteration_id",
            json!({
                "last_tx_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                "n_transactions": 10,
                "block_number": 100
            }),
        ),
    ];

    for (name, commit_head) in test_cases {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "sendEvents".to_string(),
            params: Some(json!({
                "events": [{"commit_head": commit_head}]
            })),
            id: Some(json!(1)),
        };

        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(result.is_err(), "Should fail for: {name}");
    }
}

#[test]
fn test_send_events_commit_head_validation_errors() {
    let test_cases = vec![
        (
            "zero_transactions_with_hash",
            json!({
                "last_tx_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                "n_transactions": 0,
                "block_number": 100,
                "timestamp": 100,
                "selected_iteration_id": 1,
                "block_hash": "0x0000000000000000000000000000000000000000000000000000000000000000"
            }),
        ),
        (
            "nonzero_transactions_without_hash",
            json!({
                "n_transactions": 10,
                "block_number": 100,
                "timestamp": 100,
                "selected_iteration_id": 1,
                "block_hash": "0x0000000000000000000000000000000000000000000000000000000000000000"
            }),
        ),
        (
            "nonzero_transactions_with_null_hash",
            json!({
                "last_tx_hash": null,
                "n_transactions": 10,
                "block_number": 100,
                "timestamp": 100,
                "selected_iteration_id": 1,
                "block_hash": "0x0000000000000000000000000000000000000000000000000000000000000000"
            }),
        ),
        (
            "nonzero_transactions_with_empty_hash",
            json!({
                "last_tx_hash": "",
                "n_transactions": 10,
                "block_number": 100,
                "timestamp": 100,
                "selected_iteration_id": 1,
                "block_hash": "0x0000000000000000000000000000000000000000000000000000000000000000"
            }),
        ),
    ];

    for (name, commit_head) in test_cases {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "sendEvents".to_string(),
            params: Some(json!({
                "events": [{"commit_head": commit_head}]
            })),
            id: Some(json!(1)),
        };

        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(
            matches!(result, Err(HttpDecoderError::InvalidTransaction(_))),
            "Should fail validation for: {name}"
        );
    }
}

#[test]
fn test_send_events_commit_head_invalid_hash() {
    let test_cases = vec![
        ("invalid_hash", "not_a_hash"),
        ("wrong_length", "0x1234"),
        ("malformed", "0xgg"),
    ];

    for (name, hash) in test_cases {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "sendEvents".to_string(),
            params: Some(json!({
                "events": [{
                    "commit_head": {
                        "last_tx_hash": hash,
                        "n_transactions": 10,
                        "block_number": 100,
                        "timestamp": 100,
                        "selected_iteration_id": 1,
                        "block_hash": "0x0000000000000000000000000000000000000000000000000000000000000000"
                    }
                }]
            })),
            id: Some(json!(1)),
        };

        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(
            matches!(result, Err(HttpDecoderError::InvalidLastTxHash(_))),
            "Should fail for: {name}"
        );
    }
}

#[test]
fn test_send_events_new_iteration_missing_fields() {
    let test_cases = vec![
        (
            "missing_iteration_id",
            json!({
                "block_env": {
                    "number": 123456u64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 1234567890u64,
                    "gas_limit": 30000000u64,
                    "basefee": 1000000000u64,
                    "difficulty": "0x0"
                }
            }),
        ),
        (
            "missing_block_env",
            json!({
                "iteration_id": 42
            }),
        ),
    ];

    for (name, new_iteration) in test_cases {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "sendEvents".to_string(),
            params: Some(json!({
                "events": [{"new_iteration": new_iteration}]
            })),
            id: Some(json!(1)),
        };

        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(result.is_err(), "Should fail for: {name}");
    }
}

#[test]
fn test_send_events_new_iteration_invalid_block_env() {
    let test_cases = vec![
        (
            "invalid_number",
            json!({
                "iteration_id": 42,
                "block_env": {
                    "number": "invalid",
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 1234567890u64,
                    "gas_limit": 30000000u64,
                    "basefee": 1000000000u64,
                    "difficulty": "0x0"
                }
            }),
        ),
        (
            "invalid_beneficiary",
            json!({
                "iteration_id": 42,
                "block_env": {
                    "number": 123456u64,
                    "beneficiary": "invalid_address",
                    "timestamp": 1234567890u64,
                    "gas_limit": 30000000u64,
                    "basefee": 1000000000u64,
                    "difficulty": "0x0"
                }
            }),
        ),
        (
            "missing_timestamp",
            json!({
                "iteration_id": 42,
                "block_env": {
                    "number": 123456u64,
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "gas_limit": 30000000u64,
                    "basefee": 1000000000u64,
                    "difficulty": "0x0"
                }
            }),
        ),
    ];

    for (name, new_iteration) in test_cases {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "sendEvents".to_string(),
            params: Some(json!({
                "events": [{"new_iteration": new_iteration}]
            })),
            id: Some(json!(1)),
        };

        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(result.is_err(), "Should fail for: {name}");
    }
}

#[test]
fn test_send_events_new_iteration_invalid_optional_fields() {
    let test_cases = vec![
        (
            "invalid_prevrandao",
            json!({
                "number": 123456u64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 1234567890u64,
                "gas_limit": 30000000u64,
                "basefee": 1000000000u64,
                "difficulty": "0x0",
                "prevrandao": "not_hex"
            }),
        ),
        (
            "invalid_blob_excess_gas",
            json!({
                "number": 123456u64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 1234567890u64,
                "gas_limit": 30000000u64,
                "basefee": 1000000000u64,
                "difficulty": "0x0",
                "blob_excess_gas_and_price": {
                    "excess_blob_gas": "invalid",
                    "blob_gasprice": 2000
                }
            }),
        ),
        (
            "missing_blob_gasprice",
            json!({
                "number": 123456u64,
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 1234567890u64,
                "gas_limit": 30000000u64,
                "basefee": 1000000000u64,
                "difficulty": "0x0",
                "blob_excess_gas_and_price": {
                    "excess_blob_gas": 1000
                }
            }),
        ),
    ];

    for (name, block_env) in test_cases {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "sendEvents".to_string(),
            params: Some(json!({
                "events": [{
                    "new_iteration": {
                        "iteration_id": 42,
                        "block_env": block_env
                    }
                }]
            })),
            id: Some(json!(1)),
        };

        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(
            result.is_err(),
            "Optional field validation should fail for: {name}"
        );
    }
}

#[test]
fn test_send_events_invalid_transaction() {
    let invalid_tx = json!({
        "tx_execution_id": {
            "block_number": 1,
            "iteration_id": 1,
            "tx_hash": "invalid_hash",
            "index": 0
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

    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "sendEvents".to_string(),
        params: Some(json!({
            "events": [{"transaction": invalid_tx}]
        })),
        id: Some(json!(1)),
    };

    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
    assert!(result.is_err());
}

#[test]
fn test_send_events_mixed_valid_and_invalid() {
    let valid_commit_head = json!({
        "commit_head": {
            "last_tx_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
            "n_transactions": 5,
            "block_number": 100,
            "selected_iteration_id": 1
        }
    });

    let invalid_commit_head = json!({
        "commit_head": {
            "last_tx_hash": "invalid_hash",
            "n_transactions": 5,
            "block_number": 100,
            "selected_iteration_id": 1
        }
    });

    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "sendEvents".to_string(),
        params: Some(json!({
            "events": [valid_commit_head, invalid_commit_head]
        })),
        id: Some(json!(1)),
    };

    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
    assert!(result.is_err());
}

#[test]
fn test_send_events_unrecognized_event_structure() {
    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "sendEvents".to_string(),
        params: Some(json!({
            "events": [
                {
                    "unknown_event": {
                        "some_field": "some_value"
                    }
                }
            ]
        })),
        id: Some(json!(1)),
    };

    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
    assert!(result.is_err());
}

#[test]
fn test_send_events_mixed_commit_head_and_new_iteration_success() {
    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "sendEvents".to_string(),
        params: Some(json!({
            "events": [
                {
                    "commit_head": {
                        "n_transactions": 0,
                        "block_number": 100,
                        "timestamp": 0,
                        "selected_iteration_id": 1,
                        "block_hash": "0x0000000000000000000000000000000000000000000000000000000000000000"
                    }
                },
                {
                    "new_iteration": {
                        "iteration_id": 7,
                        "block_env": {
                            "number": 200u64,
                            "beneficiary": "0x0000000000000000000000000000000000000000",
                            "timestamp": 1234567890u64,
                            "gas_limit": 30000000u64,
                            "basefee": 1000000000u64,
                            "difficulty": "0x0"
                        }
                    }
                }
            ]
        })),
        id: Some(json!(1)),
    };

    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
    assert!(result.is_ok());
    let events = result.unwrap();
    assert_eq!(events.len(), 2);
    assert!(
        matches!(events[0], TxQueueContents::CommitHead(_)),
        "First event should be CommitHead"
    );
    assert!(
        matches!(events[1], TxQueueContents::NewIteration(_)),
        "Second event should be NewIteration"
    );
}

#[test]
fn test_send_events_commit_head_invalid_field_types() {
    let test_cases = vec![(
        "negative_n_transactions",
        json!({
            "last_tx_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
            "n_transactions": -1,
            "block_number": 100,
            "selected_iteration_id": 1
        }),
    )];

    for (name, commit_head) in test_cases {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "sendEvents".to_string(),
            params: Some(json!({
                "events": [{"commit_head": commit_head}]
            })),
            id: Some(json!(1)),
        };

        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(result.is_err(), "Should fail for: {name}");
    }
}

#[test]
fn test_send_events_boundary_values() {
    let test_cases = vec![
        (
            "max_u64_values",
            json!({
                "commit_head": {
                    "last_tx_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                    "n_transactions": u64::MAX,
                    "block_number": u64::MAX,
                    "timestamp": u64::MAX,
                    "selected_iteration_id": u64::MAX,
                    "block_hash": "0x1111111111111111111111111111111111111111111111111111111111111111"
                }
            }),
        ),
        (
            "zero_block_number",
            json!({
                "commit_head": {
                    "n_transactions": 0,
                    "block_number": 0,
                    "timestamp": 0,
                    "selected_iteration_id": 1,
                    "block_hash": "0x0000000000000000000000000000000000000000000000000000000000000000"
                }
            }),
        ),
    ];

    for (name, event) in test_cases {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "sendEvents".to_string(),
            params: Some(json!({
                "events": [event]
            })),
            id: Some(json!(1)),
        };

        let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
        assert!(result.is_ok(), "Should succeed for: {name}");
    }
}

#[test]
fn test_send_events_large_event_array() {
    let mut events = Vec::new();

    for i in 0..100 {
        events.push(json!({
            "commit_head": {
                "n_transactions": 0,
                "block_number": i,
                "timestamp": i,
                "selected_iteration_id": 1,
                "block_hash": "0x0000000000000000000000000000000000000000000000000000000000000000"
            }
        }));
    }

    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "sendEvents".to_string(),
        params: Some(json!({
            "events": events
        })),
        id: Some(json!(1)),
    };

    let result = HttpTransactionDecoder::to_tx_queue_contents(&request);
    assert!(result.is_ok());
    let decoded_events = result.unwrap();
    assert_eq!(decoded_events.len(), 100);
}
