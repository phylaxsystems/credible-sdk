use crate::integration_tests::setup::require_test_instance;
use alloy::primitives::keccak256;
use assertion_executor::{
    db::DatabaseCommit,
    test_utils::{
        COUNTER_ADDRESS,
        CounterValidationSetup,
        SIMPLE_ASSERTION_COUNTER,
        bytecode,
        setup_counter_validation,
    },
};
use serde_json::{
    Value,
    json,
};

#[tokio::test]
async fn health_endpoint_returns_ok() {
    let Some(instance) = require_test_instance("health_endpoint_returns_ok").await else {
        return;
    };
    let response = instance.get("/health").await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn replay_start_block_endpoint_returns_preview() {
    let Some(instance) = require_test_instance("replay_start_block_endpoint_returns_preview").await
    else {
        return;
    };

    let response = instance.get("/replay/start-block").await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let body: Value = response
        .json()
        .await
        .expect("start-block response body should parse as json");
    let start_block = body["start_block"]
        .as_u64()
        .expect("start_block should be u64");
    let head_block = body["head_block"]
        .as_u64()
        .expect("head_block should be u64");
    let replay_window = body["replay_window"]
        .as_u64()
        .expect("replay_window should be u64");

    assert!(head_block >= start_block);
    assert_eq!(replay_window, 1);
}

#[tokio::test]
async fn replay_start_block_tracks_head_progression() {
    let Some(instance) = require_test_instance("replay_start_block_tracks_head_progression").await
    else {
        return;
    };

    for _ in 0..3 {
        instance.mock_node.send_new_head();
    }

    let response = instance.get("/replay/start-block").await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: Value = response
        .json()
        .await
        .expect("start-block response should parse as json");
    assert_eq!(
        body,
        json!({
            "start_block": 2,
            "head_block": 3,
            "replay_window": 1
        })
    );
}

#[tokio::test]
async fn replay_start_block_reflects_adaptive_window_after_replay() {
    let Some(instance) =
        require_test_instance("replay_start_block_reflects_adaptive_window_after_replay").await
    else {
        return;
    };

    for _ in 0..3 {
        instance.mock_node.send_new_head();
    }

    let replay_response = instance.post_json("/replay", json!({})).await;
    assert_eq!(replay_response.status(), reqwest::StatusCode::OK);

    let response = instance.get("/replay/start-block").await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: Value = response
        .json()
        .await
        .expect("start-block response should parse as json");
    assert_eq!(body["head_block"], json!(3));
    assert_eq!(body["start_block"], json!(0));
    assert!(
        body["replay_window"]
            .as_u64()
            .expect("replay_window should be u64")
            > 1
    );
}

#[tokio::test]
async fn replay_rejects_malformed_payload() {
    let Some(instance) = require_test_instance("replay_rejects_malformed_payload").await else {
        return;
    };

    let response = instance.post_raw_json("/replay", "{").await;
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn replay_accepts_empty_payload() {
    let Some(instance) = require_test_instance("replay_accepts_empty_payload").await else {
        return;
    };

    let response = instance.post_json("/replay", json!({})).await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn replay_rejects_legacy_assertion_id_string_payload() {
    let Some(instance) =
        require_test_instance("replay_rejects_legacy_assertion_id_string_payload").await
    else {
        return;
    };

    let response = instance
        .post_json(
            "/replay",
            json!({
                "assertion_ids": ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
            }),
        )
        .await;
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn replay_fails_on_invalid_override_bytecode() {
    let Some(instance) = require_test_instance("replay_fails_on_invalid_override_bytecode").await
    else {
        return;
    };

    let response = instance
        .post_json(
            "/replay",
            json!({
                "assertions": [
                    {
                        "adopter": "0x1111111111111111111111111111111111111111",
                        "deployment_bytecode": "0xdeadbeef",
                        "id": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    }
                ]
            }),
        )
        .await;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[tokio::test]
async fn replay_accepts_valid_override_bytecode() {
    let Some(instance) = require_test_instance("replay_accepts_valid_override_bytecode").await
    else {
        return;
    };

    let deployment_bytecode = format!("0x{}", hex::encode(bytecode(SIMPLE_ASSERTION_COUNTER)));
    let assertion_id = format!(
        "0x{}",
        hex::encode(keccak256(bytecode(SIMPLE_ASSERTION_COUNTER)))
    );
    let response = instance
        .post_json(
            "/replay",
            json!({
                "assertions": [
                    {
                        "adopter": "0x1111111111111111111111111111111111111111",
                        "deployment_bytecode": deployment_bytecode,
                        "id": assertion_id
                    }
                ]
            }),
        )
        .await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn assertions_are_triggered_and_validate_transaction_flow() {
    let CounterValidationSetup {
        mut fork_db,
        mut mock_db,
        mut executor,
        block_env,
        tx,
        ..
    } = setup_counter_validation();

    let mut first_tx = tx.clone();
    first_tx.nonce = 0;
    let first_result = executor
        .validate_transaction_ext_db::<_, _>(
            block_env.clone(),
            &first_tx,
            &mut fork_db,
            &mut mock_db,
        )
        .expect("first validation should succeed");
    assert!(first_result.transaction_valid);
    assert!(
        first_result.total_assertions_gas() > 0,
        "assertions should have been executed"
    );
    assert!(
        first_result
            .assertions_executions
            .iter()
            .any(|execution| execution.adopter == COUNTER_ADDRESS),
        "counter assertion adopter should be present in assertion execution results"
    );

    mock_db.commit(first_result.result_and_state.state.clone());

    let mut second_tx = tx.clone();
    second_tx.nonce = 1;
    let second_result = executor
        .validate_transaction_ext_db::<_, _>(block_env, &second_tx, &mut fork_db, &mut mock_db)
        .expect("second validation should succeed");
    assert!(
        !second_result.transaction_valid,
        "assertion should reject the second tx after state update"
    );
}
