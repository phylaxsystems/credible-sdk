use super::test_ctx::TestCtx;
use alloy_primitives::B256;
use alloy_provider::{
    Provider,
    ext::{
        AnvilApi,
        DebugApi,
    },
};
use alloy_rpc_types::TransactionRequest;
use assertion_executor::{
    primitives::Address,
    store::AssertionState,
};
use std::collections::HashMap;

// Executes transactions, mines a given number of blocks, and syncs the indexer.
// Returns a map of assertions by address.
pub async fn test_harness(
    test_ctx: &mut TestCtx,
    txs: Vec<TransactionRequest>,
    blocks_to_mine: Option<u64>,
) -> HashMap<Address, Vec<AssertionState>> {
    let tx_hashes = execute_txs(txs, test_ctx).await;
    test_ctx
        .provider
        .anvil_mine(blocks_to_mine, None)
        .await
        .unwrap();
    test_ctx.indexer.sync_to_head().await.unwrap();
    let mut assertions_by_address = HashMap::new();

    for protocol in test_ctx.protocols.iter() {
        let assertions = test_ctx.store.get_assertions_for_contract(*protocol);
        assertions_by_address.insert(*protocol, assertions);
    }

    // If we mined blocks, validate the tx hashes
    if blocks_to_mine.is_some() {
        validate_tx_hashes(tx_hashes, test_ctx).await;
    }

    assertions_by_address
}

pub async fn execute_txs(
    transactions_for_block: Vec<TransactionRequest>,
    test_ctx: &TestCtx,
) -> Vec<B256> {
    let provider = test_ctx.provider.clone();
    let mut tx_hashes = Vec::new();
    for transaction in transactions_for_block {
        let result = provider.send_transaction(transaction).await.unwrap();
        let tx_hash = result.tx_hash();
        tx_hashes.push(*tx_hash);
    }
    tx_hashes
}

pub async fn validate_tx_hashes(tx_hashes: Vec<B256>, test_ctx: &TestCtx) {
    for tx_hash in tx_hashes {
        let tx_receipt = test_ctx
            .provider
            .get_transaction_receipt(tx_hash)
            .await
            .expect("Transaction receipt request failed")
            .expect("Transaction receipt not found");

        if !tx_receipt.status() {
            let trace = test_ctx
                .provider
                .debug_trace_transaction(tx_hash, Default::default())
                .await
                .unwrap();
            panic!("Transaction failed: trace: {trace:#?}");
        }
    }
}
