mod common;

#[cfg(test)]
mod tests {

    use super::common::{harness::test_harness, test_ctx::setup_int_test_indexer};
    use alloy_rpc_types_anvil::ReorgOptions;
    use assertion_executor::primitives::{Address, U256};
    use assertion_executor::store::BlockTag;
    use rand::Rng;

    use alloy_provider::{Provider, ext::AnvilApi};

    #[tokio::test]
    async fn test_indexer_with_latest_tag() {
        let time_lock_blocks = 1;
        let mut test_ctx = setup_int_test_indexer(BlockTag::Latest, time_lock_blocks).await;
        let contract_address = Address::random();
        test_ctx
            .deploy_and_register_protocol(contract_address)
            .await;

        let add_tx = test_ctx.add_assertion_tx(contract_address).await;
        let remove_tx = test_ctx.remove_assertion_tx(contract_address, 0);
        let txs = vec![add_tx, remove_tx];

        // Submit, Mine 1 block, and index
        let results = test_harness(&mut test_ctx, txs, Some(1)).await;

        assert_eq!(results.len(), 1);
        for (contract_address_act, assertions) in results {
            assert_eq!(contract_address_act, contract_address);
            assert_eq!(assertions.len(), 1);
            let assertion = assertions.first().unwrap();
            assert_eq!(
                assertion.active_at_block,
                assertion.inactive_at_block.unwrap()
            );
        }
    }

    #[tokio::test]
    async fn test_indexer_with_ctor_args() {
        let time_lock_blocks = 1;
        let mut test_ctx = setup_int_test_indexer(BlockTag::Finalized, time_lock_blocks).await;
        let contract_address = Address::random();
        test_ctx
            .deploy_and_register_protocol(contract_address)
            .await;

        let txs = vec![test_ctx.add_assertion_tx_with_args(contract_address).await];

        // Mine 64 additional blocks for finalization
        let results = test_harness(&mut test_ctx, txs, Some(65)).await;
        assert_eq!(results.len(), 1);
        for (contract_address_act, assertions) in results {
            assert_eq!(contract_address_act, contract_address);
            assert_eq!(assertions.len(), 1);
            let assertion = assertions.first().unwrap();

            // Assert storage from constructor arg is set
            let storage_slot = assertion.assertion_contract.storage.get(&U256::from(0));
            assert!(storage_slot.is_some());
            let storage_slot_value = storage_slot.unwrap();
            assert_eq!(storage_slot_value.present_value, U256::from(3));
            assert!(assertion.inactive_at_block.is_none());
        }
    }

    #[tokio::test]
    async fn test_indexer_large_batch_single_block() {
        let time_lock_blocks = 1;
        let mut test_ctx = setup_int_test_indexer(BlockTag::Finalized, time_lock_blocks).await;
        let mut contract_addresses = Vec::new();
        let mut txs = Vec::new();
        let count = 200;
        for _ in 0..count {
            let contract_address = Address::random();
            contract_addresses.push(contract_address);
            test_ctx
                .deploy_and_register_protocol(contract_address)
                .await;

            txs.push(test_ctx.add_assertion_tx(contract_address).await);
        }

        // Mine  additional blocks
        let results = test_harness(&mut test_ctx, txs, Some(65)).await;
        assert_eq!(results.len(), count as usize);
        for (contract_address_act, assertions) in results {
            assert!(contract_addresses.contains(&contract_address_act));
            assert_eq!(assertions.len(), 1);
        }
    }

    #[tokio::test]
    async fn test_indexer_downtime() {
        let time_lock_blocks = 1;
        let mut test_ctx = setup_int_test_indexer(BlockTag::Finalized, time_lock_blocks).await;
        let contract_address = Address::random();
        test_ctx
            .deploy_and_register_protocol(contract_address)
            .await;

        let tx = test_ctx.add_assertion_tx(contract_address).await;
        test_harness(&mut test_ctx, vec![tx], Some(1)).await;
        // Simulate downtime by mining many blocks
        test_ctx
            .provider
            .anvil_mine(Some(10_000), None)
            .await
            .unwrap();
        // Submit removal tx
        let remove_tx = test_ctx.remove_assertion_tx(contract_address, 0);
        // Mine 64 additional blocks
        test_harness(&mut test_ctx, vec![remove_tx], Some(65)).await;
        let results = test_ctx.store.get_assertions_for_contract(contract_address);
        assert_eq!(results.len(), 1);
        let assertion = results.first().unwrap();
        assert!(assertion.inactive_at_block.is_some());
    }

    #[tokio::test]
    #[ignore]
    // NOTE: Op talos bug not reproducible with anvil
    async fn test_indexer_over_100k_blocks() {
        let time_lock_blocks = 1;
        let mut test_ctx = setup_int_test_indexer(BlockTag::Finalized, time_lock_blocks).await;
        let contract_address = Address::random();
        test_ctx
            .deploy_and_register_protocol(contract_address)
            .await;

        // Mine 105,000 blocks
        test_ctx
            .provider
            .anvil_mine(Some(105_000), None)
            .await
            .unwrap();

        let tx = test_ctx.add_assertion_tx(contract_address).await;
        test_harness(&mut test_ctx, vec![tx], Some(65)).await;
        let results = test_ctx.store.get_assertions_for_contract(contract_address);
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    #[should_panic(expected = "return_value: 0x9969960a")] // cast sig 'AssertionAlreadyExists'
    // NOTE: Not possible to add an assertion after removal due to smart contract logic
    async fn test_add_remove_add_across_multiple_blocks() {
        let time_lock_blocks = 1;
        let mut test_ctx = setup_int_test_indexer(BlockTag::Latest, time_lock_blocks).await;
        let contract_address = Address::random();
        test_ctx
            .deploy_and_register_protocol(contract_address)
            .await;

        let add_tx1 = test_ctx.add_assertion_tx(contract_address).await;
        let remove_tx = test_ctx.remove_assertion_tx(contract_address, 0);
        let add_tx2 = test_ctx.add_assertion_tx(contract_address).await;

        // Add assertion
        test_harness(&mut test_ctx, vec![add_tx1], Some(1)).await;
        let assertions = test_ctx.store.get_assertions_for_contract(contract_address);
        assert_eq!(assertions.len(), 1);
        assert!(assertions[0].inactive_at_block.is_none());

        // Remove assertion
        test_harness(&mut test_ctx, vec![remove_tx], Some(1)).await;
        let assertions = test_ctx.store.get_assertions_for_contract(contract_address);
        assert_eq!(assertions.len(), 1);
        assert!(assertions[0].inactive_at_block.is_some());
        let assertion = assertions.first().unwrap();
        assert!(assertion.active_at_block < assertion.inactive_at_block.unwrap());

        // Add assertion again
        test_harness(&mut test_ctx, vec![add_tx2], Some(1)).await;
        let assertions = test_ctx.store.get_assertions_for_contract(contract_address);
        assert_eq!(assertions.len(), 1);
        let assertion = assertions.first().unwrap();
        assert!(assertion.active_at_block < assertion.inactive_at_block.unwrap());
    }

    #[tokio::test]
    #[should_panic(expected = "Transaction receipt not found")] // cast sig 'AssertionAlreadyExists'
    // NOTE: Not possible to add an assertion after removal due to smart contract logic
    async fn test_add_remove_add_in_single_block() {
        let time_lock_blocks = 1;
        let mut test_ctx = setup_int_test_indexer(BlockTag::Latest, time_lock_blocks).await;
        let contract_address = Address::random();
        test_ctx
            .deploy_and_register_protocol(contract_address)
            .await;

        // Add, remove, add all in the same block
        let add_tx1 = test_ctx.add_assertion_tx(contract_address).await;
        let remove_tx = test_ctx.remove_assertion_tx(contract_address, 0);
        let add_tx2 = test_ctx.add_assertion_tx(contract_address).await;

        // All txs in the same block
        test_harness(&mut test_ctx, vec![add_tx1, remove_tx, add_tx2], Some(1)).await;
        let assertions = test_ctx.store.get_assertions_for_contract(contract_address);
        // Should have one active assertion (the last add)
        assert_eq!(assertions.len(), 1);
        let assertion = assertions.first().unwrap();
        assert!(assertion.inactive_at_block.is_none());
        assert!(assertion.active_at_block < assertion.inactive_at_block.unwrap());
    }

    #[tokio::test]
    async fn test_storage_is_persisted() {
        let time_lock_blocks = 1;
        let mut test_ctx = setup_int_test_indexer(BlockTag::Finalized, time_lock_blocks).await;
        let contract_address = Address::random();
        test_ctx
            .deploy_and_register_protocol(contract_address)
            .await;

        // Add an assertion
        let add_tx = test_ctx.add_assertion_tx_with_args(contract_address).await;
        test_harness(&mut test_ctx, vec![add_tx], Some(65)).await;

        // Retrieve the assertion from the store
        let assertions = test_ctx.store.get_assertions_for_contract(contract_address);
        assert_eq!(assertions.len(), 1);
        let assertion = assertions.first().unwrap();

        // Check that storage is persisted (not empty)
        assert!(
            !assertion.assertion_contract.storage.is_empty(),
            "Expected assertion contract storage to be persisted and not empty"
        );
        let storage_slot = assertion.assertion_contract.storage.get(&U256::from(0));
        assert!(storage_slot.is_some());
    }

    #[tokio::test]
    async fn test_triggers_are_persisted() {
        let time_lock_blocks = 1;
        let mut test_ctx = setup_int_test_indexer(BlockTag::Finalized, time_lock_blocks).await;
        let contract_address = Address::random();
        test_ctx
            .deploy_and_register_protocol(contract_address)
            .await;

        // Add an assertion
        let add_tx = test_ctx.add_assertion_tx(contract_address).await;
        test_harness(&mut test_ctx, vec![add_tx], Some(65)).await;

        // Retrieve the assertion from the store
        let assertions = test_ctx.store.get_assertions_for_contract(contract_address);
        assert_eq!(assertions.len(), 1);
        let assertion = assertions.first().unwrap();

        // Check that triggers are persisted (not empty)
        assert!(
            !assertion.trigger_recorder.triggers.is_empty(),
            "Expected triggers to be persisted and not empty"
        );
    }

    #[tokio::test]
    async fn test_active_at_block_is_expected() {
        for _ in 0..3 {
            let time_lock_blocks = rand::rng().random_range(1..=1000);
            let mut test_ctx = setup_int_test_indexer(BlockTag::Finalized, time_lock_blocks).await;
            let contract_address = Address::random();
            test_ctx
                .deploy_and_register_protocol(contract_address)
                .await;

            let latest_block = test_ctx.provider.get_block_number().await.unwrap();
            // The expected active_at_block is the block number at which the assertion passed the timeloc
            let expected_active_at_block = latest_block + time_lock_blocks + 1;
            // Add an assertion
            let add_tx = test_ctx.add_assertion_tx(contract_address).await;
            let remove_tx = test_ctx.remove_assertion_tx(contract_address, 0);
            // Mine 65 blocks for finalization
            test_harness(&mut test_ctx, vec![add_tx, remove_tx], Some(65)).await;

            // Retrieve the assertion from the store
            let assertions = test_ctx.store.get_assertions_for_contract(contract_address);
            assert_eq!(assertions.len(), 1);
            let assertion = assertions.first().unwrap();

            assert_eq!(assertion.active_at_block, expected_active_at_block);
            assert_eq!(
                assertion.inactive_at_block.unwrap(),
                expected_active_at_block
            );
        }
    }

    #[tokio::test]
    async fn test_active_at_block_is_expected_diff_blocks() {
        for _ in 0..3 {
            let time_lock_blocks = rand::rng().random_range(1..=1000);
            let mut test_ctx = setup_int_test_indexer(BlockTag::Finalized, time_lock_blocks).await;
            let contract_address = Address::random();
            test_ctx
                .deploy_and_register_protocol(contract_address)
                .await;

            let latest_block = test_ctx.provider.get_block_number().await.unwrap();
            // The expected active_at_block is the block number at which the assertion passed the timeloc
            let expected_active_at_block = latest_block + time_lock_blocks + 1;
            let expected_inactive_at_block = latest_block + time_lock_blocks + 2;
            // Add an assertion
            let add_tx = test_ctx.add_assertion_tx(contract_address).await;
            let remove_tx = test_ctx.remove_assertion_tx(contract_address, 0);
            // Mine 65 blocks for finalization
            test_harness(&mut test_ctx, vec![add_tx], Some(1)).await;
            test_harness(&mut test_ctx, vec![remove_tx], Some(65)).await;

            // Retrieve the assertion from the store
            let assertions = test_ctx.store.get_assertions_for_contract(contract_address);
            assert_eq!(assertions.len(), 1);
            let assertion = assertions.first().unwrap();

            assert_eq!(assertion.active_at_block, expected_active_at_block);
            assert_eq!(
                assertion.inactive_at_block.unwrap(),
                expected_inactive_at_block
            );
        }
    }

    #[tokio::test]
    async fn test_assertion_not_indexed_before_finalization() {
        let time_lock_blocks = 5;
        let mut test_ctx = setup_int_test_indexer(BlockTag::Finalized, time_lock_blocks).await;
        let contract_address = Address::random();
        test_ctx
            .deploy_and_register_protocol(contract_address)
            .await;

        // Add an assertion
        let add_tx = test_ctx.add_assertion_tx(contract_address).await;
        // Mine less than the required finalization blocks
        test_harness(&mut test_ctx, vec![add_tx], Some(64)).await;

        // Retrieve the assertion from the store (should not be present)
        let assertions = test_ctx.store.get_assertions_for_contract(contract_address);
        assert_eq!(
            assertions.len(),
            0,
            "Assertion should not be indexed before finalization"
        );

        // Mine the remaining blocks to reach finalization
        test_ctx.provider.anvil_mine(Some(1), None).await.unwrap();
        test_ctx.indexer.sync_to_head().await.unwrap();

        // Now the assertion should be present
        let assertions = test_ctx.store.get_assertions_for_contract(contract_address);
        assert_eq!(
            assertions.len(),
            1,
            "Assertion should be indexed after finalization"
        );
    }

    #[tokio::test]
    #[should_panic(expected = "NoCommonAncestor")]
    async fn test_indexer_with_reorgs_latest() {
        let time_lock_blocks = 1;
        let mut test_ctx = setup_int_test_indexer(BlockTag::Latest, time_lock_blocks).await;
        let contract_address = Address::random();
        test_ctx
            .deploy_and_register_protocol(contract_address)
            .await;

        let add_tx = test_ctx.add_assertion_tx(contract_address).await;
        test_harness(&mut test_ctx, vec![add_tx], Some(1)).await;

        // Revert the last block
        test_ctx
            .provider
            .anvil_reorg(ReorgOptions {
                depth: 1,
                tx_block_pairs: vec![],
            })
            .await
            .unwrap();

        test_ctx.indexer.sync_to_head().await.unwrap();

        let assertions = test_ctx.store.get_assertions_for_contract(contract_address);
        assert_eq!(assertions.len(), 0);
    }

    #[tokio::test]
    async fn test_indexer_with_reorgs_finalized() {
        for depth in [1, 10, 63] {
            let time_lock_blocks = 1;
            let mut test_ctx = setup_int_test_indexer(BlockTag::Finalized, time_lock_blocks).await;
            let contract_address = Address::random();
            test_ctx
                .deploy_and_register_protocol(contract_address)
                .await;

            let add_tx = test_ctx.add_assertion_tx(contract_address).await;
            test_harness(&mut test_ctx, vec![add_tx], Some(depth)).await;

            // Revert the last block
            test_ctx
                .provider
                .anvil_reorg(ReorgOptions {
                    depth,
                    tx_block_pairs: vec![],
                })
                .await
                .unwrap();

            test_ctx.indexer.sync_to_head().await.unwrap();

            let assertions = test_ctx.store.get_assertions_for_contract(contract_address);
            assert_eq!(assertions.len(), 0);

            let add_tx = test_ctx.add_assertion_tx(contract_address).await;
            test_harness(&mut test_ctx, vec![add_tx], Some(65)).await;

            let assertions = test_ctx.store.get_assertions_for_contract(contract_address);
            assert_eq!(assertions.len(), 1);
        }
    }

    #[tokio::test]
    async fn test_malformed_assertions() {
        let time_lock_blocks = 1;
        let mut test_ctx = setup_int_test_indexer(BlockTag::Latest, time_lock_blocks).await;
        let contract_address = Address::random();
        test_ctx
            .deploy_and_register_protocol(contract_address)
            .await;

        let malformed_submission_request =
            test_ctx.add_assertion_tx_malformed(contract_address).await;

        // Add malformed assertion (no triggers function)
        test_harness(&mut test_ctx, vec![malformed_submission_request], Some(1)).await;

        // Should not be indexed, should not panic.
        let assertions = test_ctx.store.get_assertions_for_contract(contract_address);
        assert_eq!(assertions.len(), 0);
    }

    #[tokio::test]
    async fn test_matching_oracle_signatures_ignored() {
        let time_lock_blocks = 1;
        let mut test_ctx = setup_int_test_indexer(BlockTag::Latest, time_lock_blocks).await;
        let contract_address = Address::random();
        let fake_oracle_address = Address::random();
        //Get code of state oracle
        let state_oracle_code = test_ctx
            .provider
            .get_code_at(test_ctx.contracts.state_oracle)
            .await
            .unwrap();

        //Insert to another address
        test_ctx
            .provider
            .anvil_set_code(fake_oracle_address, state_oracle_code)
            .await
            .unwrap();

        let real_oracle_address = test_ctx.contracts.state_oracle;

        for (oracle_address, expected_assertions) in
            [(fake_oracle_address, 0), (real_oracle_address, 1)]
        {
            // Set the oracle address to the fake oracle address and follow the same flow
            test_ctx.contracts.state_oracle = oracle_address;
            test_ctx
                .deploy_and_register_protocol(contract_address)
                .await;

            // Add assertion from unregistered contract
            let add_tx = test_ctx.add_assertion_tx(contract_address).await;
            test_harness(&mut test_ctx, vec![add_tx], Some(1)).await;

            // Should not index assertions from unregistered contracts
            let assertions = test_ctx.store.get_assertions_for_contract(contract_address);
            assert_eq!(assertions.len(), expected_assertions);
        }
    }
}
