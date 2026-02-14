use alloy_sol_types::sol;

// Canonical PhEvm interface loaded from Solidity source file.
// This is the single source of truth for the PhEvm precompile ABI.
// The downstream credible-std/src/PhEvm.sol should be kept in sync.
sol!("interfaces/PhEvm.sol");

// Canonical ITriggerRecorder interface loaded from Solidity source file.
// This is the single source of truth for the TriggerRecorder precompile ABI.
// The downstream credible-std/src/TriggerRecorder.sol should be kept in sync
// (note: credible-std uses the name TriggerRecorder without the I prefix).
sol!("interfaces/ITriggerRecorder.sol");

sol! {
    interface Console {
        // Log a message to the console.
        function log(string message) external;
    }

    #[derive(Debug)]
    error Error(string);
}

sol! {
    interface console {
        function log(string message) external;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_sol_types::SolCall;

    /// Guards against accidental removal or modification of `PhEvm` interface methods.
    /// If any selector changes, it means the ABI has changed and downstream
    /// credible-std must be updated in sync.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_phevm_interface_selectors_are_stable() {
        // Each selector is the first 4 bytes of keccak256(signature).
        // These values are ABI-critical and must not change.
        let expected_selectors: Vec<(&str, [u8; 4])> = vec![
            ("forkPreTx()", PhEvm::forkPreTxCall::SELECTOR),
            ("forkPostTx()", PhEvm::forkPostTxCall::SELECTOR),
            ("forkPreCall(uint256)", PhEvm::forkPreCallCall::SELECTOR),
            ("forkPostCall(uint256)", PhEvm::forkPostCallCall::SELECTOR),
            ("load(address,bytes32)", PhEvm::loadCall::SELECTOR),
            ("getLogs()", PhEvm::getLogsCall::SELECTOR),
            (
                "getAllCallInputs(address,bytes4)",
                PhEvm::getAllCallInputsCall::SELECTOR,
            ),
            (
                "getCallInputs(address,bytes4)",
                PhEvm::getCallInputsCall::SELECTOR,
            ),
            (
                "getStaticCallInputs(address,bytes4)",
                PhEvm::getStaticCallInputsCall::SELECTOR,
            ),
            (
                "getDelegateCallInputs(address,bytes4)",
                PhEvm::getDelegateCallInputsCall::SELECTOR,
            ),
            (
                "getCallCodeInputs(address,bytes4)",
                PhEvm::getCallCodeInputsCall::SELECTOR,
            ),
            (
                "getStateChanges(address,bytes32)",
                PhEvm::getStateChangesCall::SELECTOR,
            ),
            (
                "getAssertionAdopter()",
                PhEvm::getAssertionAdopterCall::SELECTOR,
            ),
            ("getTxObject()", PhEvm::getTxObjectCall::SELECTOR),
            // Scalar call-fact cheatcodes
            (
                "anyCall(address,bytes4,(uint8,uint32,uint32,bool,bool))",
                PhEvm::anyCall_0Call::SELECTOR,
            ),
            ("anyCall(address,bytes4)", PhEvm::anyCall_1Call::SELECTOR),
            (
                "countCalls(address,bytes4,(uint8,uint32,uint32,bool,bool))",
                PhEvm::countCalls_0Call::SELECTOR,
            ),
            (
                "countCalls(address,bytes4)",
                PhEvm::countCalls_1Call::SELECTOR,
            ),
            ("callerAt(uint256)", PhEvm::callerAtCall::SELECTOR),
            (
                "allCallsBy(address,bytes4,address,(uint8,uint32,uint32,bool,bool))",
                PhEvm::allCallsBy_0Call::SELECTOR,
            ),
            (
                "allCallsBy(address,bytes4,address)",
                PhEvm::allCallsBy_1Call::SELECTOR,
            ),
            (
                "sumArgUint(address,bytes4,uint256,(uint8,uint32,uint32,bool,bool))",
                PhEvm::sumArgUint_0Call::SELECTOR,
            ),
            (
                "sumArgUint(address,bytes4,uint256)",
                PhEvm::sumArgUint_1Call::SELECTOR,
            ),
            (
                "sumCallArgUintForAddress(address,bytes4,uint256,address,uint256,(uint8,uint32,uint32,bool,bool))",
                PhEvm::sumCallArgUintForAddressCall::SELECTOR,
            ),
            (
                "uniqueCallArgAddresses(address,bytes4,uint256,(uint8,uint32,uint32,bool,bool))",
                PhEvm::uniqueCallArgAddressesCall::SELECTOR,
            ),
            (
                "sumCallArgUintByAddress(address,bytes4,uint256,uint256,(uint8,uint32,uint32,bool,bool))",
                PhEvm::sumCallArgUintByAddressCall::SELECTOR,
            ),
            (
                "sumEventUintForTopicKey(address,bytes32,uint8,bytes32,uint256)",
                PhEvm::sumEventUintForTopicKeyCall::SELECTOR,
            ),
            (
                "uniqueEventTopicValues(address,bytes32,uint8)",
                PhEvm::uniqueEventTopicValuesCall::SELECTOR,
            ),
            (
                "sumEventUintByTopic(address,bytes32,uint8,uint256)",
                PhEvm::sumEventUintByTopicCall::SELECTOR,
            ),
            // Storage write-policy cheatcodes
            (
                "anySlotWritten(address,bytes32)",
                PhEvm::anySlotWrittenCall::SELECTOR,
            ),
            (
                "allSlotWritesBy(address,bytes32,address)",
                PhEvm::allSlotWritesByCall::SELECTOR,
            ),
            (
                "getTouchedContracts((uint8,uint32,uint32,bool,bool))",
                PhEvm::getTouchedContractsCall::SELECTOR,
            ),
            (
                "countEvents(address,bytes32)",
                PhEvm::countEventsCall::SELECTOR,
            ),
            ("anyEvent(address,bytes32)", PhEvm::anyEventCall::SELECTOR),
            (
                "sumEventDataUint(address,bytes32,uint256)",
                PhEvm::sumEventDataUintCall::SELECTOR,
            ),
            // Call-boundary state cheatcodes
            (
                "loadAtCall(address,bytes32,uint256,uint8)",
                PhEvm::loadAtCallCall::SELECTOR,
            ),
            (
                "slotDeltaAtCall(address,bytes32,uint256)",
                PhEvm::slotDeltaAtCallCall::SELECTOR,
            ),
            (
                "allCallsSlotDeltaGE(address,bytes4,bytes32,int256,(uint8,uint32,uint32,bool,bool))",
                PhEvm::allCallsSlotDeltaGECall::SELECTOR,
            ),
            (
                "allCallsSlotDeltaLE(address,bytes4,bytes32,int256,(uint8,uint32,uint32,bool,bool))",
                PhEvm::allCallsSlotDeltaLECall::SELECTOR,
            ),
            (
                "sumCallsSlotDelta(address,bytes4,bytes32,(uint8,uint32,uint32,bool,bool))",
                PhEvm::sumCallsSlotDeltaCall::SELECTOR,
            ),
            // Trigger context cheatcode
            (
                "getTriggerContext()",
                PhEvm::getTriggerContextCall::SELECTOR,
            ),
            // ERC20 fact cheatcodes
            (
                "erc20BalanceDiff(address,address)",
                PhEvm::erc20BalanceDiffCall::SELECTOR,
            ),
            (
                "erc20SupplyDiff(address)",
                PhEvm::erc20SupplyDiffCall::SELECTOR,
            ),
            (
                "erc20BalanceAt(address,address,uint8)",
                PhEvm::erc20BalanceAtCall::SELECTOR,
            ),
            (
                "erc20SupplyAt(address,uint8)",
                PhEvm::erc20SupplyAtCall::SELECTOR,
            ),
            (
                "erc20AllowanceAt(address,address,address,uint8)",
                PhEvm::erc20AllowanceAtCall::SELECTOR,
            ),
            (
                "erc20AllowanceDiff(address,address,address)",
                PhEvm::erc20AllowanceDiffCall::SELECTOR,
            ),
            (
                "erc20BalanceDeltaAtCall(address,address,uint256)",
                PhEvm::erc20BalanceDeltaAtCallCall::SELECTOR,
            ),
            (
                "erc20SupplyDeltaAtCall(address,uint256)",
                PhEvm::erc20SupplyDeltaAtCallCall::SELECTOR,
            ),
            (
                "erc20AllowanceAtCall(address,address,address,uint256,uint8)",
                PhEvm::erc20AllowanceAtCallCall::SELECTOR,
            ),
            (
                "erc20AllowanceDeltaAtCall(address,address,address,uint256)",
                PhEvm::erc20AllowanceDeltaAtCallCall::SELECTOR,
            ),
            (
                "getERC20NetFlow(address,address)",
                PhEvm::getERC20NetFlowCall::SELECTOR,
            ),
            (
                "getERC20FlowByCall(address,address,uint256)",
                PhEvm::getERC20FlowByCallCall::SELECTOR,
            ),
            (
                "erc4626TotalAssetsDiff(address)",
                PhEvm::erc4626TotalAssetsDiffCall::SELECTOR,
            ),
            (
                "erc4626TotalSupplyDiff(address)",
                PhEvm::erc4626TotalSupplyDiffCall::SELECTOR,
            ),
            (
                "erc4626VaultAssetBalanceDiff(address)",
                PhEvm::erc4626VaultAssetBalanceDiffCall::SELECTOR,
            ),
            (
                "erc4626AssetsPerShareDiffBps(address)",
                PhEvm::erc4626AssetsPerShareDiffBpsCall::SELECTOR,
            ),
            (
                "erc4626TotalAssetsDeltaAtCall(address,uint256)",
                PhEvm::erc4626TotalAssetsDeltaAtCallCall::SELECTOR,
            ),
            (
                "erc4626TotalSupplyDeltaAtCall(address,uint256)",
                PhEvm::erc4626TotalSupplyDeltaAtCallCall::SELECTOR,
            ),
            (
                "erc4626VaultAssetBalanceDeltaAtCall(address,uint256)",
                PhEvm::erc4626VaultAssetBalanceDeltaAtCallCall::SELECTOR,
            ),
            // P1: State/Mapping diff cheatcodes
            (
                "getChangedSlots(address)",
                PhEvm::getChangedSlotsCall::SELECTOR,
            ),
            (
                "getSlotDiff(address,bytes32)",
                PhEvm::getSlotDiffCall::SELECTOR,
            ),
            (
                "didMappingKeyChange(address,bytes32,bytes32,uint256)",
                PhEvm::didMappingKeyChangeCall::SELECTOR,
            ),
            (
                "mappingValueDiff(address,bytes32,bytes32,uint256)",
                PhEvm::mappingValueDiffCall::SELECTOR,
            ),
            (
                "didBalanceChange(address,address)",
                PhEvm::didBalanceChangeCall::SELECTOR,
            ),
            (
                "balanceDiff(address,address)",
                PhEvm::balanceDiffCall::SELECTOR,
            ),
        ];

        // Verify all selectors are non-zero (sanity check)
        for (name, selector) in &expected_selectors {
            assert_ne!(
                *selector, [0u8; 4],
                "Selector for {name} should not be zero"
            );
        }

        // Verify we have the expected count of methods
        assert_eq!(
            expected_selectors.len(),
            66,
            "PhEvm interface should have exactly 66 methods"
        );
    }

    /// Guards against accidental removal or modification of `ITriggerRecorder` interface methods.
    #[test]
    fn test_trigger_recorder_interface_selectors_are_stable() {
        let expected_selectors: Vec<(&str, [u8; 4])> = vec![
            (
                "registerCallTrigger(bytes4)",
                ITriggerRecorder::registerCallTrigger_0Call::SELECTOR,
            ),
            (
                "registerCallTrigger(bytes4,bytes4)",
                ITriggerRecorder::registerCallTrigger_1Call::SELECTOR,
            ),
            (
                "registerCallTrigger(bytes4,(uint8,uint32,uint32,bool,bool))",
                ITriggerRecorder::registerCallTrigger_2Call::SELECTOR,
            ),
            (
                "registerCallTrigger(bytes4,bytes4,(uint8,uint32,uint32,bool,bool))",
                ITriggerRecorder::registerCallTrigger_3Call::SELECTOR,
            ),
            (
                "registerCallTriggers(bytes4,bytes4[])",
                ITriggerRecorder::registerCallTriggers_0Call::SELECTOR,
            ),
            (
                "registerCallTriggers(bytes4,bytes4[],(uint8,uint32,uint32,bool,bool))",
                ITriggerRecorder::registerCallTriggers_1Call::SELECTOR,
            ),
            (
                "registerStorageChangeTrigger(bytes4)",
                ITriggerRecorder::registerStorageChangeTrigger_0Call::SELECTOR,
            ),
            (
                "registerStorageChangeTrigger(bytes4,bytes32)",
                ITriggerRecorder::registerStorageChangeTrigger_1Call::SELECTOR,
            ),
            (
                "registerBalanceChangeTrigger(bytes4)",
                ITriggerRecorder::registerBalanceChangeTriggerCall::SELECTOR,
            ),
        ];

        for (name, selector) in &expected_selectors {
            assert_ne!(
                *selector, [0u8; 4],
                "Selector for {name} should not be zero"
            );
        }

        assert_eq!(
            expected_selectors.len(),
            9,
            "ITriggerRecorder interface should have exactly 9 methods"
        );
    }

    /// Verifies that overloaded functions produce distinct selectors.
    #[test]
    fn test_overloaded_functions_have_distinct_selectors() {
        // registerCallTrigger(bytes4) vs registerCallTrigger(bytes4,bytes4)
        assert_ne!(
            ITriggerRecorder::registerCallTrigger_0Call::SELECTOR,
            ITriggerRecorder::registerCallTrigger_1Call::SELECTOR,
            "registerCallTrigger overloads must have distinct selectors"
        );
        assert_ne!(
            ITriggerRecorder::registerCallTrigger_0Call::SELECTOR,
            ITriggerRecorder::registerCallTrigger_2Call::SELECTOR,
            "registerCallTrigger overloads must have distinct selectors"
        );
        assert_ne!(
            ITriggerRecorder::registerCallTrigger_1Call::SELECTOR,
            ITriggerRecorder::registerCallTrigger_3Call::SELECTOR,
            "registerCallTrigger overloads must have distinct selectors"
        );
        assert_ne!(
            ITriggerRecorder::registerCallTrigger_2Call::SELECTOR,
            ITriggerRecorder::registerCallTrigger_3Call::SELECTOR,
            "registerCallTrigger overloads must have distinct selectors"
        );

        // registerStorageChangeTrigger(bytes4) vs registerStorageChangeTrigger(bytes4,bytes32)
        assert_ne!(
            ITriggerRecorder::registerStorageChangeTrigger_0Call::SELECTOR,
            ITriggerRecorder::registerStorageChangeTrigger_1Call::SELECTOR,
            "registerStorageChangeTrigger overloads must have distinct selectors"
        );

        // registerCallTriggers overloads
        assert_ne!(
            ITriggerRecorder::registerCallTriggers_0Call::SELECTOR,
            ITriggerRecorder::registerCallTriggers_1Call::SELECTOR,
            "registerCallTriggers overloads must have distinct selectors"
        );
    }
}
