pub use revm::{
    db::AccountState,
    primitives::{
        address,
        bytes,
        fixed_bytes,
        hex,
        keccak256,
        result::ResultAndState,
        uint,
        Account,
        AccountInfo,
        AccountStatus,
        Address,
        BlockEnv,
        Bytecode,
        Bytes,
        EVMError,
        EvmState,
        EvmStorage,
        EvmStorageSlot,
        ExecutionResult as EvmExecutionResult,
        FixedBytes,
        Output,
        SpecId,
        TxEnv,
        TxKind,
        B256,
        U256,
    },
    JournalEntry,
    JournaledState,
};

use serde::{
    Deserialize,
    Serialize,
};

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssertionContract {
    /// The hash of the deployment data.
    pub id: B256,

    /// The deployed code of the assertion contract.
    pub deployed_code: Bytecode,
    /// The hash of the deployed code.
    pub code_hash: B256,
    /// The Storage of the contract after being deployed against an empty state.
    pub storage: EvmStorage,
    /// The account state of the contract after being deployed against an empty state.
    pub account_status: AccountStatus,
}

/// Id of an assertion function
#[derive(Debug, Clone, Copy)]
pub struct AssertionFnId {
    /// The selector of the assertion function
    pub fn_selector: FixedBytes<4>,
    /// The hash of the assertion contract deployment data.
    pub assertion_contract_id: B256,
}

/// Result of a transaction validation against a set of assertions
#[derive(Debug)]
pub struct TxValidationResult {
    /// Whether the transaction is valid
    pub transaction_valid: bool,
    /// Result of the transaction execution
    pub result_and_state: ResultAndState,
    /// Results of the assertions executions
    pub assertions_executions: Vec<AssertionContractExecution>,
}

impl TxValidationResult {
    /// Create a new TxValidationResult instance
    pub fn new(
        transaction_valid: bool,
        result_and_state: ResultAndState,
        assertions_executions: Vec<AssertionContractExecution>,
    ) -> Self {
        Self {
            transaction_valid,
            result_and_state,
            assertions_executions,
        }
    }
    /// Whether the transaction is valid
    pub fn is_valid(&self) -> bool {
        self.transaction_valid
    }

    /// Total gas used to execute all the assertion functions of all the assertion contracts
    pub fn total_assertions_gas(&self) -> u64 {
        self.assertions_executions
            .iter()
            .map(|a| a.total_assertion_gas)
            .sum()
    }

    /// The number of assertion functions that were executed
    pub fn total_assertion_funcs_ran(&self) -> u64 {
        self.assertions_executions
            .iter()
            .map(|a| a.total_assertion_funcs_ran)
            .sum()
    }
}

/// Result of a single assertion contract execution
#[derive(Debug, Default)]
pub struct AssertionContractExecution {
    /// Results of the assertion functions executions
    pub assertion_fns_results: Vec<AssertionFunctionResult>,
    /// Total gas used to execute all the assertion functions of the assertion contract
    pub total_assertion_gas: u64,
    /// The number of assertion functions that were executed
    pub total_assertion_funcs_ran: u64,
}

/// Result of a single assertion function execution
#[derive(Debug)]
pub struct AssertionFunctionResult {
    /// The id of the assertion function
    pub id: AssertionFnId,
    /// The result of the assertion function execution
    pub result: AssertionFunctionExecutionResult,
}

#[derive(Debug)]
pub enum AssertionFunctionExecutionResult {
    /// The constructor function of the assertion contract failed to execute
    AssertionContractDeployFailure(EvmExecutionResult),
    /// The assertion function execution result
    AssertionExecutionResult(EvmExecutionResult),
}

impl AssertionFunctionResult {
    /// Whether the assertion function execution was succesful or reverted
    pub fn is_success(&self) -> bool {
        if let AssertionFunctionExecutionResult::AssertionExecutionResult(result) = &self.result {
            result.is_success()
        } else {
            false
        }
    }

    /// Convert the assertion function execution result into an execution result
    pub fn as_result(&self) -> &EvmExecutionResult {
        match &self.result {
            AssertionFunctionExecutionResult::AssertionContractDeployFailure(result) => result,
            AssertionFunctionExecutionResult::AssertionExecutionResult(result) => result,
        }
    }
}

/// Represents an update block or fork choice update
/// Has all information required to update to a new fork choice
#[derive(Debug)]
pub struct UpdateBlock {
    pub block_number: u64,
    pub block_hash: B256,
    pub parent_hash: B256,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BlockChanges {
    pub block_num: u64,
    pub block_hash: B256,
    pub state_changes: EvmState,
}

impl BlockChanges {
    /// Create a new BlockChanges instance, with empty state changes.
    pub fn new(block_num: u64, block_hash: B256) -> Self {
        Self {
            block_num,
            block_hash,
            state_changes: Default::default(),
        }
    }
    /// Merge `Vec<HashMap<Address, Account>>` into block changes.
    pub fn merge_state(&mut self, evm_state: EvmState) {
        // Process states in order so later states override earlier ones for overlapping values
        for (key, value) in evm_state {
            if let Some(existing_account) = self.state_changes.get_mut(&key) {
                // Update account info and status from latest
                existing_account.info = value.info;
                // Update account info and status from latest
                existing_account.status = value.status;
                // Merge storage - keep old slots but override with new values when they exist
                existing_account.storage.extend(value.storage);
            } else {
                self.state_changes.insert(key, value);
            }
        }
    }
}

#[cfg(test)]
mod test_merge_state {
    use super::*;

    use std::str::FromStr;

    use std::collections::HashMap;

    // Helper functions remain the same
    fn create_test_account(balance: u64, nonce: u64, code: Vec<u8>) -> Account {
        Account {
            info: AccountInfo {
                balance: U256::from(balance),
                nonce,
                code_hash: FixedBytes::<32>::default(),
                code: Some(Bytecode::LegacyRaw(code.into())),
            },
            status: AccountStatus::default(),
            storage: HashMap::from_iter([]),
        }
    }

    fn create_storage_slot(value: u64) -> EvmStorageSlot {
        EvmStorageSlot::new(U256::from(value))
    }

    fn addr(s: &str) -> Address {
        Address::from_str(s).unwrap()
    }

    #[test]
    fn test_merge_storage_changes() {
        let mut state0 = HashMap::from_iter([]);
        let mut state1 = HashMap::from_iter([]);
        let mut state2 = HashMap::from_iter([]);

        let addr = addr("0x1000000000000000000000000000000000000000");

        // Initial state
        let mut account0 = create_test_account(100, 1, vec![0x60]);
        account0
            .storage
            .insert(U256::from(1), create_storage_slot(10));
        account0
            .storage
            .insert(U256::from(2), create_storage_slot(20));
        state0.insert(addr, account0);

        // State update 1
        let mut account1 = create_test_account(200, 2, vec![0x60]);
        account1
            .storage
            .insert(U256::from(2), create_storage_slot(25)); // Modify existing slot
        account1
            .storage
            .insert(U256::from(3), create_storage_slot(30)); // Add new slot
        state1.insert(addr, account1);

        // State update 2 (latest)
        let mut account2 = create_test_account(300, 3, vec![0x60]);
        account2
            .storage
            .insert(U256::from(3), create_storage_slot(35)); // Modify slot from state1
        account2
            .storage
            .insert(U256::from(4), create_storage_slot(40)); // Add new slot
        state2.insert(addr, account2);

        let block_changes = BlockChanges {
            state_changes: state0,
            ..Default::default()
        };

        // States pushed in order [state0, state1, state2]
        // After merging, we expect:
        // - Account info from state2 (balance 300, nonce 3)
        // - Combined storage with latest values taking precedence:
        //   - slot 1 = 10 (from state0, unchanged)
        //   - slot 2 = 25 (from state1, overrode state0)
        //   - slot 3 = 35 (from state2, overrode state1)
        //   - slot 4 = 40 (from state2)
        let mut merged = block_changes.clone();
        merged.merge_state(state1);
        merged.merge_state(state2);

        let merged_account = merged.state_changes.get(&addr).unwrap();

        // Verify account info is from latest state
        assert_eq!(
            merged_account.info.balance,
            U256::from(300),
            "Should have latest balance"
        );
        assert_eq!(merged_account.info.nonce, 3, "Should have latest nonce");

        // Verify storage has combined values with latest taking precedence
        assert_eq!(
            merged_account
                .storage
                .get(&U256::from(1))
                .expect("Storage slot 1 should exist")
                .present_value(),
            U256::from(10),
            "Should keep original value from state0"
        );

        assert_eq!(
            merged_account
                .storage
                .get(&U256::from(2))
                .expect("Storage slot 2 should exist")
                .present_value(),
            U256::from(25),
            "Should have state1's value for slot 2"
        );

        assert_eq!(
            merged_account
                .storage
                .get(&U256::from(3))
                .expect("Storage slot 3 should exist")
                .present_value(),
            U256::from(35),
            "Should have state2's value for slot 3"
        );

        assert_eq!(
            merged_account
                .storage
                .get(&U256::from(4))
                .expect("Storage slot 4 should exist")
                .present_value(),
            U256::from(40),
            "Should have state2's value for slot 4"
        );
    }

    #[test]
    fn test_merge_status_changes() {
        let mut state0 = HashMap::from_iter([]);
        let mut state1 = HashMap::from_iter([]);

        let addr = addr("0x1000000000000000000000000000000000000000");

        // Initial state
        let mut account0 = create_test_account(100, 1, vec![]);
        account0.status = AccountStatus::default();
        state0.insert(addr, account0);

        // Updated state (latest)
        let mut account1 = create_test_account(200, 2, vec![]);
        account1.status = AccountStatus::default();
        state1.insert(addr, account1);

        let block_changes = BlockChanges {
            state_changes: state0,
            ..Default::default()
        };

        let mut merged = block_changes.clone();
        merged.merge_state(state1);

        let merged_account = merged.state_changes.get(&addr).unwrap();

        // Verify account info is from latest state
        assert_eq!(
            merged_account.info.balance,
            U256::from(200),
            "Should have latest balance"
        );
        assert_eq!(merged_account.info.nonce, 2, "Should have latest nonce");
    }

    #[test]
    fn test_merge_new_accounts() {
        let mut state0 = HashMap::from_iter([]);
        let mut state1 = HashMap::from_iter([]);

        let addr1 = addr("0x1000000000000000000000000000000000000000");
        let addr2 = addr("0x2000000000000000000000000000000000000000");

        // First state has account1
        let account1_initial = create_test_account(100, 1, vec![]);
        state0.insert(addr1, account1_initial);

        // Second state updates account1 and adds account2
        let account1_updated = create_test_account(200, 2, vec![]);
        let account2 = create_test_account(300, 1, vec![]);

        state1.insert(addr1, account1_updated);
        state1.insert(addr2, account2);

        let block_changes = BlockChanges {
            state_changes: state0,
            ..Default::default()
        };

        let mut merged = block_changes.clone();
        merged.merge_state(state1);

        // Verify account1 has latest state
        let merged_account1 = merged.state_changes.get(&addr1).unwrap();
        assert_eq!(
            merged_account1.info.balance,
            U256::from(200),
            "Account1 should have latest balance"
        );
        assert_eq!(
            merged_account1.info.nonce, 2,
            "Account1 should have latest nonce"
        );

        // Verify account2 exists with its state
        let merged_account2 = merged.state_changes.get(&addr2).unwrap();
        assert_eq!(
            merged_account2.info.balance,
            U256::from(300),
            "Account2 should be present with its balance"
        );
        assert_eq!(
            merged_account2.info.nonce, 1,
            "Account2 should be present with its nonce"
        );
    }
}
