pub mod build_evm;
pub mod macros;

#[cfg(test)]
mod selfdestruct_tests {
    use crate::{
        build_evm_by_features,
        evm::build_evm::evm_env,
        inspectors::CallTracer,
        primitives::{
            AccountInfo,
            Address,
            BlockEnv,
            Bytecode,
            Bytes,
            EvmExecutionResult,
            SpecId,
            TxEnv,
            TxKind,
            U256,
            address,
            keccak256,
        },
        test_utils::bytecode,
        wrap_tx_env_for_optimism,
    };
    use revm::{
        DatabaseCommit,
        ExecuteEvm,
        database::InMemoryDB,
        state::AccountStatus,
    };

    // Artifact paths
    const SELF_DESTRUCTABLE: &str = "SelfDestructTest.sol:SelfDestructable";
    const ORCHESTRATOR: &str = "SelfDestructTest.sol:Orchestrator";
    const PRE_EXISTING_CONTRACT: &str = "SelfDestructTest.sol:PreExistingContract";

    const DEPLOYER: Address = address!("1000000000000000000000000000000000000001");
    const BENEFICIARY: Address = address!("000000000000000000000000000000000000bEEF");

    /// Insert a funded account into the database
    fn insert_funded_account(db: &mut InMemoryDB, address: Address, balance: U256) {
        db.insert_account_info(
            address,
            AccountInfo {
                nonce: 0,
                balance,
                code_hash: keccak256([]),
                code: None,
            },
        );
    }

    /// Insert a contract with code into the database
    fn insert_contract(db: &mut InMemoryDB, address: Address, code: Bytes, balance: U256) {
        db.insert_account_info(
            address,
            AccountInfo {
                nonce: 1,
                balance,
                code_hash: keccak256(&code),
                code: Some(Bytecode::new_legacy(code)),
            },
        );
    }

    /// Build calldata for executeScenario()
    fn build_execute_scenario_calldata() -> Bytes {
        // Compute selector: keccak256("executeScenario()")[:4]
        let hash = keccak256("executeScenario()");
        let selector = &hash[..4];
        println!(
            "DEBUG: executeScenario() selector = {:02x}{:02x}{:02x}{:02x}",
            selector[0], selector[1], selector[2], selector[3]
        );
        Bytes::copy_from_slice(selector)
    }

    /// Build calldata for destroy(address)
    fn build_destroy_calldata(beneficiary: Address) -> Bytes {
        // Compute selector: keccak256("destroy(address)")[:4]
        let hash = keccak256("destroy(address)");
        let mut data = hash[..4].to_vec();
        data.extend_from_slice(&[0u8; 12]); // padding for address
        data.extend_from_slice(beneficiary.as_slice());
        Bytes::from(data)
    }

    /// Build calldata for setValue(uint256)
    fn build_setvalue_calldata(value: u64) -> Bytes {
        // Compute selector: keccak256("setValue(uint256)")[:4]
        let hash = keccak256("setValue(uint256)");
        let mut data = hash[..4].to_vec();
        data.extend_from_slice(&U256::from(value).to_be_bytes::<32>());
        Bytes::from(data)
    }

    /// The Orchestrator.executeScenario() does in ONE transaction:
    /// 1. Create Contract (with 1 ETH)
    /// 2. Write state (setValue(100))
    /// 3. Call selfdestruct (sends funds to BENEFICIARY)
    /// 4. Send funds to the destroyed address (0.5 ETH)
    #[test]
    fn test_same_tx_create_selfdestruct_send_funds_orchestrator() {
        let mut db = InMemoryDB::default();

        // Fund deployer with 100 ETH
        insert_funded_account(
            &mut db,
            DEPLOYER,
            U256::from(100_000_000_000_000_000_000u128),
        );

        let env = evm_env(1, SpecId::OSAKA, BlockEnv::default());
        let inspector = CallTracer::default();

        let mut evm = build_evm_by_features!(&mut db, &env, inspector);

        // Deploy Orchestrator
        let orchestrator_bytecode = bytecode(ORCHESTRATOR);
        println!(
            "Orchestrator bytecode length: {} bytes",
            orchestrator_bytecode.len()
        );

        let deploy_tx = TxEnv {
            caller: DEPLOYER,
            kind: TxKind::Create,
            data: orchestrator_bytecode,
            value: U256::ZERO,
            gas_limit: 5_000_000,
            gas_price: 1,
            nonce: 0,
            ..Default::default()
        };

        let result = evm
            .transact(wrap_tx_env_for_optimism!(deploy_tx))
            .expect("Orchestrator deployment should succeed");

        let orchestrator_addr = match &result.result {
            EvmExecutionResult::Success { output, .. } => {
                // Extract address from create output
                let addr = output.address().expect("Should have created address");
                println!("✓ Orchestrator deployed at: {:?}\n", addr);
                addr
            }
            other => {
                panic!("Orchestrator deployment failed: {:?}", other);
            }
        };

        // Commit deployment state
        db.commit(result.state);

        // Rebuild EVM after commit (db was mutably borrowed)
        let inspector = CallTracer::default();
        let mut evm = build_evm_by_features!(&mut db, &env, inspector);

        // Call executeScenario() - this does everything in ONE transaction
        // Selector: keccak256("executeScenario()")[:4]
        // Verify with: cast sig "executeScenario()"
        let execute_tx = TxEnv {
            caller: DEPLOYER,
            kind: TxKind::Call(*orchestrator_addr),
            data: build_execute_scenario_calldata(),
            value: U256::from(2_000_000_000_000_000_000u128), // 2 ETH
            gas_limit: 10_000_000,
            gas_price: 1,
            nonce: 1,
            ..Default::default()
        };

        println!("Calling executeScenario() with:");
        println!("  caller: {:?}", DEPLOYER);
        println!("  target: {:?}", orchestrator_addr);
        println!("  value: 2 ETH");
        println!("  data: {:?}", build_execute_scenario_calldata());
        println!();

        let result = evm
            .transact(wrap_tx_env_for_optimism!(execute_tx))
            .expect("executeScenario should succeed");

        // Check execution result
        match &result.result {
            EvmExecutionResult::Success { gas_used, .. } => {
                println!("executeScenario() succeeded");
                println!("Gas used: {}\n", gas_used);
            }
            EvmExecutionResult::Revert {
                output, gas_used, ..
            } => {
                println!("Transaction reverted!");
                println!("Gas used: {}", gas_used);
                println!("Output (raw): {:?}", output);
                println!("Output length: {} bytes", output.len());

                // Try to decode revert reason if it looks like Error(string)
                if output.len() >= 4 {
                    let selector = &output[..4];
                    println!(
                        "Revert selector: {:02x}{:02x}{:02x}{:02x}",
                        selector[0], selector[1], selector[2], selector[3]
                    );

                    // Error(string) selector is 0x08c379a0
                    if selector == [0x08, 0xc3, 0x79, 0xa0] && output.len() > 68 {
                        // Try to decode the string
                        let string_data = &output[68..];
                        if let Ok(s) = String::from_utf8(string_data.to_vec()) {
                            println!("Revert reason: {}", s.trim_matches('\0'));
                        }
                    }
                }
            }
            EvmExecutionResult::Halt { reason, gas_used } => {
                panic!("Transaction halted: {:?}, gas_used: {}", reason, gas_used);
            }
        }

        for (addr, account) in result.state.iter() {
            // Skip deployer and orchestrator for cleaner output
            if *addr == DEPLOYER || *addr == *orchestrator_addr {
                continue;
            }

            let is_selfdestructed = account.status.contains(AccountStatus::SelfDestructed);
            let is_created = account.status.contains(AccountStatus::Created);
            let is_touched = account.status.contains(AccountStatus::Touched);
            let is_empty = account.is_empty();

            println!("Address: {:?}", addr);
            println!("  Status flags (raw): {:?}", account.status);
            println!("    SelfDestructed: {}", is_selfdestructed);
            println!("    Created: {}", is_created);
            println!("    Touched: {}", is_touched);
            println!("    Empty: {}", is_empty);
            println!("  Account info:");
            println!("    balance: {} wei", account.info.balance);
            println!("    nonce: {}", account.info.nonce);
            println!("    has_code: {}", account.info.code.is_some());
            if let Some(ref code) = account.info.code {
                println!("    code_size: {} bytes", code.len());
            }
            println!("  Storage changes: {} slots", account.storage.len());
            println!();
        }
    }
}
