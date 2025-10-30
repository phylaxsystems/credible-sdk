use alloy::{
    consensus::{
        Transaction,
        TxType,
    },
    eips::eip2930::AccessListItem,
    network::TransactionResponse,
    signers::Either,
};
use alloy_primitives::{
    Address,
    B256,
    Bytes,
    U256,
};
use alloy_provider::{
    Provider,
    ProviderBuilder,
    RootProvider,
};
use anyhow::{
    Context,
    Result,
};
use assertion_executor::{
    build_evm_by_features,
    db::{
        DatabaseRef,
        overlay::OverlayDb,
    },
    evm::build_evm::evm_env,
    inspectors::CallTracer,
    wrap_tx_env_for_optimism,
};
use revm::{
    Database,
    DatabaseCommit,
    InspectEvm,
    context::{
        BlockEnv,
        TxEnv,
        result::{
            ExecutionResult,
            Output,
        },
        tx::{
            TxEnvBuildError,
            TxEnvBuilder,
        },
    },
    context_interface::block::BlobExcessGasAndPrice,
    inspector::NoOpInspector,
    primitives::hardfork::SpecId,
    state::{
        Account,
        AccountInfo,
        Bytecode,
    },
};
use sidecar::{
    Cache,
    Source,
    SourceError,
    cache::sources::SourceName,
};
use std::{
    collections::HashMap,
    sync::Arc,
};

/// RPC Database that fetches state on-demand from an Ethereum RPC node
#[derive(Debug)]
struct RpcDatabase {
    provider: Arc<RootProvider>,
    block_number: u64,
    cache: HashMap<Address, AccountInfo>,
    storage_cache: HashMap<(Address, U256), U256>,
}

impl RpcDatabase {
    fn new(provider: Arc<RootProvider>, block_number: u64) -> Self {
        Self {
            provider,
            block_number,
            cache: HashMap::new(),
            storage_cache: HashMap::new(),
        }
    }
}

impl revm::context::DBErrorMarker for RpcDatabaseError {}

#[derive(Debug, thiserror::Error)]
pub enum RpcDatabaseError {}

impl DatabaseRef for RpcDatabase {
    type Error = SourceError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        // Check cache first
        if let Some(info) = self.cache.get(&address) {
            return Ok(Some(info.clone()));
        }

        let future = async {
            // Fetch balance
            let balance = self
                .provider
                .get_balance(address)
                .block_id(self.block_number.into())
                .await
                .expect("Failed to fetch balance");

            // Fetch nonce
            let nonce = self
                .provider
                .get_transaction_count(address)
                .block_id(self.block_number.into())
                .await
                .expect("Failed to fetch nonce");

            // Fetch code
            let code = self
                .provider
                .get_code_at(address)
                .block_id(self.block_number.into())
                .await
                .expect("Failed to fetch code");

            let bytecode = if code.is_empty() {
                Bytecode::new()
            } else {
                Bytecode::new_raw(code)
            };

            let code_hash = bytecode.hash_slow();

            let info = AccountInfo {
                balance,
                nonce,
                code_hash,
                code: Some(bytecode),
            };

            Ok(Some(info))
        };

        let handle = tokio::runtime::Handle::current();
        let result = std::thread::scope(|s| {
            s.spawn(move || handle.block_on(future))
                .join()
                .expect("Failed to join thread")
        });

        // Cache the result
        // if let Ok(Some(ref info)) = result {
        //     self.cache.insert(address, info.clone());
        // }

        println!("basic_ref requeted for address: {}", address);
        println!("basic_ref result: {:?}", result);

        result
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        // Check cache first
        if let Some(value) = self.storage_cache.get(&(address, index)) {
            return Ok(*value);
        }

        let future = async {
            let value = self
                .provider
                .get_storage_at(address, index)
                .block_id(self.block_number.into())
                .await
                .expect("Failed to fetch storage");

            Ok(value)
        };

        let handle = tokio::runtime::Handle::current();
        let result = std::thread::scope(|s| {
            s.spawn(move || handle.block_on(future))
                .join()
                .expect("Failed to join thread")
        });

        // Cache the result
        // if let Ok(value) = result {
        //     self.storage_cache.insert((address, index), value);
        // }

        println!(
            "storage_ref requeted for address: {}, index: {}",
            address, index
        );
        println!("storage_ref result: {:?}", result);

        result
    }

    fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
        println!("code_by_hash_ref requeted for code_hash: {}", _code_hash);
        Ok(Bytecode::new())
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        let future = async {
            let block = self
                .provider
                .get_block_by_number(number.into())
                .await
                .expect("Failed to fetch block")
                .expect("Block not found");

            Ok::<alloy_primitives::FixedBytes<32>, SourceError>(block.header.hash)
        };

        let handle = tokio::runtime::Handle::current();
        let res = std::thread::scope(|s| {
            s.spawn(move || handle.block_on(future))
                .join()
                .expect("Failed to join thread")
        })?;

        println!("block_hash_ref requeted for number: {}", number);
        println!("block_hash_ref result: {:?}", res);

        Ok(res)
    }
}

impl Source for RpcDatabase {
    fn is_synced(&self, _: u64) -> bool {
        true
    }

    fn name(&self) -> SourceName {
        SourceName::Sequencer
    }

    fn update_target_block(&self, _: u64) {}
}

impl DatabaseCommit for RpcDatabase {
    fn commit(&mut self, changes: revm::primitives::HashMap<Address, Account>) {
        // Apply state changes to our cache
        for (address, account) in changes {
            self.cache.insert(address, account.info);

            // Update storage cache
            for (key, value) in account.storage {
                self.storage_cache
                    .insert((address, key), value.present_value);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // ========================================
    // 🔧 CONFIGURATION - CHANGE THESE VALUES
    // ========================================
    let rpc_url = "https://rpc.linea.build";
    let target_tx_hash = "0xc2b2a274220d63e11c96bfa82f22429844351900908c04abc50d046a70c3fa10";

    // 🎯 START FROM THIS BLOCK (set to None to start from target tx block)
    let start_from_block: Option<u64> = None; //Some(25079830);
    // Example: Some(10000000) = start from block 10M
    //          None = start from the same block as target tx

    println!("╔══════════════════════════════════════════════════╗");
    println!("║     Multi-Block Transaction Replayer            ║");
    println!("╚══════════════════════════════════════════════════╝");
    println!();
    println!("🔗 RPC: {}", rpc_url);
    println!("🎯 Target TX: {}", target_tx_hash);

    // Setup provider
    let provider = ProviderBuilder::new().connect(rpc_url).await?;
    let provider = Arc::new(provider.root().clone());

    // Fetch target transaction to find its block
    let target_tx_hash: B256 = target_tx_hash.parse().context("Invalid transaction hash")?;
    let target_tx = provider
        .get_transaction_by_hash(target_tx_hash)
        .await?
        .context("Target transaction not found")?;

    let target_block_number = target_tx.block_number.context("Transaction not mined")?;
    let target_tx_index = target_tx
        .transaction_index
        .context("Transaction index not found")?;

    println!(
        "📍 Target TX in block: {} (index: {})",
        target_block_number, target_tx_index
    );

    // Determine starting block
    let start_block = start_from_block.unwrap_or(target_block_number);

    if start_block > target_block_number {
        anyhow::bail!(
            "Start block ({}) is after target transaction block ({})",
            start_block,
            target_block_number
        );
    }

    println!("🚀 Starting from block: {}", start_block);
    println!(
        "📊 Will process {} blocks",
        target_block_number - start_block + 1
    );
    println!();

    // Setup REVM database with state from block BEFORE our start block
    let db: Arc<dyn Source> = Arc::new(RpcDatabase::new(provider.clone(), start_block - 1));
    let cache = Arc::new(Cache::new(vec![db], 10));
    let mut state: OverlayDb<sidecar::cache::Cache> = OverlayDb::new(Some(cache.clone()), 100_000);

    let mut total_txs_executed = 0;
    let mut fork_db = state.fork();

    // Process all blocks from start_block to target_block_number
    for block_num in start_block..=target_block_number {
        println!("╔══════════════════════════════════════════════════╗");
        println!(
            "║  Processing Block: {:>10}                     ║",
            block_num
        );
        println!("╚══════════════════════════════════════════════════╝");

        // Fetch block with full transactions
        let block = provider
            .get_block_by_number(block_num.into())
            .full()
            .await?
            .context("Block not found")?;

        // Build block environment
        let mut block_env = BlockEnv::default();
        block_env.number = block_num;
        block_env.timestamp = block.header.timestamp;
        block_env.gas_limit = block.header.gas_limit;
        block_env.difficulty = block.header.difficulty;
        if let Some(base_fee) = block.header.base_fee_per_gas {
            block_env.basefee = base_fee;
        }
        block_env.prevrandao = Some(block.header.mix_hash);
        block_env.blob_excess_gas_and_price = Some(BlobExcessGasAndPrice {
            excess_blob_gas: 0,
            blob_gasprice: 1,
        });

        println!("⏰ Timestamp: {}", block.header.timestamp);
        println!("⛽ Gas Limit: {}", block.header.gas_limit);

        // Get transactions
        let transactions = match &block.transactions {
            alloy::rpc::types::BlockTransactions::Full(txs) => txs,
            _ => anyhow::bail!("Block transactions not in full format"),
        };

        println!("📦 Total TXs in block: {}", transactions.len());
        println!();

        // Determine how many transactions to process in this block
        let tx_limit = if block_num == target_block_number {
            target_tx_index as usize // Stop before target tx
        } else {
            transactions.len() // Process all txs in this block
        };

        fork_db = state.fork();
        // Execute transactions in this block
        for (i, tx) in transactions.iter().enumerate() {
            if i >= tx_limit {
                break;
            }

            let is_target = block_num == target_block_number && i == target_tx_index as usize;

            if is_target {
                println!("\n🎯🎯🎯 TARGET TRANSACTION REACHED 🎯🎯🎯");
                println!("Block: {}, Index: {}", block_num, i);
                println!("Hash: {}", tx.tx_hash());
                break; // Stop before executing the target tx
            }

            print!("   TX {}/{}: {} ", i, transactions.len() - 1, tx.tx_hash());

            let tx_env = to_revm_tx_env(&tx).expect("Failed to build transaction environment");

            let env = evm_env(59144, SpecId::PRAGUE, block_env.clone());
            let mut no_op_inspector = NoOpInspector {};
            let mut evm = build_evm_by_features!(&mut fork_db, &env, &mut no_op_inspector);
            let tx_env = wrap_tx_env_for_optimism!(tx_env);

            let result_and_state = evm
                .inspect_with_tx(tx_env)
                .context("Failed to execute transaction")?;

            // Commit the state changes
            fork_db.commit(result_and_state.state);
            total_txs_executed += 1;

            match result_and_state.result {
                ExecutionResult::Success { gas_used, .. } => {
                    println!("✅ (gas: {})", gas_used);
                }
                ExecutionResult::Revert { gas_used, .. } => {
                    println!("❌ Reverted (gas: {})", gas_used);
                }
                ExecutionResult::Halt { gas_used, reason } => {
                    println!("⚠️  Halted (gas: {}, reason: {:?})", gas_used, reason);
                }
            }
        }

        if block_num != target_block_number {
            state.commit_overlay_fork_db(fork_db.clone());
        }

        println!();
    }

    println!("╔══════════════════════════════════════════════════╗");
    println!("║     State Built - Ready for Target TX           ║");
    println!("╚══════════════════════════════════════════════════╝");
    println!("✅ Total transactions executed: {}", total_txs_executed);
    println!("🎯 Now executing target transaction...");
    println!();

    // Now execute the target transaction with CallTracer
    let tx_env =
        to_revm_tx_env(&target_tx).expect("Failed to build target transaction environment");

    // Get the block environment for the target block
    let target_block = provider
        .get_block_by_number(target_block_number.into())
        .await?
        .context("Target block not found")?;

    let mut block_env = BlockEnv::default();
    block_env.number = target_block_number;
    block_env.timestamp = target_block.header.timestamp;
    block_env.gas_limit = target_block.header.gas_limit;
    block_env.difficulty = target_block.header.difficulty;
    if let Some(base_fee) = target_block.header.base_fee_per_gas {
        block_env.basefee = base_fee;
    }
    block_env.prevrandao = Some(target_block.header.mix_hash);
    block_env.blob_excess_gas_and_price = Some(BlobExcessGasAndPrice {
        excess_blob_gas: 0,
        blob_gasprice: 1,
    });

    println!("📊 Gas limit: {}", target_tx.gas_limit());
    println!();

    let mut call_tracer = CallTracer::default();
    let env = evm_env(59144, SpecId::PRAGUE, block_env.clone());

    let mut evm = build_evm_by_features!(&mut fork_db, &env, &mut call_tracer);
    let tx_env = wrap_tx_env_for_optimism!(tx_env);

    let result_and_state = evm
        .inspect_with_tx(tx_env)
        .context("Failed to execute target transaction")?;

    println!("╔═══════════════════════════════════════════════════╗");
    println!("║       Target Transaction Replay Results          ║");
    println!("╚═══════════════════════════════════════════════════╝");
    println!();

    match result_and_state.result {
        ExecutionResult::Success {
            reason,
            gas_used,
            gas_refunded,
            output,
            logs,
            ..
        } => {
            println!("✅ Status: SUCCESS");
            println!("📝 Reason: {:?}", reason);
            println!("⛽ Gas Used: {}", gas_used);
            println!("💰 Gas Refunded: {}", gas_refunded);
            println!("📋 Logs Emitted: {}", logs.len());

            match output {
                Output::Call(data) => {
                    if !data.is_empty() {
                        println!("\n📤 Return Data:");
                        println!("   Length: {} bytes", data.len());
                        println!("   Hex: 0x{}", hex::encode(&data));

                        if data.len() > 64 {
                            println!("   Preview: 0x{}...", hex::encode(&data[..64]));
                        }
                    } else {
                        println!("\n📤 Return Data: (empty)");
                    }
                }
                Output::Create(data, addr) => {
                    println!("\n🏗️  Contract Creation:");
                    if let Some(address) = addr {
                        println!("   📍 Contract Address: {}", address);
                    }
                    println!("   📦 Bytecode Size: {} bytes", data.len());
                }
            }

            if !logs.is_empty() {
                println!("\n📋 Event Logs:");
                for (i, log) in logs.iter().enumerate() {
                    println!("   Log #{}", i);
                    println!("   Address: {}", log.address);
                    println!("   Topics: {}", log.topics().len());
                    for (j, topic) in log.topics().iter().enumerate() {
                        println!("     Topic {}: {}", j, topic);
                    }
                    println!("   Data: {} bytes", log.data.data.len());
                }
            }
        }
        ExecutionResult::Revert { gas_used, output } => {
            println!("❌ Status: REVERTED");
            println!("⛽ Gas Used: {}", gas_used);
            println!("\n🔴 Revert Data:");
            println!("   Length: {} bytes", output.len());
            println!("   Hex: 0x{}", hex::encode(&output));

            if output.len() >= 4 {
                let selector = &output[0..4];
                println!("   Selector: 0x{}", hex::encode(selector));

                if selector == [0x08, 0xc3, 0x79, 0xa0] && output.len() > 4 {
                    println!("   (Standard revert with message)");
                }
            }
        }
        ExecutionResult::Halt { reason, gas_used } => {
            println!("⚠️  Status: HALTED");
            println!("📝 Reason: {:?}", reason);
            println!("⛽ Gas Used: {}", gas_used);
        }
    }

    println!("\n╔═══════════════════════════════════════════════════╗");
    println!("║                 Replay Complete                   ║");
    println!("╚═══════════════════════════════════════════════════╝");

    Ok(())
}

fn to_revm_tx_env(tx: &alloy::rpc::types::Transaction) -> Result<TxEnv, TxEnvBuildError> {
    let mut tx_env = TxEnvBuilder::new();

    tx_env = tx_env.caller(tx.inner.signer());
    tx_env = tx_env.gas_limit(tx.inner.gas_limit());
    tx_env = tx_env.gas_price(tx.inner.gas_price().unwrap_or_default());

    let kind = if let Some(to) = tx.to() {
        revm::primitives::TxKind::Call(to)
    } else {
        revm::primitives::TxKind::Create
    };
    tx_env = tx_env.kind(kind);

    tx_env = tx_env.value(tx.value());
    tx_env = tx_env.data(Bytes::from(tx.input().to_vec()));
    tx_env = tx_env.nonce(tx.nonce());
    tx_env = tx_env.chain_id(tx.chain_id());

    if let Some(access_list) = tx.access_list() {
        tx_env = tx_env.access_list(
            access_list
                .0
                .iter()
                .map(|item| {
                    AccessListItem {
                        address: item.address,
                        storage_keys: item
                            .storage_keys
                            .iter()
                            .map(|key| B256::from(*key))
                            .collect(),
                    }
                })
                .collect::<Vec<_>>()
                .into(),
        );
    }

    match tx.inner.tx_type() {
        TxType::Legacy | TxType::Eip2930 => {}
        TxType::Eip1559 => {
            let max_fee = tx.inner.max_fee_per_gas();
            let max_priority = tx.inner.max_priority_fee_per_gas();

            tx_env = tx_env.gas_price(max_fee);
            tx_env = tx_env.gas_priority_fee(max_priority);
        }
        TxType::Eip4844 => {
            let max_fee = tx.inner.max_fee_per_gas();
            let max_priority = tx.inner.max_priority_fee_per_gas();

            tx_env = tx_env.gas_price(max_fee);
            tx_env = tx_env.gas_priority_fee(max_priority);

            if let Some(blob_versioned_hashes) = tx.blob_versioned_hashes() {
                tx_env = tx_env.blob_hashes(
                    blob_versioned_hashes
                        .iter()
                        .map(|hash| B256::from(*hash))
                        .collect(),
                );
            }

            if let Some(max_fee_per_blob_gas) = tx.max_fee_per_blob_gas() {
                tx_env = tx_env.max_fee_per_blob_gas(max_fee_per_blob_gas);
            }
        }
        TxType::Eip7702 => {
            let max_fee = tx.inner.max_fee_per_gas();
            let max_priority = tx.inner.max_priority_fee_per_gas();

            tx_env = tx_env.gas_price(max_fee);
            tx_env = tx_env.gas_priority_fee(max_priority);

            if let Some(auth_list) = tx.authorization_list() {
                tx_env = tx_env.authorization_list(
                    auth_list
                        .iter()
                        .map(|auth| Either::Left(auth.clone()))
                        .collect(),
                );
            }
        }
    }

    tx_env.build()
}
