//! Helpers that collapse per-transaction debug traces into per-account commits.
//!
//! The worker consumes Geth's `prestateTracer` output, merges the resulting
//! diffs into account-level snapshots, and exposes a compact representation that
//! mirrors the Redis schema the sidecar expects.

use std::{
    collections::{
        HashMap,
        HashSet,
    },
    time::Duration,
};

use alloy::primitives::{
    Address,
    B256,
    U256,
    keccak256,
};
use alloy_rpc_types_trace::geth::{
    GethDebugBuiltInTracerType,
    GethDebugTracingOptions,
    GethTrace,
    PreStateConfig,
    PreStateFrame,
    TraceResult,
    pre_state::{
        AccountState,
        DiffMode,
    },
};
use revm::primitives::KECCAK_EMPTY;
use tracing::warn;

#[derive(Default)]
struct AccountSnapshot {
    /// Most recent balance observed in the trace stream; filled lazily.
    balance: Option<U256>,
    /// Most recent nonce observed in the trace stream; filled lazily.
    nonce: Option<u64>,
    /// Optional bytecode blob; preserved so we can emit code updates exactly once.
    code: Option<Vec<u8>>,
    /// Sparse storage slots touched by the block (slot -> value).
    storage_updates: HashMap<B256, B256>,
    /// Whether we saw a post-state for this account.
    touched: bool,
    /// Whether the account was deleted by the block.
    deleted: bool,
}

/// Aggregate of all state changes produced while processing a block.
pub struct BlockStateUpdate {
    pub block_number: u64,
    pub block_hash: B256,
    pub accounts: Vec<AccountCommit>,
}

/// Flattened representation of an account diff that mirrors the Redis schema.
pub struct AccountCommit {
    pub address: Address,
    pub balance: U256,
    pub nonce: u64,
    pub code_hash: B256,
    pub code: Option<Vec<u8>>,
    pub storage: Vec<(B256, B256)>,
    pub deleted: bool,
}

impl BlockStateUpdate {
    /// Collapse the per-transaction pre-state traces into per-account updates.
    pub fn from_traces(block_number: u64, block_hash: B256, traces: Vec<TraceResult>) -> Self {
        let mut accounts: HashMap<Address, AccountSnapshot> = HashMap::new();

        for trace in traces {
            match trace {
                TraceResult::Success { result, tx_hash } => {
                    match result {
                        GethTrace::PreStateTracer(frame) => {
                            match frame {
                                PreStateFrame::Diff(diff) => process_diff(&mut accounts, diff),
                                other => {
                                    warn!(?tx_hash, ?other, "unexpected prestate frame variant")
                                }
                            }
                        }
                        other => warn!(?tx_hash, ?other, "unexpected tracer response"),
                    }
                }
                TraceResult::Error { error, tx_hash } => {
                    warn!(?tx_hash, error, "trace error for transaction");
                }
            }
        }

        let accounts = accounts
            .into_iter()
            .filter_map(|(address, snapshot)| snapshot.finalize(address))
            .collect();

        Self {
            block_number,
            block_hash,
            accounts,
        }
    }

    /// Consume the update and return its constituent parts for downstream
    /// writers. This keeps the write path ergonomic without cloning vectors.
    pub fn into_parts(self) -> (u64, B256, Vec<AccountCommit>) {
        (self.block_number, self.block_hash, self.accounts)
    }
}

impl AccountSnapshot {
    /// Prime the snapshot with the best-known "pre" values so we always emit a
    /// full record even if the account later disappears from the post state.
    fn ensure_initialized(&mut self, state: &AccountState) {
        if self.balance.is_none() {
            self.balance = Some(state.balance.unwrap_or_default());
        }
        if self.nonce.is_none() {
            self.nonce = Some(state.nonce.unwrap_or_default());
        }
        if self.code.is_none() {
            let code = state
                .code
                .as_ref()
                .map(|bytes| Vec::<u8>::from(bytes.clone()))
                .unwrap_or_default();
            self.code = Some(code);
        }
    }

    /// Apply the post-state diff for updates or creations.
    fn apply_post(&mut self, state: &AccountState) {
        self.touched = true;
        self.deleted = false;
        if let Some(balance) = state.balance {
            self.balance = Some(balance);
        }
        if let Some(nonce) = state.nonce {
            self.nonce = Some(nonce);
        }
        if let Some(code) = &state.code {
            self.code = Some(Vec::<u8>::from(code.clone()));
        }
        for (slot, value) in &state.storage {
            self.storage_updates.insert(*slot, *value);
        }
    }

    /// Apply tombstone semantics for deletions while keeping prior values so we
    /// can emit zeroed storage/code when the account is removed.
    fn mark_deleted(&mut self, state: &AccountState) {
        self.touched = true;
        self.deleted = true;
        self.ensure_initialized(state);
        for slot in state.storage.keys() {
            self.storage_updates.insert(*slot, B256::ZERO);
        }
        for value in self.storage_updates.values_mut() {
            *value = B256::ZERO;
        }
    }

    /// Convert the accumulated snapshot into a commit payload if anything
    /// meaningful changed. Returning `None` allows callsites to drop untouched
    /// snapshots without extra bookkeeping.
    fn finalize(mut self, address: Address) -> Option<AccountCommit> {
        if !self.touched && self.storage_updates.is_empty() {
            return None;
        }

        let mut code_bytes = self.code.take().unwrap_or_default();
        let (balance, nonce) = if self.deleted {
            code_bytes.clear();
            (U256::ZERO, 0_u64)
        } else {
            (
                self.balance.unwrap_or_default(),
                self.nonce.unwrap_or_default(),
            )
        };

        let code_hash = if code_bytes.is_empty() {
            KECCAK_EMPTY
        } else {
            keccak256(&code_bytes)
        };

        let code = if self.deleted || code_bytes.is_empty() {
            None
        } else {
            Some(code_bytes)
        };

        let storage = self.storage_updates.into_iter().collect();

        Some(AccountCommit {
            address,
            balance,
            nonce,
            code_hash,
            code,
            storage,
            deleted: self.deleted,
        })
    }
}

/// Merge a single transaction diff into the pending account snapshots.
fn process_diff(accounts: &mut HashMap<Address, AccountSnapshot>, diff: DiffMode) {
    let DiffMode { pre, post } = diff;
    let post_addresses: HashSet<Address> = post.keys().copied().collect();

    for (address, state) in pre.iter() {
        accounts
            .entry(*address)
            .or_default()
            .ensure_initialized(state);
    }

    for (address, state) in post.iter() {
        accounts.entry(*address).or_default().apply_post(state);
    }

    // Any address that had only a `pre` entry but no `post` entry was deleted
    // during the block. Mark them as tombstones so we zero out Redis.
    for (address, state) in pre.into_iter() {
        if !post_addresses.contains(&address) {
            accounts.entry(address).or_default().mark_deleted(&state);
        }
    }
}

/// Build the canonical tracer configuration we expect from the execution node.
/// We enable diff mode so pre/post states arrive side-by-side and set a timeout
/// to avoid hanging the worker if the client is overloaded.
pub fn build_trace_options(timeout: Duration) -> GethDebugTracingOptions {
    let config = PreStateConfig {
        diff_mode: Some(true),
        disable_code: Some(false),
        disable_storage: Some(false),
    };
    GethDebugTracingOptions::new_tracer(GethDebugBuiltInTracerType::PreStateTracer)
        .with_prestate_config(config)
        .with_timeout(timeout)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::{
        network::{
            EthereumWallet,
            TransactionBuilder,
        },
        node_bindings::Anvil,
        primitives::{
            Address,
            Bytes,
            TxKind,
            U256,
            keccak256,
        },
        providers::ProviderBuilder,
        rpc::types::TransactionRequest,
        signers::local::LocalSigner,
    };
    use alloy_provider::{
        Provider,
        WsConnect,
    };
    use anyhow::Result;
    use revm::primitives::KECCAK_EMPTY;
    use std::collections::{
        BTreeMap,
        HashMap,
    };

    fn collect_accounts(accounts: Vec<AccountCommit>) -> HashMap<Address, AccountCommit> {
        accounts
            .into_iter()
            .map(|account| (account.address, account))
            .collect()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prestate_tracer_captures_transfer_diffs() -> Result<()> {
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new()
            .connect_ws(WsConnect::new(anvil.ws_endpoint()))
            .await?;
        let sender_key = anvil.keys()[0].clone();
        let sender = anvil.addresses()[0];
        let sender_wallet = EthereumWallet::from(LocalSigner::from(sender_key));
        let recipient = Address::repeat_byte(0x23);

        let initial_sender_balance = provider.get_balance(sender).await?;
        let chain_id = provider.get_chain_id().await?;
        let nonce = provider.get_transaction_count(sender).await?;
        let gas_price = provider.get_gas_price().await?;
        let transfer_value = U256::from(1_000_000_000u64); // 1 gwei
        let gas_limit = U256::from(21_000u64);

        let tx = TransactionRequest {
            from: Some(sender),
            to: Some(TxKind::Call(recipient)),
            value: Some(transfer_value),
            chain_id: Some(chain_id),
            nonce: Some(nonce),
            gas: Some(gas_limit.try_into().expect("gas fits in u128")),
            max_fee_per_gas: Some(gas_price),
            max_priority_fee_per_gas: Some(gas_price),
            gas_price: Some(gas_price),
            ..Default::default()
        };

        let signed = tx.build(&sender_wallet).await?;
        let pending = provider.send_tx_envelope(signed).await?;
        let tx_hash = *pending.tx_hash();
        let receipt = pending.get_receipt().await?;

        let block_number = receipt
            .block_number
            .expect("block number must be present for mined tx");
        let block_hash = receipt
            .block_hash
            .expect("block hash must be present for mined tx");

        let sender_balance = provider.get_balance(sender).await?;
        let sender_nonce = provider.get_transaction_count(sender).await?;
        let recipient_balance = provider.get_balance(recipient).await?;
        let recipient_nonce = provider.get_transaction_count(recipient).await?;

        let mut pre = BTreeMap::new();
        pre.insert(
            sender,
            AccountState {
                balance: Some(initial_sender_balance),
                nonce: Some(nonce),
                code: None,
                storage: Default::default(),
            },
        );

        let mut post = BTreeMap::new();
        post.insert(
            sender,
            AccountState {
                balance: Some(sender_balance),
                nonce: Some(sender_nonce),
                code: None,
                storage: Default::default(),
            },
        );
        post.insert(
            recipient,
            AccountState {
                balance: Some(recipient_balance),
                nonce: Some(recipient_nonce),
                code: None,
                storage: Default::default(),
            },
        );

        let diff = DiffMode { pre, post };
        let trace = GethTrace::PreStateTracer(PreStateFrame::Diff(diff));
        let traces = vec![TraceResult::new_success(trace, Some(tx_hash))];
        let update = BlockStateUpdate::from_traces(block_number, block_hash, traces);
        let accounts = collect_accounts(update.accounts);

        let sender_commit = accounts
            .get(&sender)
            .expect("sender account should be present in diff");
        assert_eq!(sender_commit.balance, sender_balance);
        assert_eq!(sender_commit.nonce, sender_nonce);
        assert!(sender_commit.code.is_none());
        assert_eq!(sender_commit.code_hash, KECCAK_EMPTY);
        assert!(sender_commit.storage.is_empty());
        assert!(!sender_commit.deleted);
        assert!(sender_balance < initial_sender_balance);

        let recipient_commit = accounts
            .get(&recipient)
            .expect("recipient account should be present in diff");
        assert_eq!(recipient_commit.balance, recipient_balance);
        assert_eq!(recipient_commit.nonce, recipient_nonce);
        assert!(recipient_commit.code.is_none());
        assert_eq!(recipient_commit.code_hash, KECCAK_EMPTY);
        assert!(recipient_commit.storage.is_empty());
        assert!(!recipient_commit.deleted);
        assert_eq!(recipient_balance, transfer_value);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prestate_tracer_captures_contract_creation_code() -> Result<()> {
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new()
            .connect_ws(WsConnect::new(anvil.ws_endpoint()))
            .await?;
        let deployer = anvil.addresses()[1];
        let initial_deployer_balance = provider.get_balance(deployer).await?;
        let deployer_key = anvil.keys()[1].clone();
        let deployer_wallet = EthereumWallet::from(LocalSigner::from(deployer_key));
        let chain_id = provider.get_chain_id().await?;
        let nonce = provider.get_transaction_count(deployer).await?;
        let gas_price = provider.get_gas_price().await?;
        let gas_limit = U256::from(2_000_000u64);

        // Minimal init code that returns runtime `602a60005260206000f3`.
        let init_code = hex::decode("600a600c600039600a6000f3602a60005260206000f3")?;
        let tx = TransactionRequest {
            from: Some(deployer),
            to: Some(TxKind::Create),
            input: Bytes::from(init_code).into(),
            chain_id: Some(chain_id),
            nonce: Some(nonce),
            gas: Some(gas_limit.try_into().expect("gas fits in u128")),
            max_fee_per_gas: Some(gas_price),
            max_priority_fee_per_gas: Some(gas_price),
            gas_price: Some(gas_price),
            ..Default::default()
        };

        let signed = tx.build(&deployer_wallet).await?;
        let pending = provider.send_tx_envelope(signed).await?;
        let tx_hash = *pending.tx_hash();
        let receipt = pending.get_receipt().await?;

        let contract_address = receipt
            .contract_address
            .expect("deployment must include contract address");
        let block_number = receipt
            .block_number
            .expect("block number must be present for mined tx");
        let block_hash = receipt
            .block_hash
            .expect("block hash must be present for mined tx");

        let contract_code = provider.get_code_at(contract_address).await?;
        let contract_balance = provider.get_balance(contract_address).await?;
        let contract_nonce = provider.get_transaction_count(contract_address).await?;
        let deployer_balance_after = provider.get_balance(deployer).await?;
        let deployer_nonce_after = provider.get_transaction_count(deployer).await?;

        let mut pre = BTreeMap::new();
        pre.insert(
            deployer,
            AccountState {
                balance: Some(initial_deployer_balance),
                nonce: Some(nonce),
                code: None,
                storage: Default::default(),
            },
        );

        let mut post = BTreeMap::new();
        post.insert(
            deployer,
            AccountState {
                balance: Some(deployer_balance_after),
                nonce: Some(deployer_nonce_after),
                code: None,
                storage: Default::default(),
            },
        );
        post.insert(
            contract_address,
            AccountState {
                balance: Some(contract_balance),
                nonce: Some(contract_nonce),
                code: Some(contract_code.clone()),
                storage: Default::default(),
            },
        );

        let diff = DiffMode { pre, post };
        let trace = GethTrace::PreStateTracer(PreStateFrame::Diff(diff));
        let traces = vec![TraceResult::new_success(trace, Some(tx_hash))];
        let update = BlockStateUpdate::from_traces(block_number, block_hash, traces);
        let accounts = collect_accounts(update.accounts);

        let contract_commit = accounts
            .get(&contract_address)
            .expect("contract account should be present in diff");
        assert_eq!(contract_commit.balance, contract_balance);
        assert_eq!(contract_commit.nonce, contract_nonce);
        assert_eq!(
            contract_commit.code.as_deref(),
            Some(contract_code.as_ref())
        );
        assert_eq!(contract_commit.code_hash, keccak256(contract_code.as_ref()));
        assert!(contract_commit.storage.is_empty());
        assert!(!contract_commit.deleted);

        // The deployer's nonce should also advance in the diff.
        let deployer_commit = accounts
            .get(&deployer)
            .expect("deployer account should appear in diff");
        assert_eq!(deployer_commit.nonce, deployer_nonce_after);

        Ok(())
    }
}
