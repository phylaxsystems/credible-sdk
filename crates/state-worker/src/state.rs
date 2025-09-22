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
