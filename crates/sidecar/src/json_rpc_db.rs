use alloy::{
    primitives::{
        Address,
        B256,
        U256,
    },
    providers::ProviderBuilder,
    transports::{
        RpcError,
        TransportErrorKind,
    },
};
use alloy_provider::{
    Provider,
    RootProvider,
};
use dashmap::DashMap;
use revm::{
    DatabaseRef,
    database::DBErrorMarker,
    state::{
        AccountInfo,
        Bytecode,
    },
};
use std::sync::Arc;

impl DBErrorMarker for JsonRpcDbError {}

/// A DatabaseRef implementation that fetches data from an Ethereum node via JSON-RPC
pub struct JsonRpcDb {
    provider: Arc<RootProvider>,
    runtime: tokio::runtime::Handle,
    code_cache: DashMap<B256, Bytecode>,
}

impl JsonRpcDb {
    pub async fn try_new(rpc_url: &str) -> Result<Arc<Self>, JsonRpcDbError> {
        // Try to get current runtime handle, or create a new one
        let runtime = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle
        } else {
            // We're not in an async context, so we need to create a minimal runtime
            // Use a single-threaded runtime for efficiency
            let rt = tokio::runtime::Runtime::new().map_err(JsonRpcDbError::Runtime)?;
            rt.handle().clone()
        };

        // Create provider (this needs to be done in async context)
        let provider = ProviderBuilder::new()
            .connect(rpc_url)
            .await
            .map_err(JsonRpcDbError::BuildProvider)?;

        Ok(Arc::new(Self {
            provider: Arc::new(provider.root().clone()),
            runtime,
            code_cache: DashMap::default(),
        }))
    }
}

impl DatabaseRef for JsonRpcDb {
    type Error = JsonRpcDbError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let provider = self.provider.clone();

        self.runtime.block_on(async move {
            // Get balance, nonce, and code in parallel for efficiency
            let (balance_result, nonce_result, code_result) = tokio::join!(
                provider.get_balance(address),
                provider.get_transaction_count(address),
                provider.get_code_at(address)
            );

            let balance = balance_result.map_err(|e| JsonRpcDbError::Provider(Box::new(e)))?;
            let nonce = nonce_result.map_err(|e| JsonRpcDbError::Provider(Box::new(e)))?;
            let code = code_result.map_err(|e| JsonRpcDbError::Provider(Box::new(e)))?;

            let code_hash = if code.is_empty() {
                revm::primitives::KECCAK_EMPTY
            } else {
                revm::primitives::keccak256(&code)
            };

            Ok(Some(AccountInfo {
                balance,
                nonce,
                code_hash,
                code: if code.is_empty() {
                    None
                } else {
                    let bytecode = Bytecode::new_raw(code);
                    self.code_cache.insert(code_hash, bytecode.clone());
                    Some(bytecode)
                },
            }))
        })
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        if code_hash == revm::primitives::KECCAK_EMPTY {
            return Ok(Bytecode::default());
        }

        if let Some(bytecode) = self.code_cache.get(&code_hash) {
            return Ok(bytecode.clone());
        }

        // If not in cache, we can't retrieve it via standard JSON-RPC
        // This should not happen if basic_ref is always called before code_by_hash_ref
        Err(JsonRpcDbError::CodeByHashNotFound)
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let provider = self.provider.clone();

        self.runtime.block_on(async move {
            let value = provider
                .get_storage_at(address, index)
                .await
                .map_err(|e| JsonRpcDbError::Provider(Box::new(e)))?;

            Ok(value)
        })
    }

    fn block_hash_ref(&self, block_number: u64) -> Result<B256, Self::Error> {
        let provider = self.provider.clone();

        self.runtime.block_on(async move {
            let block = provider
                .get_block_by_number(block_number.into())
                .await
                .map_err(|e| JsonRpcDbError::Provider(Box::new(e)))?
                .ok_or(JsonRpcDbError::BlockNotFound)?;

            Ok(block.header.hash)
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum JsonRpcDbError {
    #[error("Provider error during RPC call")]
    Provider(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("Failed to build the RPC provider")]
    BuildProvider(#[source] RpcError<TransportErrorKind>),
    #[error("Block not found")]
    BlockNotFound,
    #[error("Code by hash not found")]
    CodeByHashNotFound,
    #[error("Runtime error")]
    Runtime(#[source] std::io::Error),
}
