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
#[derive(Debug)]
pub struct JsonRpcDb {
    provider: Arc<RootProvider>,
}

impl JsonRpcDb {
    pub async fn try_new(rpc_url: &str) -> Result<Self, JsonRpcDbError> {
        // Create provider (this needs to be done in async context)
        let provider = ProviderBuilder::new()
            .connect(rpc_url)
            .await
            .map_err(JsonRpcDbError::BuildProvider)?;

        Ok(Self {
            provider: Arc::new(provider.root().clone()),
        })
    }
}

impl DatabaseRef for JsonRpcDb {
    type Error = JsonRpcDbError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let provider = self.provider.clone();
        let future = async move {
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
                    Some(bytecode)
                },
            }))
        };
        let handle = tokio::runtime::Handle::current();
        std::thread::scope(|s| {
            s.spawn(|| handle.block_on(future))
                .join()
                .map_err(|_| JsonRpcDbError::Runtime)?
        })
    }

    fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
        // If not in cache, we can't retrieve it via standard JSON-RPC
        // This should not happen if basic_ref is always called before code_by_hash_ref
        // NOTE: Since this will be part of the OverlayDB, it is guaranteed to be in the cache of the OverlayDB
        Err(JsonRpcDbError::CodeByHashNotFound)
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let provider = self.provider.clone();

        let future = async move {
            let value = provider
                .get_storage_at(address, index)
                .await
                .map_err(|e| JsonRpcDbError::Provider(Box::new(e)))?;

            Ok(value)
        };
        let handle = tokio::runtime::Handle::current();
        std::thread::scope(|s| {
            s.spawn(|| handle.block_on(future))
                .join()
                .map_err(|_| JsonRpcDbError::Runtime)?
        })
    }

    fn block_hash_ref(&self, block_number: u64) -> Result<B256, Self::Error> {
        let provider = self.provider.clone();

        let future = async move {
            let block = provider
                .get_block_by_number(block_number.into())
                .await
                .map_err(|e| JsonRpcDbError::Provider(Box::new(e)))?
                .ok_or(JsonRpcDbError::BlockNotFound)?;

            Ok(block.header.hash)
        };
        let handle = tokio::runtime::Handle::current();
        std::thread::scope(|s| {
            s.spawn(|| handle.block_on(future))
                .join()
                .map_err(|_| JsonRpcDbError::Runtime)?
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
    Runtime,
}

impl From<JsonRpcDbError> for super::SourceError {
    fn from(value: JsonRpcDbError) -> Self {
        match value {
            JsonRpcDbError::Provider(e) => super::SourceError::Request(e),
            JsonRpcDbError::BuildProvider(e) => super::SourceError::Other(e.to_string()),
            JsonRpcDbError::BlockNotFound => super::SourceError::BlockNotFound,
            JsonRpcDbError::CodeByHashNotFound => super::SourceError::CodeByHashNotFound,
            JsonRpcDbError::Runtime => super::SourceError::Other("Runtime error".to_string()),
        }
    }
}
