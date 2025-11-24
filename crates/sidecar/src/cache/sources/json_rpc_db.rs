use alloy::{
    eips::BlockId,
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
use std::sync::{
    Arc,
    atomic::{
        AtomicU64,
        Ordering,
    },
};

impl DBErrorMarker for JsonRpcDbError {}

/// A `DatabaseRef` implementation that fetches data from an Ethereum node via JSON-RPC
#[derive(Debug)]
pub struct JsonRpcDb {
    provider: Arc<RootProvider>,
    target_block: AtomicU64,
    handle: tokio::runtime::Handle,
}

impl JsonRpcDb {
    pub async fn try_new_with_rpc_url(rpc_url: &str) -> Result<Self, JsonRpcDbError> {
        // Create provider (this needs to be done in async context)
        let provider = ProviderBuilder::new()
            .connect(rpc_url)
            .await
            .map_err(JsonRpcDbError::BuildProvider)?;

        Ok(Self {
            provider: Arc::new(provider.root().clone()),
            target_block: AtomicU64::new(0),
            handle: tokio::runtime::Handle::current(),
        })
    }

    pub fn new_with_provider(provider: Arc<RootProvider>) -> Self {
        Self {
            provider,
            target_block: AtomicU64::new(0),
            handle: tokio::runtime::Handle::current(),
        }
    }

    pub fn set_target_block(&self, block_number: u64) {
        self.target_block.store(block_number, Ordering::Relaxed);
    }

    fn target_block(&self) -> u64 {
        self.target_block.load(Ordering::Acquire)
    }
}

impl DatabaseRef for JsonRpcDb {
    type Error = JsonRpcDbError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let provider = self.provider.clone();
        let target_block = self.target_block();
        let handle = self.handle.clone();
        let span = tracing::Span::current();

        std::thread::scope(|s| {
            s.spawn(move || {
                span.in_scope(|| {
                    handle.block_on(async move {
                        let (balance_result, nonce_result, code_result) = tokio::join!(
                            provider.get_balance(address).number(target_block),
                            provider.get_transaction_count(address).number(target_block),
                            provider.get_code_at(address).number(target_block)
                        );

                        let balance =
                            balance_result.map_err(|e| JsonRpcDbError::Provider(Box::new(e)))?;
                        let nonce =
                            nonce_result.map_err(|e| JsonRpcDbError::Provider(Box::new(e)))?;
                        let code =
                            code_result.map_err(|e| JsonRpcDbError::Provider(Box::new(e)))?;

                        let code_hash = if code.is_empty() {
                            revm::primitives::KECCAK_EMPTY
                        } else {
                            revm::primitives::keccak256(&code)
                        };

                        let account_info = AccountInfo {
                            balance,
                            nonce,
                            code_hash,
                            code: if code.is_empty() {
                                None
                            } else {
                                Some(Bytecode::new_raw(code))
                            },
                        };

                        Ok(Some(account_info))
                    })
                })
            })
            .join()
            .map_err(|_| JsonRpcDbError::Runtime)?
        })
    }

    fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
        Err(JsonRpcDbError::CodeByHashNotFound)
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let provider = self.provider.clone();
        let target_block = self.target_block();
        let handle = self.handle.clone();
        let span = tracing::Span::current();

        std::thread::scope(|s| {
            s.spawn(move || {
                span.in_scope(|| {
                    handle.block_on(async move {
                        provider
                            .get_storage_at(address, index)
                            .block_id(BlockId::number(target_block))
                            .await
                            .map_err(|e| JsonRpcDbError::Provider(Box::new(e)))
                    })
                })
            })
            .join()
            .map_err(|_| JsonRpcDbError::Runtime)?
        })
    }

    fn block_hash_ref(&self, block_number: u64) -> Result<B256, Self::Error> {
        let provider = self.provider.clone();
        let handle = self.handle.clone();
        let span = tracing::Span::current();

        std::thread::scope(|s| {
            s.spawn(move || {
                span.in_scope(|| {
                    handle.block_on(async move {
                        let block = provider
                            .get_block_by_number(block_number.into())
                            .await
                            .map_err(|e| JsonRpcDbError::Provider(Box::new(e)))?
                            .ok_or(JsonRpcDbError::BlockNotFound)?;

                        Ok(block.header.hash)
                    })
                })
            })
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
