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
use parking_lot::RwLock;
use revm::{
    DatabaseRef,
    database::DBErrorMarker,
    state::{
        AccountInfo,
        Bytecode,
    },
};
use std::sync::Arc;
use tokio::runtime::Handle;
use tracing::Span;

impl DBErrorMarker for JsonRpcDbError {}

/// A `DatabaseRef` implementation that fetches data from an Ethereum node via JSON-RPC
#[derive(Debug, Clone)]
pub struct JsonRpcDb {
    provider: Arc<RootProvider>,
    target_block: Arc<RwLock<U256>>,
    tokio_handle: Handle,
}

impl JsonRpcDb {
    pub async fn try_new_with_rpc_url(rpc_url: &str) -> Result<Self, JsonRpcDbError> {
        let provider = ProviderBuilder::new()
            .connect(rpc_url)
            .await
            .map_err(JsonRpcDbError::BuildProvider)?;

        Ok(Self {
            provider: Arc::new(provider.root().clone()),
            target_block: Arc::new(RwLock::new(U256::ZERO)),
            tokio_handle: Handle::current(),
        })
    }

    pub fn new_with_provider(provider: Arc<RootProvider>) -> Self {
        Self {
            provider,
            target_block: Arc::new(RwLock::new(U256::ZERO)),
            tokio_handle: Handle::current(),
        }
    }

    /// Creates a new instance with an explicit Tokio handle.
    /// Use this when constructing from a non-async context.
    pub fn new_with_provider_and_handle(provider: Arc<RootProvider>, tokio_handle: Handle) -> Self {
        Self {
            provider,
            target_block: Arc::new(RwLock::new(U256::ZERO)),
            tokio_handle,
        }
    }

    pub fn set_target_block(&self, block_number: U256) {
        *self.target_block.write() = block_number;
    }

    pub fn target_block(&self) -> U256 {
        *self.target_block.read()
    }

    /// Helper to convert U256 to u64 for RPC calls.
    #[inline]
    fn u256_to_u64(value: U256) -> Result<u64, JsonRpcDbError> {
        value
            .try_into()
            .map_err(|_| JsonRpcDbError::BlockNumberOverflow(value))
    }

    /// Executes an async future on the Tokio runtime from any thread.
    /// This is safe to call from the dedicated engine thread.
    fn block_on<F, T>(&self, future: F) -> Result<T, JsonRpcDbError>
    where
        F: Future<Output = Result<T, JsonRpcDbError>> + Send,
        T: Send,
    {
        let span = Span::current();
        // Use thread::scope to ensure the spawned thread doesn't outlive the future
        std::thread::scope(|s| {
            s.spawn(|| span.in_scope(|| self.tokio_handle.block_on(future)))
                .join()
                .map_err(|_| JsonRpcDbError::Runtime)?
        })
    }
}

impl DatabaseRef for JsonRpcDb {
    type Error = JsonRpcDbError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let provider = self.provider.clone();
        let target_block = Self::u256_to_u64(self.target_block())?;

        self.block_on(async move {
            // Get balance, nonce, and code in parallel for efficiency
            let (balance_result, nonce_result, code_result) = tokio::join!(
                provider.get_balance(address).number(target_block),
                provider.get_transaction_count(address).number(target_block),
                provider.get_code_at(address).number(target_block)
            );

            let balance = balance_result.map_err(|e| JsonRpcDbError::Provider(Box::new(e)))?;
            let nonce = nonce_result.map_err(|e| JsonRpcDbError::Provider(Box::new(e)))?;
            let code = code_result.map_err(|e| JsonRpcDbError::Provider(Box::new(e)))?;

            // JSON-RPC returns zeros for non-existent accounts; treat them as None ie not found.
            if balance.is_zero() && nonce == 0 && code.is_empty() {
                return Ok(None);
            }

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
                    let bytecode = Bytecode::new_raw(code);
                    Some(bytecode)
                },
            };

            Ok(Some(account_info))
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
        let target_block = Self::u256_to_u64(self.target_block())?;

        self.block_on(async move {
            let value = provider
                .get_storage_at(address, index)
                .block_id(BlockId::number(target_block))
                .await
                .map_err(|e| JsonRpcDbError::Provider(Box::new(e)))?;

            Ok(value)
        })
    }

    fn block_hash_ref(&self, block_number: u64) -> Result<B256, Self::Error> {
        let provider = self.provider.clone();

        self.block_on(async move {
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
    Runtime,
    #[error("block number {0} overflows u64")]
    BlockNumberOverflow(U256),
}

impl From<JsonRpcDbError> for super::SourceError {
    fn from(value: JsonRpcDbError) -> Self {
        match value {
            JsonRpcDbError::Provider(e) => super::SourceError::Request(e),
            JsonRpcDbError::BuildProvider(e) => super::SourceError::Other(e.to_string()),
            JsonRpcDbError::BlockNotFound => super::SourceError::BlockNotFound,
            JsonRpcDbError::CodeByHashNotFound => super::SourceError::CodeByHashNotFound,
            JsonRpcDbError::Runtime => super::SourceError::Other("Runtime error".to_string()),
            JsonRpcDbError::BlockNumberOverflow(_) => {
                super::SourceError::Other("Block number overflow".to_string())
            }
        }
    }
}
