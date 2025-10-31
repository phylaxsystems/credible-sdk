use alloy::{
    eips::BlockId,
    primitives::{
        Address,
        B256,
        U256,
    },
    providers::ProviderBuilder,
    rpc::types::eth::EIP1186AccountProofResponse,
    transports::{
        RpcError,
        TransportError,
        TransportErrorKind,
    },
};
use alloy_provider::{
    Provider,
    RootProvider,
    ext::DebugApi,
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
use tracing::{
    Span,
    trace,
};

impl DBErrorMarker for JsonRpcDbError {}

/// A `DatabaseRef` implementation that fetches data from an Ethereum node via JSON-RPC
#[derive(Debug)]
pub struct JsonRpcDb {
    provider: Arc<RootProvider>,
    target_block: AtomicU64,
    /// Enables the use of the debug `code_by_hash` RPC method.
    ///
    /// Use of this method means that code will only be queried when
    /// `DatabaseRef` `code_by_hash` is called.
    use_debug_code_by_hash: bool,
}

impl JsonRpcDb {
    pub async fn try_new_with_rpc_url(
        rpc_url: &str,
        use_debug_code_by_hash: bool,
    ) -> Result<Self, JsonRpcDbError> {
        // Create provider (this needs to be done in async context)
        let provider = ProviderBuilder::new()
            .connect(rpc_url)
            .await
            .map_err(JsonRpcDbError::BuildProvider)?;

        Ok(Self {
            provider: Arc::new(provider.root().clone()),
            target_block: AtomicU64::new(0),
            use_debug_code_by_hash,
        })
    }

    pub fn new_with_provider(provider: Arc<RootProvider>, use_debug_code_by_hash: bool) -> Self {
        Self {
            provider,
            target_block: AtomicU64::new(0),
            use_debug_code_by_hash,
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
        let future = async move {
            let proof = provider
                .get_proof(address, vec![])
                .number(target_block)
                .await
                .map_err(|e| JsonRpcDbError::Provider(Box::new(e)))?;

            if proof_indicates_missing_account(&proof) {
                trace!(
                    target = "engine::overlay",
                    overlay_kind = "json_rpc",
                    access = "basic_ref",
                    block = target_block,
                    address = ?address,
                    status = "account_not_found"
                );
                return Ok(None);
            }

            let mut code = None;
            let code_hash = if proof.code_hash == revm::primitives::KECCAK_EMPTY {
                revm::primitives::KECCAK_EMPTY
            } else {
                // If we have `use_debug_code_by_hash` disabled, we have to
                // querry the code here
                //
                // Otherwise, if enabled, the code will remain `None` and
                // we will querry it in `code_by_hash_ref`
                if !self.use_debug_code_by_hash {
                    // If we have a code hash, we query the bytecode
                    let code_bytes = provider
                        .get_code_at(address)
                        .number(target_block)
                        .await
                        .map_err(|e| JsonRpcDbError::Provider(Box::new(e)))?;

                    // If the code is not empty we create a `Bytecode` from it
                    if !code_bytes.is_empty() {
                        code = Some(Bytecode::new_raw(code_bytes));
                    }
                }

                proof.code_hash
            };

            let account_info = AccountInfo {
                balance: proof.balance,
                nonce: proof.nonce,
                code_hash,
                code,
            };

            trace!(
                target = "engine::overlay",
                overlay_kind = "json_rpc",
                access = "basic_ref",
                block = target_block,
                address = ?address,
                value = ?account_info
            );

            Ok(Some(account_info))
        };
        let handle = tokio::runtime::Handle::current();
        let span = Span::current();
        std::thread::scope(|s| {
            s.spawn(move || span.in_scope(|| handle.block_on(future)))
                .join()
                .map_err(|_| JsonRpcDbError::Runtime)?
        })
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        if !self.use_debug_code_by_hash {
            // If not in cache, we can't retrieve it via standard JSON-RPC
            // This should not happen if basic_ref is always called before code_by_hash_ref
            // NOTE: Since this will be part of the OverlayDB, it is guaranteed to be in the cache of the OverlayDB
            trace!(
                target = "engine::overlay",
                overlay_kind = "json_rpc",
                access = "code_by_hash_ref",
                status = "not_supported"
            );
            return Err(JsonRpcDbError::CodeByHashNotFound);
        }

        let future = async move {
            let bytecode = self
                .provider
                .debug_code_by_hash(
                    code_hash,
                    Some(BlockId::Number(alloy::eips::BlockNumberOrTag::Number(
                        self.target_block(),
                    ))),
                )
                .await
                .map_err(|e| JsonRpcDbError::Provider(Box::new(e)))?;
            if let Some(bytecode) = bytecode {
                return Ok(Bytecode::new_raw(bytecode));
            }

            Ok(Bytecode::new())
        };

        let handle = tokio::runtime::Handle::current();
        let span = Span::current();
        std::thread::scope(|s| {
            s.spawn(move || span.in_scope(|| handle.block_on(future)))
                .join()
                .map_err(|_| JsonRpcDbError::Runtime)?
        })
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let provider = self.provider.clone();
        let target_block = self.target_block();

        let future = async move {
            let value = provider
                .get_storage_at(address, index)
                .block_id(BlockId::number(target_block))
                .await
                .map_err(|e| JsonRpcDbError::Provider(Box::new(e)))?;

            trace!(
                target = "engine::overlay",
                overlay_kind = "json_rpc",
                access = "storage_ref",
                block = target_block,
                address = ?address,
                index = %format_args!("{:#x}", index),
                value = %format_args!("{:#x}", value)
            );

            Ok(value)
        };
        let handle = tokio::runtime::Handle::current();
        let span = Span::current();
        std::thread::scope(|s| {
            s.spawn(move || span.in_scope(|| handle.block_on(future)))
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

            trace!(
                target = "engine::overlay",
                overlay_kind = "json_rpc",
                access = "block_hash_ref",
                block_number = block_number,
                block_hash = ?block.header.hash
            );

            Ok(block.header.hash)
        };
        let handle = tokio::runtime::Handle::current();
        let span = Span::current();
        std::thread::scope(|s| {
            s.spawn(move || span.in_scope(|| handle.block_on(future)))
                .join()
                .map_err(|_| JsonRpcDbError::Runtime)?
        })
    }
}

/// Detects whether a proof corresponds to an account that does not exist on-chain.
fn proof_indicates_missing_account(proof: &EIP1186AccountProofResponse) -> bool {
    let code_hash_is_empty =
        proof.code_hash == B256::ZERO || proof.code_hash == revm::primitives::KECCAK_EMPTY;
    let storage_root_is_empty = proof.storage_hash == B256::ZERO;

    code_hash_is_empty && storage_root_is_empty && proof.balance.is_zero() && proof.nonce == 0
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
