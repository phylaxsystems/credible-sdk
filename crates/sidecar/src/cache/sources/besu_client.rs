use crate::cache::sources::{
    Source,
    SourceError,
    SourceName,
    json_rpc_db::JsonRpcDb,
};
use alloy::{
    providers::{
        Provider,
        ProviderBuilder,
        WsConnect,
    },
    rpc::types::Header,
    transports::{
        RpcError,
        TransportErrorKind,
    },
};
use alloy_provider::RootProvider;
use assertion_executor::primitives::{
    AccountInfo,
    Address,
    B256,
    Bytecode,
};
use futures::StreamExt;
use revm::{
    DatabaseRef,
    primitives::{
        StorageKey,
        StorageValue,
    },
};
use std::{
    sync::{
        Arc,
        atomic::{
            AtomicU64,
            Ordering,
        },
    },
    time::Duration,
};
use thiserror::Error;
use tokio::task::AbortHandle;
use tracing::{
    error,
    info,
    warn,
};

/// Besu state sync manager using Alloy
#[derive(Debug)]
pub struct BesuClient {
    inner: Arc<BesuClientInner>,
    handler: AbortHandle,
}

impl Drop for BesuClient {
    fn drop(&mut self) {
        self.handler.abort();
        info!("BesuClient subscription cleaned up");
    }
}

#[derive(Debug)]
struct BesuClientInner {
    /// Provider for sync status
    provider: Arc<RootProvider>,
    /// Current block
    current_block: Arc<AtomicU64>,
    /// JsonRpcDb using http for making `DatabaseRef` calls
    json_rpc_db: JsonRpcDb,
}

impl BesuClient {
    /// Create a new `BesuStateSync` instance
    pub async fn try_build(ws_url: impl Into<String>, http_url: impl Into<String>) -> Result<Arc<Self>, BesuClientError> {
        let ws = WsConnect::new(ws_url.into());
        let provider = Arc::new(
            ProviderBuilder::new()
                .connect_ws(ws)
                .await
                .map_err(BesuClientError::Provider)?
                .root()
                .clone(),
        );

        let http_provider = Arc::new(
            ProviderBuilder::new().connect_http(reqwest::Url::parse(&http_url.into())?).root().clone());

        let inner = Arc::new(BesuClientInner {
            current_block: Arc::new(AtomicU64::new(0)),
            json_rpc_db: JsonRpcDb::new_with_provider(http_provider.clone()),
            provider,
        });
        let handler = tokio::task::spawn(inner.clone().run_with_reconnect());

        Ok(Arc::new(Self {
            inner,
            handler: handler.abort_handle(),
        }))
    }
}

impl BesuClientInner {
    const MAX_BACKOFF: Duration = Duration::from_secs(60);

    /// Connect to Besu node and start syncing
    async fn connect_and_sync_head(self: Arc<Self>) -> Result<(), Box<dyn std::error::Error>> {
        // Get the current block to initialize state
        if let Ok(block_number) = self.provider.get_block_number().await {
            self.current_block.store(block_number, Ordering::Relaxed);
            info!("Initial block number: {}", block_number);
        }

        // Subscribe to new heads
        let subscription = self.provider.subscribe_blocks().await?;
        let mut stream = subscription.into_stream();

        // Process incoming header
        while let Some(header) = stream.next().await {
            self.handle_new_header(&header);
        }

        warn!("Subscription ended");

        Ok(())
    }

    /// Handle new block
    fn handle_new_header(&self, header: &Header) {
        let block_number = header.inner.number;
        info!("New block: #{}", block_number);

        // Update state
        self.current_block.store(block_number, Ordering::Relaxed);
    }

    /// Get the current block number
    pub fn get_current_block(&self) -> u64 {
        self.current_block.load(Ordering::Acquire)
    }

    /// Run sync with automatic reconnection
    pub async fn run_with_reconnect(self: Arc<Self>) {
        let mut backoff = Duration::from_secs(0);

        loop {
            match self.clone().connect_and_sync_head().await {
                Ok(()) => {
                    info!("Sync completed normally");
                    // Reset backoff
                    backoff = Duration::from_secs(0);
                }
                Err(e) => {
                    error!(error = ?e, "Sync error, retrying in {:?}", backoff);
                    backoff = ((backoff + Duration::from_secs(1)) * 2).min(Self::MAX_BACKOFF);
                }
            }

            tokio::time::sleep(backoff).await;
            info!("Reconnecting...");
        }
    }
}

impl DatabaseRef for BesuClient {
    type Error = SourceError;
    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.inner
            .json_rpc_db
            .basic_ref(address)
            .map_err(Into::into)
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.inner
            .json_rpc_db
            .code_by_hash_ref(code_hash)
            .map_err(Into::into)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        self.inner
            .json_rpc_db
            .block_hash_ref(number)
            .map_err(Into::into)
    }

    fn storage_ref(
        &self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        self.inner
            .json_rpc_db
            .storage_ref(address, index)
            .map_err(Into::into)
    }
}

impl Source for BesuClient {
    fn name(&self) -> SourceName {
        SourceName::BesuClient
    }

    fn is_synced(&self, current_block_number: u64) -> bool {
        if current_block_number > 0 {
            self.inner.get_current_block() >= current_block_number
        } else {
            false
        }
    }

    fn update_target_block(&self, block_number: u64) {
        self.inner.json_rpc_db.set_target_block(block_number);
    }
}

#[derive(Error, Debug)]
pub enum BesuClientError {
    #[error("Failed to connect to the websocket provider")]
    Provider(#[source] RpcError<TransportErrorKind>),
    #[error("Failed to parse the HTTP provider URL")]
    HttpUrl(#[from] url::ParseError),
}
