use crate::cache::sources::{
    Source,
    SourceError,
    SourceName,
    json_rpc_db::JsonRpcDb,
};
use alloy::{
    primitives::U256,
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
use futures_util::StreamExt;
use parking_lot::RwLock;
use revm::{
    DatabaseRef,
    primitives::{
        StorageKey,
        StorageValue,
    },
};
use std::{
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::task::AbortHandle;
use tracing::{
    debug,
    error,
    info,
    instrument,
    warn,
};

/// Eth RPC source state sync manager using Alloy
#[derive(Debug)]
pub struct EthRpcSource {
    inner: Arc<EthRpcSourceInner>,
    handler: AbortHandle,
}

impl Drop for EthRpcSource {
    fn drop(&mut self) {
        self.handler.abort();
        info!("EthRpcSource subscription cleaned up");
    }
}

#[derive(Debug)]
struct EthRpcSourceInner {
    /// Provider for sync status
    ws_provider: Arc<RootProvider>,
    /// The latest head the underlying node has seen
    latest_head: Arc<RwLock<U256>>,
    /// `JsonRpcDb` using http for making `DatabaseRef` calls
    json_rpc_db: JsonRpcDb,
}

impl EthRpcSource {
    /// Create a new `EthRpcSource` instance
    pub async fn try_build(
        ws_url: impl Into<String>,
        http_url: impl Into<String>,
    ) -> Result<Arc<Self>, EthRpcSourceError> {
        let ws = WsConnect::new(ws_url.into());
        let ws_provider = Arc::new(
            ProviderBuilder::new()
                .connect_ws(ws)
                .await
                .map_err(EthRpcSourceError::Provider)?
                .root()
                .clone(),
        );

        let http_provider = Arc::new(
            ProviderBuilder::new()
                .connect_http(reqwest::Url::parse(&http_url.into())?)
                .root()
                .clone(),
        );

        let inner = Arc::new(EthRpcSourceInner {
            latest_head: Arc::new(RwLock::new(U256::ZERO)),
            json_rpc_db: JsonRpcDb::new_with_provider(http_provider.clone()),
            ws_provider,
        });
        let handler = tokio::task::spawn(inner.clone().run_with_reconnect());

        Ok(Arc::new(Self {
            inner,
            handler: handler.abort_handle(),
        }))
    }

    /// The target block is the minimum of the requested range and the client's latest head.
    /// If the client is synced, the target block is the maximum of the requested range and the
    /// client's latest head.
    fn calculate_target_block(
        min_synced_block: U256,
        latest_head: U256,
        client_latest_head: U256,
    ) -> U256 {
        (client_latest_head.min(latest_head)).max(min_synced_block)
    }
}

impl EthRpcSourceInner {
    const MAX_BACKOFF: Duration = Duration::from_secs(60);

    /// Connect to an Eth RPC node and start syncing
    async fn connect_and_sync_head(self: Arc<Self>) -> Result<(), Box<dyn std::error::Error>> {
        // Get the current block to initialize state
        if let Ok(block_number) = self.ws_provider.get_block_number().await {
            *self.latest_head.write() = U256::from(block_number);
            info!("Initial block number: {}", block_number);
        }

        // Subscribe to new heads
        let subscription = self.ws_provider.subscribe_blocks().await?;
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
        debug!("New block: #{}", block_number);

        // Update state
        *self.latest_head.write() = U256::from(block_number);
    }

    /// Get the latest head
    pub fn get_latest_head(&self) -> U256 {
        *self.latest_head.read()
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

impl DatabaseRef for EthRpcSource {
    type Error = SourceError;
    #[instrument(
        name = "cache_source::basic_ref",
        level = "trace",
        skip(self),
        fields(source = %self.name(), address = %address)
    )]
    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.inner
            .json_rpc_db
            .basic_ref(address)
            .map_err(Into::into)
    }

    #[instrument(
        name = "cache_source::code_by_hash_ref",
        level = "trace",
        skip(self),
        fields(source = %self.name(), code_hash = %code_hash)
    )]
    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.inner
            .json_rpc_db
            .code_by_hash_ref(code_hash)
            .map_err(Into::into)
    }

    #[instrument(
        name = "cache_source::block_hash_ref",
        level = "trace",
        skip(self),
        fields(source = %self.name(), block_number = number)
    )]
    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        self.inner
            .json_rpc_db
            .block_hash_ref(number)
            .map_err(Into::into)
    }

    #[instrument(
        name = "cache_source::storage_ref",
        level = "trace",
        skip(self),
        fields(source = %self.name(), address = %address, index = %index)
    )]
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

impl Source for EthRpcSource {
    fn name(&self) -> SourceName {
        SourceName::EthRpcSource
    }

    fn is_synced(&self, min_synced_block: U256, latest_head: U256) -> bool {
        let client_latest_head = *self.inner.latest_head.read();
        let target_block =
            Self::calculate_target_block(min_synced_block, latest_head, client_latest_head);
        client_latest_head >= target_block
    }

    fn update_cache_status(&self, min_synced_block: U256, latest_head: U256) {
        let client_latest_head = *self.inner.latest_head.read();
        // Update the target block if the client is synced and the latest head is within the
        // range of the client's latest head
        self.inner
            .json_rpc_db
            .set_target_block(Self::calculate_target_block(
                min_synced_block,
                latest_head,
                client_latest_head,
            ));
    }
}

#[derive(Error, Debug)]
pub enum EthRpcSourceError {
    #[error("Failed to connect to the websocket provider")]
    Provider(#[source] RpcError<TransportErrorKind>),
    #[error("Failed to parse the HTTP provider URL")]
    HttpUrl(#[from] url::ParseError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::U256;

    fn u(n: u64) -> U256 {
        U256::from(n)
    }

    #[test]
    fn client_behind_and_min_synced_below_client() {
        // min(50, 100) = 50, max(50, 30) = 50
        let result = EthRpcSource::calculate_target_block(u(30), u(100), u(50));
        assert_eq!(result, u(50));
    }

    #[test]
    fn client_behind_and_min_synced_equals_client() {
        // min(50, 100) = 50, max(50, 50) = 50
        let result = EthRpcSource::calculate_target_block(u(50), u(100), u(50));
        assert_eq!(result, u(50));
    }

    #[test]
    fn client_behind_and_min_synced_above_client() {
        // min(50, 100) = 50, max(50, 70) = 70
        // min_synced_block wins because client can't satisfy the minimum requirement
        let result = EthRpcSource::calculate_target_block(u(70), u(100), u(50));
        assert_eq!(result, u(70));
    }

    #[test]
    fn client_ahead_and_min_synced_below_latest() {
        // min(100, 50) = 50, max(50, 30) = 50
        let result = EthRpcSource::calculate_target_block(u(30), u(50), u(100));
        assert_eq!(result, u(50));
    }

    #[test]
    fn client_ahead_and_min_synced_equals_latest() {
        // min(100, 50) = 50, max(50, 50) = 50
        let result = EthRpcSource::calculate_target_block(u(50), u(50), u(100));
        assert_eq!(result, u(50));
    }

    #[test]
    fn client_ahead_and_min_synced_above_latest() {
        // min(100, 50) = 50, max(50, 70) = 70
        // min_synced_block wins
        let result = EthRpcSource::calculate_target_block(u(70), u(50), u(100));
        assert_eq!(result, u(70));
    }

    #[test]
    fn client_equals_latest_and_min_synced_below() {
        // min(50, 50) = 50, max(50, 30) = 50
        let result = EthRpcSource::calculate_target_block(u(30), u(50), u(50));
        assert_eq!(result, u(50));
    }

    #[test]
    fn client_equals_latest_and_min_synced_above() {
        // min(50, 50) = 50, max(50, 70) = 70
        let result = EthRpcSource::calculate_target_block(u(70), u(50), u(50));
        assert_eq!(result, u(70));
    }

    #[test]
    fn all_three_equal() {
        // min(50, 50) = 50, max(50, 50) = 50
        let result = EthRpcSource::calculate_target_block(u(50), u(50), u(50));
        assert_eq!(result, u(50));
    }

    #[test]
    fn all_zero() {
        let result = EthRpcSource::calculate_target_block(u(0), u(0), u(0));
        assert_eq!(result, u(0));
    }

    #[test]
    fn client_at_zero() {
        // min(0, 100) = 0, max(0, 50) = 50
        let result = EthRpcSource::calculate_target_block(u(50), u(100), u(0));
        assert_eq!(result, u(50));
    }

    #[test]
    fn min_synced_at_zero() {
        // min(100, 50) = 50, max(50, 0) = 50
        let result = EthRpcSource::calculate_target_block(u(0), u(50), u(100));
        assert_eq!(result, u(50));
    }
}
