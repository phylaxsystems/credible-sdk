//! Core orchestration loop that keeps Redis in sync with the execution client.
//!
//! The worker bootstraps from the last persisted block, catches up to head via
//! RPC, and then tails new blocks from the `newHeads` subscription. Each block
//! is traced with the pre-state tracer and written into Redis.
//!
//! Uses gRPC bidirectional streaming via `StreamEvents` RPC.

use alloy::{
    consensus::{
        Transaction,
        TxType,
    },
    primitives::U256,
    providers::WsConnect,
    rpc::types::Header,
};
use alloy_provider::{
    Provider,
    ProviderBuilder,
    RootProvider,
};
use anyhow::{
    Context,
    Result,
    anyhow,
};
use futures::StreamExt;
use sidecar::transport::grpc::pb::{
    AccessListItem as ProtoAccessListItem,
    Authorization as ProtoAuthorization,
    BlobExcessGasAndPrice,
    BlockEnv,
    CommitHead,
    Event,
    GetTransactionsRequest,
    NewIteration,
    ResultStatus,
    StreamAck,
    Transaction as ProtoTransaction,
    TransactionEnv,
    TransactionResult,
    TxExecutionId as ProtoTxExecutionId,
    event::Event as EventVariant,
    sidecar_transport_client::SidecarTransportClient,
};
use std::{
    collections::HashMap,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::Mutex,
    time,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tracing::{
    debug,
    error,
    info,
    warn,
};

const SUBSCRIPTION_RETRY_DELAY_SECS: u64 = 5;
const MAX_RETRY_DELAY_SECS: u64 = 300;
const ACK_TIMEOUT_SECS: u64 = 30;
const EVENT_CHANNEL_BUFFER_SIZE: usize = 1024;
const DEFAULT_ITERATION_ID: u64 = 0;
const MAX_EVENT_RETRIES: u32 = 3;
const RETRY_DELAY_MS: u64 = 500;

/// Pending event awaiting acknowledgment
struct PendingEvent {
    event_id: u64,
    block_number: u64,
    event_type: &'static str,
    notify: Arc<tokio::sync::Notify>,
    result: Arc<Mutex<Option<bool>>>, // Some(true) = success, Some(false) = failed
}

/// Manages the gRPC bidirectional stream
struct EventStream {
    tx: tokio::sync::mpsc::Sender<Event>,
    pending_acks: Arc<Mutex<HashMap<u64, PendingEvent>>>,
    next_event_id: u64,
}

impl EventStream {
    fn new(tx: tokio::sync::mpsc::Sender<Event>) -> Self {
        Self {
            tx,
            pending_acks: Arc::new(Mutex::new(HashMap::new())),
            next_event_id: 1,
        }
    }

    /// Send an event and wait for its acknowledgment with timeout
    async fn send_event_and_wait(
        &mut self,
        event: Event,
        block_number: u64,
        event_type: &'static str,
    ) -> Result<()> {
        let event_id = event.event_id;
        let notify = Arc::new(tokio::sync::Notify::new());
        let result = Arc::new(Mutex::new(None));

        // Track pending ack
        self.pending_acks.lock().await.insert(
            event_id,
            PendingEvent {
                event_id,
                block_number,
                event_type,
                notify: notify.clone(),
                result: result.clone(),
            },
        );

        // Send the event, clean up pending_acks on failure
        if let Err(e) = self.tx.send(event).await {
            self.pending_acks.lock().await.remove(&event_id);
            return Err(anyhow!("failed to send event to stream: {e}"));
        }

        // Wait for ack with timeout
        let timeout_duration = Duration::from_secs(ACK_TIMEOUT_SECS);
        if let Ok(()) = time::timeout(timeout_duration, notify.notified()).await {
            // Check if the ack was successful
            let ack_result = result.lock().await.take();
            match ack_result {
                Some(true) => Ok(()),
                Some(false) => {
                    Err(anyhow!(
                        "event {event_type} (id={event_id}) for block {block_number} was rejected by sidecar"
                    ))
                }
                None => {
                    Err(anyhow!(
                        "event {event_type} (id={event_id}) for block {block_number}: notified but no result"
                    ))
                }
            }
        } else {
            // Timeout
            self.pending_acks.lock().await.remove(&event_id);
            Err(anyhow!(
                "timeout waiting for ack of {event_type} (id={event_id}) for block {block_number}"
            ))
        }
    }

    /// Send an event with retries, returning error only after all retries exhausted
    async fn send_event_with_retry<F>(
        &mut self,
        event_builder: F,
        block_number: u64,
        event_type: &'static str,
    ) -> Result<()>
    where
        F: Fn(u64) -> Event,
    {
        let mut last_error = None;

        for attempt in 1..=MAX_EVENT_RETRIES {
            let event_id = self.next_id();
            let event = event_builder(event_id);

            match self
                .send_event_and_wait(event, block_number, event_type)
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    warn!(
                        "Attempt {attempt}/{MAX_EVENT_RETRIES} failed for {event_type} \
                         on block {block_number}: {e}"
                    );
                    last_error = Some(e);

                    if attempt < MAX_EVENT_RETRIES {
                        time::sleep(Duration::from_millis(RETRY_DELAY_MS * u64::from(attempt)))
                            .await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("retry failed with no error")))
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_event_id;
        self.next_event_id += 1;
        id
    }
}

/// Coordinates block ingestion, tracing, and persistence via gRPC streaming.
pub struct Listener {
    ws_url: String,
    provider: Arc<RootProvider>,
    grpc_endpoint: String,
    /// Tracks the last block that was fully committed (new iteration + txs + commit head)
    last_committed_block: Option<u64>,
    starting_block: Option<u64>,
    /// Flag to enable/disable transaction result querying
    query_results: bool,
}

impl Listener {
    /// Build a worker that shares the provider/Redis client across async tasks.
    pub async fn new(ws_url: &str, grpc_endpoint: &str, starting_block: Option<u64>) -> Self {
        Self {
            ws_url: ws_url.to_string(),
            provider: Self::connect_provider_with_url(ws_url)
                .await
                .expect("failed to connect to websocket provider"),
            grpc_endpoint: grpc_endpoint.to_string(),
            last_committed_block: None,
            starting_block,
            query_results: false,
        }
    }

    /// Establish a WebSocket connection to the execution node and expose the
    /// underlying `RootProvider`. The root provider gives us access to the
    /// subscription + debug APIs used throughout the worker.
    async fn connect_provider(&self) -> Result<Arc<RootProvider>> {
        Self::connect_provider_with_url(&self.ws_url).await
    }

    async fn connect_provider_with_url(ws_url: &str) -> Result<Arc<RootProvider>> {
        let ws = WsConnect::new(ws_url);
        let provider = ProviderBuilder::new()
            .connect_ws(ws)
            .await
            .context("failed to connect to websocket provider")?;
        Ok(Arc::new(provider.root().clone()))
    }

    /// Enable transaction result querying and comparison
    pub fn with_result_querying(mut self, enabled: bool) -> Self {
        self.query_results = enabled;
        self
    }

    /// Connect to the gRPC sidecar endpoint
    async fn connect_grpc(&self) -> Result<SidecarTransportClient<Channel>> {
        let channel = Channel::from_shared(self.grpc_endpoint.clone())
            .context("invalid gRPC endpoint")?
            .connect()
            .await
            .context("failed to connect to gRPC endpoint")?;
        Ok(SidecarTransportClient::new(channel))
    }

    /// We keep retrying the subscription because websocket connections can drop in practice.
    pub async fn run(&mut self) -> Result<()> {
        let mut retry_delay = Duration::from_secs(SUBSCRIPTION_RETRY_DELAY_SECS);
        let max_delay = Duration::from_secs(MAX_RETRY_DELAY_SECS);

        loop {
            if let Err(err) = self.stream_blocks().await {
                warn!(error = %err, "block subscription ended, retrying");
                time::sleep(retry_delay).await;
                // Exponential backoff with cap
                retry_delay = std::cmp::min(retry_delay * 2, max_delay);
            }
            // Reconnect on error
            self.provider = self.connect_provider().await?;
        }
    }

    /// Follow the `newHeads` stream and process new blocks in order, tolerating
    /// duplicate/stale headers after it reconnects.
    async fn stream_blocks(&mut self) -> Result<()> {
        // Connect to gRPC and establish bidirectional stream
        let mut client = self.connect_grpc().await?;
        let (tx, rx) = tokio::sync::mpsc::channel::<Event>(EVENT_CHANNEL_BUFFER_SIZE);

        let response = client
            .stream_events(ReceiverStream::new(rx))
            .await
            .context("failed to establish event stream")?;

        let mut ack_stream = response.into_inner();
        let mut event_stream = EventStream::new(tx);

        // Spawn ack handler
        let pending_acks = event_stream.pending_acks.clone();
        let ack_handle = tokio::spawn(async move {
            while let Some(ack_result) = ack_stream.next().await {
                match ack_result {
                    Ok(ack) => Self::handle_ack(&pending_acks, ack).await,
                    Err(e) => {
                        error!(error = ?e, "Error receiving ack from stream");
                        break;
                    }
                }
            }
        });

        // First, catch up on any missed blocks
        self.catch_up_missed_blocks(&mut event_stream, &mut client)
            .await
            .context("failed to catch up on missed blocks")?;

        // Subscribe to new blocks
        let subscription = self.provider.subscribe_full_blocks().full();
        let mut block_stream = subscription.into_stream().await?;

        info!("Started block subscription with gRPC streaming");

        while let Some(block_result) = block_stream.next().await {
            let block = match block_result {
                Ok(b) => b,
                Err(e) => {
                    error!(error = ?e, "Error receiving block from stream");
                    ack_handle.abort();
                    return Err(anyhow!("block stream error: {e}"));
                }
            };

            let Header {
                hash: _, ref inner, ..
            } = block.header;
            let block_number = inner.number;

            // Check for missing blocks in the stream
            if let Some(last_committed) = self.last_committed_block {
                if block_number > last_committed + 1 {
                    // We've skipped blocks, fill them in
                    warn!(
                        "Block skip detected in stream: expected {}, got {block_number}",
                        last_committed + 1,
                    );

                    if let Err(e) = self
                        .fill_missing_blocks(
                            &mut event_stream,
                            &mut client,
                            last_committed,
                            block_number,
                        )
                        .await
                    {
                        error!(error = ?e, "Failed to fill missing blocks");
                        ack_handle.abort();
                        return Err(e);
                    }
                } else if block_number <= last_committed {
                    // Skip duplicate/stale blocks
                    debug!("Skipping already processed block {block_number}");
                    continue;
                }
            }

            info!("Processing block {block_number}");

            if let Err(e) = self
                .process_block(&mut event_stream, &mut client, &block)
                .await
            {
                error!(error = ?e, "Failed to process block {block_number}");
                ack_handle.abort();
                // Return error to trigger reconnection and catch-up
                return Err(e);
            }
        }

        ack_handle.abort();
        Err(anyhow!("block subscription completed"))
    }

    async fn handle_ack(pending: &Arc<Mutex<HashMap<u64, PendingEvent>>>, ack: StreamAck) {
        let mut pending = pending.lock().await;
        if let Some(event) = pending.remove(&ack.event_id) {
            // Store the result and notify the waiter
            *event.result.lock().await = Some(ack.success);
            event.notify.notify_one();

            if ack.success {
                debug!(
                    "Ack received for {} event_id={} block={}",
                    event.event_type, event.event_id, event.block_number
                );
            } else {
                warn!(
                    "Event failed: {} event_id={} block={}: {}",
                    event.event_type, event.event_id, event.block_number, ack.message
                );
            }
        }
    }

    /// Catch up on any missed blocks by fetching them via RPC
    async fn catch_up_missed_blocks(
        &mut self,
        stream: &mut EventStream,
        client: &mut SidecarTransportClient<Channel>,
    ) -> Result<()> {
        // Get the current head block
        let current_block = self
            .provider
            .get_block_number()
            .await
            .context("failed to get current block number")?;

        // Determine which block to use for the initial CommitHead.
        // This must be sent on EVERY new gRPC connection to sync state with the sidecar.
        let initial_commit_block = if let Some(last_committed) = self.last_committed_block {
            // We have a previously committed block, use it to re-sync with sidecar
            last_committed
        } else if let Some(start_block) = self.starting_block {
            // First run with a specified starting block - commit the block before it
            if start_block == 0 {
                warn!(
                    "Starting block is 0, cannot commit previous block. Committing block 0 instead."
                );
                0
            } else {
                start_block.saturating_sub(1)
            }
        } else {
            // First run with no starting block specified - commit the current block
            current_block
        };

        info!("Sending initial CommitHead for block {initial_commit_block} to sync with sidecar");

        // Fetch the block to get proper timestamp and hash
        let initial_block = self
            .provider
            .get_block_by_number(initial_commit_block.into())
            .full()
            .await
            .context("failed to fetch initial commit block")?
            .ok_or_else(|| anyhow!("initial commit block {initial_commit_block} not found"))?;

        // Send CommitHead with no transactions (we're just syncing state with sidecar)
        self.send_commit_head_with_retry(
            stream,
            initial_commit_block,
            None,
            0,
            initial_block.header.timestamp,
            Some(initial_block.header.hash),
            initial_block.header.parent_beacon_block_root,
        )
        .await
        .context("failed to send initial CommitHead")?;

        self.last_committed_block = Some(initial_commit_block);
        info!("Successfully sent initial CommitHead for block {initial_commit_block}");

        // Now check if we need to catch up on any blocks
        if let Some(last_committed) = self.last_committed_block {
            if current_block > last_committed {
                let missed_count = current_block - last_committed;
                info!(
                    "Catching up on {missed_count} missed blocks (from {} to {current_block})",
                    last_committed + 1,
                );

                self.process_block_range(stream, client, last_committed + 1, current_block)
                    .await?;
            } else {
                debug!("No missed blocks to catch up on");
            }
        }

        Ok(())
    }

    /// Fill in missing blocks when WebSocket skips blocks
    async fn fill_missing_blocks(
        &mut self,
        stream: &mut EventStream,
        client: &mut SidecarTransportClient<Channel>,
        last_block: u64,
        current_block: u64,
    ) -> Result<()> {
        if current_block <= last_block + 1 {
            return Ok(());
        }

        let missing_start = last_block + 1;
        let missing_end = current_block - 1;

        warn!(
            "Detected missing blocks in stream: {missing_start} to {missing_end} ({} blocks)",
            missing_end - missing_start + 1
        );

        self.process_block_range(stream, client, missing_start, missing_end)
            .await
    }

    async fn process_block_range(
        &mut self,
        stream: &mut EventStream,
        client: &mut SidecarTransportClient<Channel>,
        start: u64,
        end: u64,
    ) -> Result<()> {
        let mut failed_blocks = Vec::new();

        for block_num in start..=end {
            // Don't propagate errors during batch processing
            // If sidecar is down, we skip failed blocks and continue
            // This prevents getting stuck in a retry loop
            if let Err(e) = self
                .fetch_and_process_block(stream, client, block_num)
                .await
            {
                warn!(
                    error = ?e,
                    "Failed to process block {block_num} during catch-up, continuing to next block"
                );
                failed_blocks.push(block_num);
            }
        }

        if !failed_blocks.is_empty() {
            let total_blocks = end - start + 1;
            let failed_count = failed_blocks.len();
            warn!(
                "Catch-up completed with {failed_count}/{total_blocks} failed blocks: {:?}",
                failed_blocks
            );
        }

        Ok(())
    }

    /// Fetch a specific block by number and process it
    async fn fetch_and_process_block(
        &mut self,
        stream: &mut EventStream,
        client: &mut SidecarTransportClient<Channel>,
        block_num: u64,
    ) -> Result<()> {
        match self
            .provider
            .get_block_by_number(block_num.into())
            .full()
            .await
        {
            Ok(Some(block)) => {
                info!("Processing block {block_num}");
                self.process_block(stream, client, &block).await
            }
            Ok(None) => {
                warn!("Block {block_num} not found");
                Err(anyhow!("Block {block_num} not found"))
            }
            Err(e) => {
                error!(error = ?e, "Failed to fetch block {block_num}");
                Err(e.into())
            }
        }
    }

    /// Process a single block with the flow using `StreamEvents`:
    /// 1. Send `NewIteration` event (with retries)
    /// 2. Send all transactions in order (with retries per tx)
    /// 3. Send `CommitHead` event to finalize the block (with retries)
    /// 4. Mark block as committed only after all steps succeed
    ///
    /// If `NewIteration` or transactions fail after retries, we skip to `CommitHead`
    /// to ensure we don't get stuck on a single block.
    #[allow(clippy::too_many_lines)]
    async fn process_block(
        &mut self,
        stream: &mut EventStream,
        client: &mut SidecarTransportClient<Channel>,
        block: &alloy::rpc::types::Block,
    ) -> Result<()> {
        let block_number = block.header.number;

        // Skip already processed blocks (prevent duplicates)
        if let Some(last_committed) = self.last_committed_block
            && block_number <= last_committed
        {
            debug!("Skipping already committed block {block_number}");
            return Ok(());
        }

        // Extract transactions from the block
        let transactions = match &block.transactions {
            alloy::rpc::types::BlockTransactions::Full(txs) => txs.clone(),
            alloy::rpc::types::BlockTransactions::Hashes(_) => {
                return Err(anyhow!(
                    "Got hashes instead of full transactions despite using .full()"
                ));
            }
            alloy::rpc::types::BlockTransactions::Uncle => Vec::new(),
        };

        debug!(
            "Block {block_number} has {} transactions",
            transactions.len()
        );

        // STEP 1: Send NewIteration event (contains block_env) with retries
        info!("Step 1/3: Sending NewIteration event for block {block_number}");
        let new_iteration_success = match self.send_new_iteration_with_retry(stream, block).await {
            Ok(()) => {
                debug!("Successfully sent NewIteration for block {block_number}");
                true
            }
            Err(e) => {
                warn!(
                    "Failed to send NewIteration for block {block_number} after retries: {e}. \
                     Skipping transactions and moving to CommitHead."
                );
                false
            }
        };

        // STEP 2: Send all transactions for the current block (only if NewIteration succeeded)
        let mut last_successful_tx_hash: Option<Vec<u8>> = None;
        let mut successful_tx_count = 0;
        let mut successful_tx_hashes: Vec<alloy::primitives::B256> = Vec::new();

        if new_iteration_success && !transactions.is_empty() {
            info!(
                "Step 2/3: Sending {} transactions for block {block_number}",
                transactions.len()
            );

            for (index, tx) in transactions.iter().enumerate() {
                let prev_hash = last_successful_tx_hash.clone();
                let tx_hash = tx.inner.hash();

                match self
                    .send_transaction_with_retry(stream, tx, index as u64, block_number, prev_hash)
                    .await
                {
                    Ok(()) => {
                        last_successful_tx_hash = Some(tx_hash.to_vec());
                        successful_tx_count += 1;
                        successful_tx_hashes.push(*tx_hash);
                    }
                    Err(e) => {
                        warn!(
                            "Failed to send tx {index} in block {block_number} after retries: {e}. \
                             Moving to CommitHead with {successful_tx_count} successful txs."
                        );
                        // Stop processing transactions, move to commit
                        break;
                    }
                }
            }

            if successful_tx_count > 0 {
                debug!(
                    "Successfully sent {successful_tx_count}/{} transactions for block {block_number}",
                    transactions.len()
                );
            }
        } else if !new_iteration_success {
            info!("Step 2/3: Skipping transactions (NewIteration failed)");
        } else {
            info!("Step 2/3: No transactions in block {block_number}");
        }

        // STEP 3: Send CommitHead event to finalize the block (with retries)
        info!("Step 3/3: Sending CommitHead event for block {block_number}");
        self.send_commit_head_with_retry(
            stream,
            block_number,
            last_successful_tx_hash,
            successful_tx_count,
            block.header.timestamp,
            Some(block.header.hash),
            block.header.parent_beacon_block_root,
        )
        .await?;

        // STEP 4: Mark block as committed ONLY after CommitHead succeeds
        self.last_committed_block = Some(block_number);

        info!(
            "Successfully committed block {block_number} ({successful_tx_count}/{} txs)",
            transactions.len()
        );

        // STEP 5: Compare transaction results if enabled
        if self.query_results
            && !successful_tx_hashes.is_empty()
            && let Err(e) = self
                .compare_transaction_results(client, block_number, &successful_tx_hashes)
                .await
        {
            warn!("Failed to compare transaction results for block {block_number}: {e}");
        }

        Ok(())
    }

    /// Query and compare transaction results between provider and sidecar
    async fn compare_transaction_results(
        &self,
        client: &mut SidecarTransportClient<Channel>,
        block_number: u64,
        tx_hashes: &[alloy::primitives::B256],
    ) -> Result<()> {
        if tx_hashes.is_empty() {
            return Ok(());
        }

        info!(
            "Comparing {} transaction results for block {block_number}",
            tx_hashes.len()
        );

        // Build request for sidecar
        let tx_execution_ids: Vec<ProtoTxExecutionId> = tx_hashes
            .iter()
            .enumerate()
            .map(|(index, hash)| {
                ProtoTxExecutionId {
                    block_number: u256_to_bytes(U256::from(block_number)),
                    iteration_id: DEFAULT_ITERATION_ID,
                    tx_hash: hash.to_vec(),
                    index: index as u64,
                }
            })
            .collect();

        // Query sidecar for results
        let sidecar_response = client
            .get_transactions(GetTransactionsRequest { tx_execution_ids })
            .await
            .context("failed to query sidecar for transaction results")?
            .into_inner();

        // Build a map of tx_hash -> sidecar result
        let sidecar_results: HashMap<Vec<u8>, &TransactionResult> = sidecar_response
            .results
            .iter()
            .filter_map(|r| r.tx_execution_id.as_ref().map(|id| (id.tx_hash.clone(), r)))
            .collect();

        // Log not found transactions from sidecar
        if !sidecar_response.not_found.is_empty() {
            warn!(
                "Sidecar did not find {} transactions for block {block_number}",
                sidecar_response.not_found.len()
            );
        }

        let mut mismatches = 0;
        let mut matches = 0;

        // Query provider receipts and compare
        for tx_hash in tx_hashes {
            let receipt = match self.provider.get_transaction_receipt(*tx_hash).await {
                Ok(Some(r)) => r,
                Ok(None) => {
                    warn!("Transaction receipt not found from provider for {tx_hash}");
                    continue;
                }
                Err(e) => {
                    warn!("Failed to get transaction receipt for {tx_hash}: {e}");
                    continue;
                }
            };

            let provider_success = receipt.status();
            let provider_gas_used = receipt.gas_used;

            // Get sidecar result
            let Some(sidecar_result) = sidecar_results.get(&tx_hash.to_vec()) else {
                warn!("Sidecar result not found for {tx_hash}");
                mismatches += 1;
                continue;
            };

            // Convert sidecar status to success bool
            let sidecar_success = sidecar_result.status == ResultStatus::Success as i32;
            let sidecar_gas_used = sidecar_result.gas_used;

            // Compare status
            let status_matches = provider_success == sidecar_success;

            // Compare gas used
            let gas_matches = provider_gas_used == sidecar_gas_used;

            if status_matches && gas_matches {
                matches += 1;
                debug!(
                    "TX {tx_hash}: MATCH - status={provider_success}, gas_used={provider_gas_used}"
                );
            } else {
                mismatches += 1;
                let sidecar_status_str = result_status_to_string(sidecar_result.status);
                warn!(
                    "TX {tx_hash}: MISMATCH - \
                     provider(status={provider_success}, gas={provider_gas_used}) vs \
                     sidecar(status={sidecar_status_str}, gas={sidecar_gas_used})"
                );

                if !status_matches {
                    warn!(
                        "  Status mismatch: provider={provider_success}, sidecar={sidecar_status_str}"
                    );
                }
                if !gas_matches {
                    let gas_diff = provider_gas_used.abs_diff(sidecar_gas_used);
                    warn!(
                        "  Gas mismatch: provider={provider_gas_used}, sidecar={sidecar_gas_used} (diff={gas_diff})"
                    );
                }

                // Log sidecar error if present
                if !sidecar_result.error.is_empty() {
                    warn!("  Sidecar error: {}", sidecar_result.error);
                }
            }
        }

        info!(
            "Block {block_number} result comparison: {matches} matches, {mismatches} mismatches out of {} transactions",
            tx_hashes.len()
        );

        Ok(())
    }

    /// Build the `BlockEnv` protobuf message from an Alloy block
    fn build_block_env(block: &alloy::rpc::types::Block) -> BlockEnv {
        BlockEnv {
            number: u256_to_bytes(U256::from(block.header.number)),
            beneficiary: block.header.beneficiary.to_vec(),
            timestamp: u256_to_bytes(U256::from(block.header.timestamp)),
            gas_limit: block.header.gas_limit,
            basefee: block.header.base_fee_per_gas.unwrap_or_default(),
            difficulty: u256_to_bytes(block.header.difficulty),
            prevrandao: Some(block.header.mix_hash.to_vec()),
            blob_excess_gas_and_price: Some(BlobExcessGasAndPrice {
                excess_blob_gas: 0,
                blob_gasprice: u128_to_bytes(1),
            }),
        }
    }

    /// Send `NewIteration` event to the sidecar via gRPC stream with retries
    async fn send_new_iteration_with_retry(
        &self,
        stream: &mut EventStream,
        block: &alloy::rpc::types::Block,
    ) -> Result<()> {
        let block_number = block.header.number;
        let block_env = Self::build_block_env(block);

        stream
            .send_event_with_retry(
                |event_id| {
                    Event {
                        event_id,
                        event: Some(EventVariant::NewIteration(NewIteration {
                            iteration_id: DEFAULT_ITERATION_ID,
                            block_env: Some(block_env.clone()),
                        })),
                    }
                },
                block_number,
                "NewIteration",
            )
            .await
    }

    /// Send a transaction to the sidecar via gRPC stream with retries
    async fn send_transaction_with_retry(
        &self,
        stream: &mut EventStream,
        tx: &alloy::rpc::types::Transaction,
        index: u64,
        block_number: u64,
        prev_tx_hash: Option<Vec<u8>>,
    ) -> Result<()> {
        let tx_hash = tx.inner.hash();
        let tx_env = Self::to_proto_tx_env(tx);

        stream
            .send_event_with_retry(
                |event_id| {
                    Event {
                        event_id,
                        event: Some(EventVariant::Transaction(ProtoTransaction {
                            tx_execution_id: Some(ProtoTxExecutionId {
                                block_number: u256_to_bytes(U256::from(block_number)),
                                iteration_id: DEFAULT_ITERATION_ID,
                                tx_hash: tx_hash.to_vec(),
                                index,
                            }),
                            tx_env: Some(tx_env.clone()),
                            prev_tx_hash: prev_tx_hash.clone(),
                        })),
                    }
                },
                block_number,
                "Transaction",
            )
            .await
    }

    /// Send `CommitHead` event to finalize the block with retries
    #[allow(clippy::too_many_arguments)]
    async fn send_commit_head_with_retry(
        &self,
        stream: &mut EventStream,
        block_number: u64,
        last_tx_hash: Option<Vec<u8>>,
        n_transactions: usize,
        timestamp: u64,
        block_hash: Option<alloy::primitives::B256>,
        parent_beacon_block_root: Option<alloy::primitives::B256>,
    ) -> Result<()> {
        let block_hash_bytes = block_hash.map(|h| h.to_vec());
        let parent_beacon_bytes = parent_beacon_block_root.map(|h| h.to_vec());

        stream
            .send_event_with_retry(
                |event_id| {
                    Event {
                        event_id,
                        event: Some(EventVariant::CommitHead(CommitHead {
                            block_number: u256_to_bytes(U256::from(block_number)),
                            last_tx_hash: last_tx_hash.clone(),
                            n_transactions: n_transactions as u64,
                            selected_iteration_id: DEFAULT_ITERATION_ID,
                            block_hash: block_hash_bytes.clone(),
                            parent_beacon_block_root: parent_beacon_bytes.clone(),
                            timestamp: u256_to_bytes(U256::from(timestamp)),
                        })),
                    }
                },
                block_number,
                "CommitHead",
            )
            .await
    }

    /// Convert an Alloy transaction to protobuf `TransactionEnv`
    fn to_proto_tx_env(tx: &alloy::rpc::types::Transaction) -> TransactionEnv {
        let tx_type = match tx.inner.tx_type() {
            TxType::Legacy => 0,
            TxType::Eip2930 => 1,
            TxType::Eip1559 => 2,
            TxType::Eip4844 => 3,
            TxType::Eip7702 => 4,
        };

        let transact_to = tx.to().map(|a| a.to_vec()).unwrap_or_default();

        let access_list: Vec<ProtoAccessListItem> = tx
            .access_list()
            .map(|al| {
                al.0.iter()
                    .map(|item| {
                        ProtoAccessListItem {
                            address: item.address.to_vec(),
                            storage_keys: item.storage_keys.iter().map(|k| k.to_vec()).collect(),
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        let blob_hashes: Vec<Vec<u8>> = tx
            .blob_versioned_hashes()
            .map(|hashes| hashes.iter().map(|h| h.to_vec()).collect())
            .unwrap_or_default();

        let max_fee_per_blob_gas = tx
            .max_fee_per_blob_gas()
            .map_or_else(|| u128_to_bytes(0), u128_to_bytes);

        let authorization_list: Vec<ProtoAuthorization> = tx
            .authorization_list()
            .map(|auths| {
                auths
                    .iter()
                    .filter_map(|auth| {
                        let sig = auth.signature().ok()?;
                        Some(ProtoAuthorization {
                            chain_id: U256::from(auth.chain_id).to_be_bytes_vec(),
                            address: auth.address.to_vec(),
                            nonce: auth.nonce,
                            y_parity: u32::from(sig.v()),
                            r: sig.r().to_be_bytes_vec(),
                            s: sig.s().to_be_bytes_vec(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        // Handle gas_price and gas_priority_fee based on transaction type
        let (gas_price, gas_priority_fee) = match tx.inner.tx_type() {
            // Legacy transaction
            TxType::Legacy |
            // EIP-2930 - Access list transaction
            TxType::Eip2930 => {
                // Use gas_price directly, no priority fee
                (
                    u128_to_bytes(tx.inner.gas_price().unwrap_or_default()),
                    None,
                )
            }
            // EIP-1559 - Dynamic fee transaction
            TxType::Eip1559 |
            // EIP-4844 - Blob transaction
            TxType::Eip4844 |
            // EIP-7702 - Account delegation transaction
            TxType::Eip7702 => {
                // Use max_fee_per_gas as gas_price, and set priority fee
                let max_fee = tx.inner.max_fee_per_gas();
                let max_priority = tx.inner.max_priority_fee_per_gas();

                (
                    u128_to_bytes(max_fee),
                    max_priority.map(u128_to_bytes),
                )
            }
        };

        TransactionEnv {
            tx_type,
            caller: tx.inner.signer().to_vec(),
            gas_limit: tx.inner.gas_limit(),
            gas_price,
            transact_to,
            value: u256_to_bytes(tx.value()),
            data: tx.input().to_vec(),
            nonce: tx.nonce(),
            chain_id: tx.chain_id(),
            access_list,
            gas_priority_fee,
            blob_hashes,
            max_fee_per_blob_gas,
            authorization_list,
        }
    }
}

// Helper functions for big-endian encoding
fn u256_to_bytes(val: U256) -> Vec<u8> {
    val.to_be_bytes_vec()
}

fn u128_to_bytes(val: u128) -> Vec<u8> {
    val.to_be_bytes().to_vec()
}

/// Convert `ResultStatus` enum value to human-readable string
fn result_status_to_string(status: i32) -> &'static str {
    match status {
        x if x == ResultStatus::Success as i32 => "SUCCESS",
        x if x == ResultStatus::Reverted as i32 => "REVERTED",
        x if x == ResultStatus::Halted as i32 => "HALTED",
        x if x == ResultStatus::Failed as i32 => "FAILED",
        x if x == ResultStatus::AssertionFailed as i32 => "ASSERTION_FAILED",
        _ => "UNKNOWN",
    }
}
