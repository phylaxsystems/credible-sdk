#![allow(clippy::too_many_lines)]
#![allow(clippy::format_collect)]
//! Core orchestration loop that keeps MDBX in sync with the execution client.
//!
//! The worker bootstraps from the last persisted block, catches up to head via
//! RPC, and then tails new blocks from the `newHeads` subscription. Each block
//! is traced with the pre-state tracer and written into MDBX.
//!
//! Uses gRPC bidirectional streaming via `StreamEvents` RPC.

use alloy::{
    consensus::{
        Transaction,
        TxType,
    },
    primitives::{
        B256,
        U256,
    },
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
    collections::{
        HashMap,
        VecDeque,
    },
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{
        Mutex,
        Notify,
        mpsc,
        watch,
    },
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
const MAX_GRPC_CONNECTION_RETRIES: u32 = 3;
const GRPC_RETRY_DELAY_SECS: u64 = 2;

// Timeout constants
const WS_CONNECT_TIMEOUT_SECS: u64 = 30;
const GRPC_CONNECT_TIMEOUT_SECS: u64 = 30;
const RPC_CALL_TIMEOUT_SECS: u64 = 30;
const BLOCK_FETCH_TIMEOUT_SECS: u64 = 60;
const TX_RESULT_COMPARISON_TIMEOUT_SECS: u64 = 300;
const GET_TX_RECEIPT_TIMEOUT_SECS: u64 = 60;
const GRPC_GET_TRANSACTIONS_TIMEOUT_SECS: u64 = 120;
const SUBSCRIPTION_SETUP_TIMEOUT_SECS: u64 = 30;
const COMPARISON_CHANNEL_BUFFER_SIZE: usize = 256;
const COMPARISON_BACKLOG_LIMIT: usize = 4096;
const COMPARISON_MAX_QUEUE_AGE: Duration = Duration::from_secs(2);

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
    /// Watch channel receiver to detect stream death
    stream_dead_rx: watch::Receiver<bool>,
}

impl EventStream {
    fn new(tx: tokio::sync::mpsc::Sender<Event>, stream_dead_rx: watch::Receiver<bool>) -> Self {
        Self {
            tx,
            pending_acks: Arc::new(Mutex::new(HashMap::new())),
            next_event_id: 1,
            stream_dead_rx,
        }
    }

    /// Check if the stream is still alive
    fn is_stream_dead(&self) -> bool {
        *self.stream_dead_rx.borrow()
    }

    /// Send an event and wait for its acknowledgment with timeout
    async fn send_event_and_wait(
        &mut self,
        event: Event,
        block_number: u64,
        event_type: &'static str,
    ) -> Result<()> {
        // Check if stream is already dead before attempting to send
        if self.is_stream_dead() {
            return Err(anyhow!(
                "stream is dead, cannot send {event_type} for block {block_number}"
            ));
        }

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

        debug!("Sent {event_type} event (id={event_id}) for block {block_number}, awaiting ack");

        // Wait for ack with timeout, also checking for stream death
        let timeout_duration = Duration::from_secs(ACK_TIMEOUT_SECS);
        let mut stream_dead_rx = self.stream_dead_rx.clone();

        tokio::select! {
            // Wait for the ack notification
            () = notify.notified() => {
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
            }
            // Watch for stream death
            _ = stream_dead_rx.changed() => {
                self.pending_acks.lock().await.remove(&event_id);
                Err(anyhow!(
                    "stream died while waiting for ack of {event_type} (id={event_id}) for block {block_number}"
                ))
            }
            // Timeout
            () = time::sleep(timeout_duration) => {
                self.pending_acks.lock().await.remove(&event_id);
                Err(anyhow!(
                    "timeout waiting for ack of {event_type} (id={event_id}) for block {block_number}"
                ))
            }
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
            // Check if stream is dead before each attempt
            if self.is_stream_dead() {
                return Err(anyhow!(
                    "stream is dead, aborting retries for {event_type} on block {block_number}"
                ));
            }

            let event_id = self.next_id();
            let event = event_builder(event_id);

            match self
                .send_event_and_wait(event, block_number, event_type)
                .await
            {
                Ok(()) => {
                    info!(
                        "{event_type} sent successfully to sidecar (block={block_number}, event_id={event_id})"
                    );
                    return Ok(());
                }
                Err(e) => {
                    // If stream is dead, don't bother retrying
                    if self.is_stream_dead() {
                        return Err(anyhow!(
                            "stream died during {event_type} on block {block_number}: {e}"
                        ));
                    }

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

/// Request to compare transaction results for a block
struct ComparisonRequest {
    block_number: u64,
    tx_hashes: Vec<alloy::primitives::B256>,
    client: SidecarTransportClient<Channel>,
    provider: Arc<RootProvider>,
    enqueued_at: time::Instant,
}

/// Manages comparison request buffering and backpressure.
struct ComparisonManager {
    tx: mpsc::Sender<ComparisonRequest>,
    backlog: Mutex<VecDeque<ComparisonRequest>>,
    backlog_limit: usize,
    backlog_notify: Notify,
    backlog_space_notify: Notify,
}

impl ComparisonManager {
    fn new(tx: mpsc::Sender<ComparisonRequest>, backlog_limit: usize) -> Self {
        Self {
            tx,
            backlog: Mutex::new(VecDeque::new()),
            backlog_limit,
            backlog_notify: Notify::new(),
            backlog_space_notify: Notify::new(),
        }
    }

    async fn enqueue(&self, request: ComparisonRequest) -> Result<()> {
        let mut pending = Some(request);

        loop {
            if pending
                .as_ref()
                .is_some_and(|r| r.enqueued_at.elapsed() > COMPARISON_MAX_QUEUE_AGE)
            {
                warn!(
                    "Dropping stale comparison request (>{}s)",
                    COMPARISON_MAX_QUEUE_AGE.as_secs()
                );
                return Ok(());
            }

            let notified = self.backlog_space_notify.notified();
            let mut backlog = self.backlog.lock().await;

            if !backlog.is_empty() {
                if backlog.len() >= self.backlog_limit {
                    drop(backlog);
                    notified.await;
                    continue;
                }

                backlog.push_back(pending.take().expect("pending request missing"));
                drop(backlog);
                self.backlog_notify.notify_one();
                return Ok(());
            }

            match self
                .tx
                .try_send(pending.take().expect("pending request missing"))
            {
                Ok(()) => return Ok(()),
                Err(mpsc::error::TrySendError::Full(request)) => {
                    if request.enqueued_at.elapsed() > COMPARISON_MAX_QUEUE_AGE {
                        drop(backlog);
                        warn!(
                            "Dropping stale comparison request (>{}s)",
                            COMPARISON_MAX_QUEUE_AGE.as_secs()
                        );
                        return Ok(());
                    }
                    if backlog.len() >= self.backlog_limit {
                        pending = Some(request);
                        drop(backlog);
                        notified.await;
                        continue;
                    }

                    backlog.push_back(request);
                    drop(backlog);
                    self.backlog_notify.notify_one();
                    return Ok(());
                }
                Err(mpsc::error::TrySendError::Closed(_request)) => {
                    return Err(anyhow!("comparison worker closed"));
                }
            }
        }
    }

    async fn run_backlog_drainer(self: Arc<Self>) {
        loop {
            let next = {
                let mut backlog = self.backlog.lock().await;
                let mut next = backlog.pop_front();
                while let Some(request) = next.as_ref() {
                    if request.enqueued_at.elapsed() <= COMPARISON_MAX_QUEUE_AGE {
                        break;
                    }
                    warn!(
                        "Dropping stale comparison request from backlog (>{}s)",
                        COMPARISON_MAX_QUEUE_AGE.as_secs()
                    );
                    next = backlog.pop_front();
                }
                if next.is_some() {
                    self.backlog_space_notify.notify_one();
                }
                next
            };

            if let Some(request) = next {
                let elapsed = request.enqueued_at.elapsed();
                if elapsed > COMPARISON_MAX_QUEUE_AGE {
                    warn!(
                        "Dropping stale comparison request before send (>{}s)",
                        COMPARISON_MAX_QUEUE_AGE.as_secs()
                    );
                    continue;
                }

                let remaining = COMPARISON_MAX_QUEUE_AGE.saturating_sub(elapsed);
                match time::timeout(remaining, self.tx.send(request)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        warn!("Comparison backlog drainer failed to send request: {e}");
                        return;
                    }
                    Err(_) => {
                        warn!(
                            "Dropping stale comparison request while waiting to send (>{}s)",
                            COMPARISON_MAX_QUEUE_AGE.as_secs()
                        );
                    }
                }
                continue;
            }

            self.backlog_notify.notified().await;
        }
    }
}

/// Background worker that processes transaction result comparisons
struct ComparisonWorker {
    rx: mpsc::Receiver<ComparisonRequest>,
}

impl ComparisonWorker {
    fn new(rx: mpsc::Receiver<ComparisonRequest>) -> Self {
        Self { rx }
    }

    async fn run(mut self) {
        info!("Comparison worker started");

        while let Some(request) = self.rx.recv().await {
            let block_number = request.block_number;

            // Small delay to allow sidecar to process results
            time::sleep(Duration::from_millis(100)).await;

            // Process comparison without blocking the sender
            match time::timeout(
                Duration::from_secs(TX_RESULT_COMPARISON_TIMEOUT_SECS),
                Listener::compare_transaction_results(
                    request.client,
                    request.provider,
                    request.block_number,
                    request.tx_hashes,
                ),
            )
            .await
            {
                Ok(Ok(())) => {
                    debug!("Comparison completed for block {block_number}");
                }
                Ok(Err(e)) => {
                    warn!("Failed to compare results for block {block_number}: {e}");
                }
                Err(_) => {
                    warn!(
                        "Timeout comparing results for block {block_number} (exceeded {}s)",
                        TX_RESULT_COMPARISON_TIMEOUT_SECS
                    );
                }
            }
        }

        info!("Comparison worker shutting down");
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
    /// Channel to send comparison requests to background worker
    comparison_manager: Option<Arc<ComparisonManager>>,
}

impl Listener {
    /// Build a worker that shares the provider/MDBX client across async tasks.
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
            comparison_manager: None,
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
        let connect_future = ProviderBuilder::new().connect_ws(ws);

        let provider = time::timeout(Duration::from_secs(WS_CONNECT_TIMEOUT_SECS), connect_future)
            .await
            .context("timeout connecting to websocket provider")?
            .context("failed to connect to websocket provider")?;

        Ok(Arc::new(provider.root().clone()))
    }

    /// Enable transaction result querying and comparison
    pub fn with_result_querying(mut self, enabled: bool) -> Self {
        self.query_results = enabled;

        if enabled {
            // Create channel and spawn background worker
            let (tx, rx) = mpsc::channel::<ComparisonRequest>(COMPARISON_CHANNEL_BUFFER_SIZE);
            let manager = Arc::new(ComparisonManager::new(tx, COMPARISON_BACKLOG_LIMIT));
            self.comparison_manager = Some(manager.clone());

            tokio::spawn(manager.run_backlog_drainer());

            let worker = ComparisonWorker::new(rx);
            tokio::spawn(worker.run());
        }

        self
    }

    /// Connect to the gRPC sidecar endpoint
    async fn connect_grpc(&self) -> Result<SidecarTransportClient<Channel>> {
        let channel =
            Channel::from_shared(self.grpc_endpoint.clone()).context("invalid gRPC endpoint")?;

        let channel = time::timeout(
            Duration::from_secs(GRPC_CONNECT_TIMEOUT_SECS),
            channel.connect(),
        )
        .await
        .context("timeout connecting to gRPC endpoint")?
        .context("failed to connect to gRPC endpoint")?;

        Ok(SidecarTransportClient::new(channel))
    }

    /// Connect to gRPC with retry logic. Retries up to `MAX_GRPC_CONNECTION_RETRIES` times
    /// before returning an error that triggers a full restart.
    async fn connect_grpc_with_retry(&self) -> Result<SidecarTransportClient<Channel>> {
        let mut last_error = None;

        for attempt in 1..=MAX_GRPC_CONNECTION_RETRIES {
            info!(
                "Attempting gRPC connection (attempt {}/{})",
                attempt, MAX_GRPC_CONNECTION_RETRIES
            );

            match self.connect_grpc().await {
                Ok(client) => {
                    info!("Successfully connected to gRPC endpoint");
                    return Ok(client);
                }
                Err(e) => {
                    warn!(
                        "gRPC connection attempt {}/{} failed: {}",
                        attempt, MAX_GRPC_CONNECTION_RETRIES, e
                    );
                    last_error = Some(e);

                    if attempt < MAX_GRPC_CONNECTION_RETRIES {
                        let delay = Duration::from_secs(GRPC_RETRY_DELAY_SECS * u64::from(attempt));
                        info!("Retrying gRPC connection in {:?}", delay);
                        time::sleep(delay).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("gRPC connection failed with no error")))
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
        // Connect to gRPC with retry logic (tries 3 times before full restart)
        let mut client = self.connect_grpc_with_retry().await?;

        // Inner loop to handle gRPC stream reconnection without full restart
        loop {
            match self.run_stream_loop(&mut client).await {
                Ok(()) => {
                    // Stream completed normally (shouldn't happen in practice)
                    return Ok(());
                }
                Err(e) => {
                    // Check if this is a gRPC stream error that we should retry
                    let error_str = e.to_string();
                    let is_grpc_stream_error = error_str.contains("stream died")
                        || error_str.contains("gRPC stream died")
                        || error_str.contains("stream is dead");

                    if is_grpc_stream_error {
                        warn!("gRPC stream died, attempting to reconnect...");

                        // Try to reconnect gRPC with retry logic
                        match self.connect_grpc_with_retry().await {
                            Ok(new_client) => {
                                info!("Successfully reconnected to gRPC after stream death");
                                client = new_client;
                                // Continue the loop with the new client
                                continue;
                            }
                            Err(reconnect_err) => {
                                error!(
                                    "Failed to reconnect gRPC after {MAX_GRPC_CONNECTION_RETRIES} attempts: {reconnect_err}",
                                );
                                // Return error to trigger full restart in run()
                                return Err(anyhow!(
                                    "gRPC reconnection failed after {MAX_GRPC_CONNECTION_RETRIES} attempts, triggering full restart: {reconnect_err}",
                                ));
                            }
                        }
                    }
                    // Non-gRPC error (e.g., block stream error), trigger full restart
                    return Err(e);
                }
            }
        }
    }

    /// Run the main stream processing loop.
    /// to allow for gRPC reconnection without full restart.
    async fn run_stream_loop(
        &mut self,
        client: &mut SidecarTransportClient<Channel>,
    ) -> Result<()> {
        let (tx, rx) = tokio::sync::mpsc::channel::<Event>(EVENT_CHANNEL_BUFFER_SIZE);

        let response = time::timeout(
            Duration::from_secs(GRPC_CONNECT_TIMEOUT_SECS),
            client.stream_events(ReceiverStream::new(rx)),
        )
        .await
        .context("timeout establishing event stream")?
        .context("failed to establish event stream")?;

        let mut ack_stream = response.into_inner();

        // Create watch channel to signal stream death
        let (stream_dead_tx, stream_dead_rx) = watch::channel(false);

        let mut event_stream = EventStream::new(tx, stream_dead_rx.clone());

        // Spawn ack handler with stream death signaling
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

            // Signal that the stream is dead
            let _ = stream_dead_tx.send(true);

            // Wake up all pending waiters with failure
            let mut pending = pending_acks.lock().await;
            for (_, event) in pending.drain() {
                *event.result.lock().await = Some(false);
                event.notify.notify_one();
                debug!(
                    "Notified pending event {} (id={}) of stream death",
                    event.event_type, event.event_id
                );
            }

            info!("Ack handler exited, stream marked as dead");
        });

        // First, catch up on any missed blocks
        self.catch_up_missed_blocks(&mut event_stream, client)
            .await
            .context("failed to catch up on missed blocks")?;

        // Subscribe to new blocks with timeout
        let subscription = time::timeout(
            Duration::from_secs(SUBSCRIPTION_SETUP_TIMEOUT_SECS),
            async { self.provider.subscribe_full_blocks().full() },
        )
        .await
        .context("timeout setting up block subscription")?;

        let mut block_stream = time::timeout(
            Duration::from_secs(SUBSCRIPTION_SETUP_TIMEOUT_SECS),
            subscription.into_stream(),
        )
        .await
        .context("timeout converting subscription to stream")?
        .context("failed to convert subscription to stream")?;

        info!("Started block subscription with gRPC streaming");

        while let Some(block_result) = block_stream.next().await {
            // Check if stream died before processing
            if event_stream.is_stream_dead() {
                warn!("Detected dead stream before processing block, triggering reconnect");
                ack_handle.abort();
                return Err(anyhow!("gRPC stream died, reconnecting"));
            }

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
                            client,
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

            if let Err(e) = self.process_block(&mut event_stream, client, &block).await {
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
        // Check stream health before starting catch-up
        if stream.is_stream_dead() {
            return Err(anyhow!("stream is dead, cannot catch up on missed blocks"));
        }

        // Get the current head block with timeout
        let current_block = time::timeout(
            Duration::from_secs(RPC_CALL_TIMEOUT_SECS),
            self.provider.get_block_number(),
        )
        .await
        .context("timeout getting current block number")?
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

        // Fetch the block to get proper timestamp and hash with timeout
        let initial_block = time::timeout(
            Duration::from_secs(BLOCK_FETCH_TIMEOUT_SECS),
            self.provider
                .get_block_by_number(initial_commit_block.into())
                .full(),
        )
        .await
        .context("timeout fetching initial commit block")?
        .context("failed to fetch initial commit block")?
        .ok_or_else(|| anyhow!("initial commit block {initial_commit_block} not found"))?;

        // Send CommitHead with no transactions (we're just syncing state with sidecar)
        self.send_commit_head_with_retry(
            stream,
            initial_commit_block,
            None,
            0,
            initial_block.header.timestamp,
            initial_block.header.hash,
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

        // Check stream health before filling
        if stream.is_stream_dead() {
            return Err(anyhow!("stream is dead, cannot fill missing blocks"));
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
            // Check stream health before each block
            if stream.is_stream_dead() {
                warn!("Stream died during block range processing at block {block_num}");
                return Err(anyhow!("stream died during block range processing"));
            }

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

                // If stream is dead, stop processing the range
                if stream.is_stream_dead() {
                    warn!("Stream died, aborting block range processing");
                    return Err(anyhow!("stream died during block range processing"));
                }
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
        // Fetch block with timeout
        let block_result = time::timeout(
            Duration::from_secs(BLOCK_FETCH_TIMEOUT_SECS),
            self.provider.get_block_by_number(block_num.into()).full(),
        )
        .await
        .context(format!("timeout fetching block {block_num}"))?;

        match block_result {
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

        // Check stream health at the start
        if stream.is_stream_dead() {
            return Err(anyhow!(
                "stream is dead, cannot process block {block_number}"
            ));
        }

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
                debug!("NewIteration accepted for block {block_number}");
                true
            }
            Err(e) => {
                // If stream is dead, propagate the error immediately
                if stream.is_stream_dead() {
                    return Err(anyhow!(
                        "stream died while sending NewIteration for block {block_number}: {e}"
                    ));
                }
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
                // Check stream health before each transaction
                if stream.is_stream_dead() {
                    warn!("Stream died while sending transactions for block {block_number}");
                    return Err(anyhow!(
                        "stream died while sending transactions for block {block_number}"
                    ));
                }

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
                        // If stream is dead, propagate immediately
                        if stream.is_stream_dead() {
                            return Err(anyhow!(
                                "stream died while sending tx {index} for block {block_number}: {e}"
                            ));
                        }
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
                info!(
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
        // Check stream health before commit
        if stream.is_stream_dead() {
            return Err(anyhow!(
                "stream died before sending CommitHead for block {block_number}"
            ));
        }

        info!("Step 3/3: Sending CommitHead event for block {block_number}");
        self.send_commit_head_with_retry(
            stream,
            block_number,
            last_successful_tx_hash,
            successful_tx_count,
            block.header.timestamp,
            block.header.hash,
            block.header.parent_beacon_block_root,
        )
        .await?;

        // STEP 4: Mark block as committed ONLY after CommitHead succeeds
        self.last_committed_block = Some(block_number);

        info!(
            "Block {block_number} committed successfully ({successful_tx_count}/{} txs)",
            transactions.len()
        );

        // STEP 5: Queue transaction result comparison (non-blocking)
        if self.query_results
            && !successful_tx_hashes.is_empty()
            && let Some(ref comparison_manager) = self.comparison_manager
        {
            let request = ComparisonRequest {
                block_number,
                tx_hashes: successful_tx_hashes,
                client: client.clone(),
                provider: self.provider.clone(),
                enqueued_at: time::Instant::now(),
            };

            // Enqueue with backpressure to avoid dropping comparisons.
            match comparison_manager.enqueue(request).await {
                Ok(()) => debug!("Queued comparison for block {block_number}"),
                Err(e) => {
                    warn!(
                        "Comparison manager unavailable, skipping comparison for block {block_number}: {e}"
                    );
                }
            }
        }

        Ok(())
    }

    /// Query and compare transaction results between provider and sidecar
    async fn compare_transaction_results(
        mut client: SidecarTransportClient<Channel>,
        provider: Arc<RootProvider>,
        block_number: u64,
        tx_hashes: Vec<B256>,
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

        // Query sidecar for results with timeout
        let sidecar_response = time::timeout(
            Duration::from_secs(GRPC_GET_TRANSACTIONS_TIMEOUT_SECS),
            client.get_transactions(GetTransactionsRequest { tx_execution_ids }),
        )
        .await
        .context("timeout querying sidecar for transaction results")?
        .context("failed to query sidecar for transaction results")?
        .into_inner();

        // Log and build a map of tx_hash -> sidecar result
        info!(
            "Received {} transaction results from sidecar for block {block_number}",
            sidecar_response.results.len()
        );

        // Print each sidecar result immediately
        for result in &sidecar_response.results {
            let tx_hash = result.tx_execution_id.as_ref().map_or_else(
                || "unknown".to_string(),
                |id| format!("0x{}", bytes_to_hex(&id.tx_hash)),
            );
            let status_str = result_status_to_string(result.status);
            info!(
                "Sidecar result for {tx_hash}: status={status_str}, gas_used={}, error={}",
                result.gas_used,
                if result.error.is_empty() {
                    "none"
                } else {
                    &result.error
                }
            );
        }

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
            for not_found in &sidecar_response.not_found {
                let tx_hash = format!("0x{}", bytes_to_hex(not_found));
                warn!("  Not found: {tx_hash}");
            }
        }

        // Query provider receipts in parallel
        info!(
            "Fetching {} transaction receipts from provider for block {block_number}",
            tx_hashes.len()
        );

        let receipt_futures: Vec<_> = tx_hashes
            .iter()
            .map(|tx_hash| {
                let provider = provider.clone();
                let tx_hash = *tx_hash;
                async move {
                    let result = time::timeout(
                        Duration::from_secs(GET_TX_RECEIPT_TIMEOUT_SECS),
                        provider.get_transaction_receipt(tx_hash),
                    )
                    .await;
                    (tx_hash, result)
                }
            })
            .collect();

        let receipt_results = futures::future::join_all(receipt_futures).await;

        info!(
            "Fetched {} transaction receipts from provider for block {block_number}",
            receipt_results.len()
        );

        let mut mismatches = 0;
        let mut matches = 0;

        // Compare results
        for (tx_hash, receipt_result) in receipt_results {
            let receipt = match receipt_result {
                Ok(Ok(Some(r))) => r,
                Ok(Ok(None)) => {
                    warn!("Transaction receipt not found from provider for {tx_hash}");
                    continue;
                }
                Ok(Err(e)) => {
                    warn!("Failed to get transaction receipt for {tx_hash}: {e}");
                    continue;
                }
                Err(_) => {
                    warn!(
                        "Timeout getting transaction receipt for {tx_hash} (exceeded {}s)",
                        GET_TX_RECEIPT_TIMEOUT_SECS
                    );
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
                info!(
                    "TX {tx_hash}: MATCH (status={provider_success}, gas_used={provider_gas_used})"
                );
            } else {
                let sidecar_status_str = result_status_to_string(sidecar_result.status);
                if sidecar_status_str == "ASSERTION_FAILED" {
                    warn!(
                        "TX {tx_hash}: x ASSERTION FAILED - \
                     provider(status={provider_success}, gas={provider_gas_used}) vs \
                     sidecar(status={sidecar_status_str}, gas={sidecar_gas_used})"
                    );
                } else {
                    mismatches += 1;
                    warn!(
                        "TX {tx_hash}: x MISMATCH - \
                     provider(status={provider_success}, gas={provider_gas_used}) vs \
                     sidecar(status={sidecar_status_str}, gas={sidecar_gas_used})"
                    );
                }

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
            "Block {block_number} result comparison complete: {matches} matches, {mismatches} x mismatches (total: {} txs)",
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
        // Parent block hash for EIP-2935, as we apply it for every iteration
        let parent_block_hash = Some(block.header.parent_hash.to_vec());
        // Parent beacon block root for EIP-4788
        let parent_beacon_block_root = block.header.parent_beacon_block_root.map(|h| h.to_vec());

        stream
            .send_event_with_retry(
                |event_id| {
                    Event {
                        event_id,
                        event: Some(EventVariant::NewIteration(NewIteration {
                            iteration_id: DEFAULT_ITERATION_ID,
                            block_env: Some(block_env.clone()),
                            parent_block_hash: parent_block_hash.clone(),
                            parent_beacon_block_root: parent_beacon_block_root.clone(),
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
        block_hash: B256,
        parent_beacon_block_root: Option<B256>,
    ) -> Result<()> {
        let block_hash_bytes = block_hash.to_vec();
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

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_provider::ProviderBuilder;
    use alloy_transport::mock::Asserter;
    use tokio::time::Duration;

    fn test_provider() -> Arc<RootProvider> {
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        Arc::new(provider.root().clone())
    }

    fn test_client() -> SidecarTransportClient<Channel> {
        let channel = Channel::from_static("http://127.0.0.1:50051").connect_lazy();
        SidecarTransportClient::new(channel)
    }

    fn make_request(block_number: u64, enqueued_at: time::Instant) -> ComparisonRequest {
        ComparisonRequest {
            block_number,
            tx_hashes: Vec::new(),
            client: test_client(),
            provider: test_provider(),
            enqueued_at,
        }
    }

    #[tokio::test]
    async fn queues_and_drains_backlog() {
        let (tx, mut rx) = mpsc::channel::<ComparisonRequest>(1);
        let manager = Arc::new(ComparisonManager::new(tx, 4));
        tokio::spawn(manager.clone().run_backlog_drainer());

        let now = time::Instant::now();
        manager.enqueue(make_request(1, now)).await.unwrap();
        manager.enqueue(make_request(2, now)).await.unwrap();

        let first = rx.recv().await.expect("first request");
        assert_eq!(first.block_number, 1);

        let second = rx.recv().await.expect("second request");
        assert_eq!(second.block_number, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn drops_stale_request_on_enqueue() {
        let (tx, mut rx) = mpsc::channel::<ComparisonRequest>(1);
        let manager = Arc::new(ComparisonManager::new(tx, 4));
        tokio::spawn(manager.clone().run_backlog_drainer());

        let enqueued_at = time::Instant::now();
        time::advance(Duration::from_secs(3)).await;
        manager.enqueue(make_request(1, enqueued_at)).await.unwrap();

        assert!(rx.try_recv().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn drops_stale_request_while_waiting_to_send() {
        let (tx, mut rx) = mpsc::channel::<ComparisonRequest>(1);
        let manager = Arc::new(ComparisonManager::new(tx, 4));
        tokio::spawn(manager.clone().run_backlog_drainer());

        let now = time::Instant::now();
        manager.enqueue(make_request(1, now)).await.unwrap();

        manager.enqueue(make_request(2, now)).await.unwrap();
        tokio::task::yield_now().await;

        time::advance(Duration::from_secs(3)).await;

        let first = rx.recv().await.expect("first request");
        assert_eq!(first.block_number, 1);

        assert!(rx.try_recv().is_err());
    }
}
