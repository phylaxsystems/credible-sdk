//! Core orchestration loop that keeps Redis in sync with the execution client.
//!
//! The worker bootstraps from the last persisted block, catches up to head via
//! RPC, and then tails new blocks from the `newHeads` subscription. Each block
//! is traced with the pre-state tracer and written into Redis.

use alloy::{
    consensus::{
        Transaction,
        TxType,
    },
    primitives::{
        B256,
        Bytes,
        TxHash,
    },
    rpc::types::Header,
    signers::Either,
};
use alloy_provider::{
    Provider,
    RootProvider,
};
use anyhow::{
    Context,
    Result,
    anyhow,
};
use futures::StreamExt;
use reqwest::Client;
use revm::context::{
    TxEnv,
    transaction::AccessListItem,
    tx::{
        TxEnvBuildError,
        TxEnvBuilder,
    },
};
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::json;
use std::{
    sync::Arc,
    time::Duration,
};
use tokio::time;
use tracing::{
    debug,
    error,
    info,
    warn,
};

const SUBSCRIPTION_RETRY_DELAY_SECS: u64 = 5;
const MAX_RETRY_DELAY_SECS: u64 = 300;
const POOL_IDLE_TIMEOUT_SECS: u64 = 90;
const POOL_MAX_IDLE_PER_HOST: usize = 100;
const CONNECT_TIMEOUT_MS: u64 = 500;
const TCP_KEEPALIVE_SECS: u64 = 60;

// Transaction retry configuration
const TX_MAX_RETRIES: u32 = 5;
const TX_INITIAL_RETRY_DELAY_MS: u64 = 10;
const TX_MAX_RETRY_DELAY_MS: u64 = 250;

/// Wrapper for serializing `TxEnv` with the transaction hash
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
struct TransactionPayload {
    tx_env: TxEnv,
    tx_execution_id: TxExecutionId,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct TxExecutionId {
    pub block_number: u64,
    pub iteration_id: u64,
    pub tx_hash: TxHash,
}

#[derive(Deserialize)]
struct Response {
    error: Option<ResponseError>,
}

#[derive(Deserialize)]
struct ResponseError {
    code: i32,
    message: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Clone)]
struct TransactionResultResponse {
    tx_execution_id: TxExecutionId,
    status: String,
    gas_used: Option<u64>,
    error: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct GetTransactionsResponse {
    jsonrpc: String,
    result: Option<GetTransactionsResult>,
    error: Option<JsonRpcErrorResponse>,
    id: Option<serde_json::Value>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Default)]
struct GetTransactionsResult {
    results: Vec<TransactionResultResponse>,
    not_found: Vec<TxExecutionId>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcErrorResponse {
    code: i32,
    message: String,
}

/// Coordinates block ingestion, tracing, and persistence.
pub struct Listener {
    provider: Arc<RootProvider>,
    sidecar_client: Client,
    sidecar_url: String,
    /// Tracks the last block whose block env was successfully sent and accepted
    last_block_env_sent: Option<u64>,
    starting_block: Option<u64>,
    /// Flag to enable/disable transaction result querying
    query_results: bool,
}

impl Listener {
    /// Build a worker that shares the provider/Redis client across async tasks.
    pub fn new(
        provider: Arc<RootProvider>,
        sidecar_url: &str,
        request_timeout_seconds: u64,
        starting_block: Option<u64>,
    ) -> Self {
        Self {
            provider,
            sidecar_client: Client::builder()
                .pool_idle_timeout(Duration::from_secs(POOL_IDLE_TIMEOUT_SECS))
                .pool_max_idle_per_host(POOL_MAX_IDLE_PER_HOST)
                .timeout(Duration::from_secs(request_timeout_seconds))
                .connect_timeout(Duration::from_millis(CONNECT_TIMEOUT_MS))
                .tcp_keepalive(Duration::from_secs(TCP_KEEPALIVE_SECS))
                .build()
                .expect("failed to create sidecar HTTP client"),
            sidecar_url: sidecar_url.to_string(),
            last_block_env_sent: None,
            starting_block,
            query_results: false, // Disabled by default
        }
    }

    /// Enable transaction result querying and comparison
    pub fn with_result_querying(mut self, enabled: bool) -> Self {
        self.query_results = enabled;
        self
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
        }
    }

    /// Catch up on any missed blocks by fetching them via RPC
    async fn catch_up_missed_blocks(&mut self) -> Result<()> {
        // Get the current head block
        let current_block = self
            .provider
            .get_block_number()
            .await
            .context("failed to get current block number")?;

        if let Some(last_block_env) = self.last_block_env_sent {
            // If we've sent block envs before and there's a gap, catch up
            if current_block > last_block_env {
                let missed_count = current_block - last_block_env;
                info!(
                    "Catching up on {missed_count} missed blocks (from {} to {current_block})",
                    last_block_env + 1,
                );

                self.process_and_handle_failures(last_block_env + 1, current_block, "Catch-up")
                    .await?;
            } else {
                debug!("No missed blocks to catch up on");
            }
        } else if let Some(start_block) = self.starting_block {
            // First time running with a specified starting block
            if start_block > current_block {
                warn!(
                    "Starting block {start_block} is ahead of current head {current_block}, will wait for blocks",
                );
                self.last_block_env_sent = Some(start_block - 1);
            } else {
                info!(
                    "Starting from block {start_block} and catching up to current head {current_block} ({} blocks)",
                    current_block - start_block + 1
                );

                self.process_and_handle_failures(start_block, current_block, "Initial catch-up")
                    .await?;
            }
        } else {
            // First time running, start from the current block
            info!("Starting from current block {current_block}");
        }

        Ok(())
    }

    /// Process a range of blocks and handle the case where all fail (sidecar down)
    async fn process_and_handle_failures(
        &mut self,
        start: u64,
        end: u64,
        operation_name: &str,
    ) -> Result<()> {
        let initial_last_sent = self.last_block_env_sent;

        self.process_block_range(start, end).await?;

        // If the sidecar was down and no blocks succeeded, skip to the end block
        if self.last_block_env_sent == initial_last_sent {
            warn!("{operation_name} failed (sidecar likely down), will skip to block {end}");
            self.last_block_env_sent = Some(end);
        } else {
            info!(
                "{operation_name} complete, now at block {}",
                self.last_block_env_sent.unwrap_or(end)
            );
        }

        Ok(())
    }

    /// Fetch a specific block by number and process it
    async fn fetch_and_process_block(&mut self, block_num: u64) -> Result<()> {
        match self
            .provider
            .get_block_by_number(block_num.into())
            .full()
            .await
        {
            Ok(Some(block)) => {
                info!("Processing block {block_num}");
                self.process_block(&block).await
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

    /// Fill in missing blocks when WebSocket skips blocks
    async fn fill_missing_blocks(&mut self, last_block: u64, current_block: u64) -> Result<()> {
        if current_block <= last_block + 1 {
            return Ok(());
        }

        let missing_start = last_block + 1;
        let missing_end = current_block - 1;

        warn!(
            "Detected missing blocks in stream: {missing_start} to {missing_end} ({} blocks)",
            missing_end - missing_start + 1
        );

        self.process_and_handle_failures(missing_start, missing_end, "Gap fill")
            .await?;

        Ok(())
    }

    async fn process_block_range(&mut self, start: u64, end: u64) -> Result<()> {
        for block_num in start..=end {
            // Don't propagate errors during batch processing
            // If sidecar is down, we skip failed blocks and continue
            // This prevents getting stuck in a retry loop
            if let Err(e) = self.fetch_and_process_block(block_num).await {
                warn!(
                    error = ?e,
                    "Failed to process block {block_num} during catch-up, continuing to next block"
                );
            }
        }
        Ok(())
    }

    /// Follow the `newHeads` stream and process new blocks in order, tolerating
    /// duplicate/stale headers after it reconnects.
    async fn stream_blocks(&mut self) -> Result<()> {
        // First, catch up on any missed blocks
        self.catch_up_missed_blocks()
            .await
            .context("failed to catch up on missed blocks")?;

        let subscription = self.provider.subscribe_full_blocks().full();
        let mut stream = subscription.into_stream().await?;

        info!("Started block subscription");

        while let Some(Ok(block)) = stream.next().await {
            let Header {
                hash: _, ref inner, ..
            } = block.header;
            let block_number = inner.number;

            // Check for missing blocks in the stream
            if let Some(last_block_env) = self.last_block_env_sent {
                if block_number > last_block_env + 1 {
                    // We've skipped blocks, fill them in
                    warn!(
                        "Block skip detected in stream: expected {}, got {block_number}",
                        last_block_env + 1,
                    );

                    if let Err(e) = self.fill_missing_blocks(last_block_env, block_number).await {
                        error!(error = ?e, "Failed to fill missing blocks");
                        // Return error to trigger reconnection
                        return Err(e);
                    }
                } else if block_number <= last_block_env {
                    // Skip duplicate/stale blocks
                    debug!("Skipping already processed block {block_number}");
                    continue;
                }
            }

            info!("Processing block {block_number}");

            if let Err(e) = self.process_block(&block).await {
                error!(error = ?e, "Failed to process block {block_number}");
                // Return error to trigger reconnection and catch-up
                return Err(e);
            }
        }

        Err(anyhow!("block subscription completed"))
    }

    /// Process a single block with strict sequencing:
    /// 1. Only send transactions if the previous block env was accepted
    /// 2. Send transactions for the current block (with retries)
    /// 3. Send block env for the current block (which allows the next block's txs)
    /// 4. Mark block env as sent only after it succeeds
    /// 5. If the sidecar is down (send fails after retries), continue to the next block without marking as sent
    /// 6. Optionally query and compare transaction results if enabled
    async fn process_block(&mut self, block: &alloy::rpc::types::Block) -> Result<()> {
        let block_number = block.header.number;

        // Skip already processed blocks (prevent duplicates)
        if let Some(last_block_env) = self.last_block_env_sent
            && block_number <= last_block_env
        {
            debug!("Skipping already processed block {block_number}");
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

        // Collect transaction hashes for later querying
        let mut tx_hashes: Vec<TxHash> = Vec::new();

        // STEP 1: Send transactions ONLY if previous block env was accepted
        // (First block won't send transactions as last_block_env_sent will be None)
        if self.last_block_env_sent.is_some() {
            for (index, tx) in transactions.iter().enumerate() {
                let tx_hash = *tx.inner.hash();
                tx_hashes.push(tx_hash);

                // Retry transaction sending with exponential backoff
                if let Err(e) = self
                    .send_transaction_with_retry(tx, index as u64, block_number)
                    .await
                {
                    warn!(
                        error = ?e,
                        "Failed to send transaction {index} in block {block_number} after all retries, will continue to block env",
                    );
                    // Transaction failed after all retries - still try to send block env
                    // so we can move to the next block
                    break;
                }
            }
            debug!("Successfully sent transactions for block {block_number}");
        } else {
            debug!(
                "Skipping transactions for block {block_number} (first block or no prior block env)"
            );
        }

        // STEP 2: Send block environment
        // This must succeed for the next block's transactions to be sent
        if let Err(e) = self.send_block_env(block, transactions.len()).await {
            warn!(
                error = ?e,
                "Failed to send block env for block {block_number} (sidecar may be down), will continue to next block",
            );
            // Sidecar is down - don't mark as sent, continue to the next block
            // When sidecar comes back up, we'll successfully send a later block env
            return Ok(());
        }

        // STEP 3: Mark block env as sent ONLY after it succeeds
        // This ensures strict sequential block env numbering for blocks that succeed
        // and prevents duplicates on reconnection
        self.last_block_env_sent = Some(block_number);

        info!("Successfully processed block {block_number} (env sent, ready for next block's txs)");

        if self.query_results
            && !tx_hashes.is_empty()
            && let Err(e) = self
                .compare_transaction_status(block_number, tx_hashes)
                .await
        {
            warn!(error = ?e, "Failed to compare transaction results for block {block_number}");
        }

        Ok(())
    }

    /// Send a transaction with retry logic and exponential backoff
    async fn send_transaction_with_retry(
        &self,
        tx: &alloy::rpc::types::Transaction,
        tx_index: u64,
        block_number: u64,
    ) -> Result<()> {
        let mut retry_delay = Duration::from_millis(TX_INITIAL_RETRY_DELAY_MS);
        let max_delay = Duration::from_millis(TX_MAX_RETRY_DELAY_MS);

        for attempt in 0..=TX_MAX_RETRIES {
            match self.send_transaction(tx, tx_index, block_number).await {
                Ok(()) => {
                    if attempt > 0 {
                        info!(
                            "Successfully sent transaction {tx_index} in block {block_number} after {attempt} retries"
                        );
                    }
                    return Ok(());
                }
                Err(e) => {
                    if attempt < TX_MAX_RETRIES {
                        warn!(
                            error = ?e,
                            "Failed to send transaction {tx_index} in block {block_number} (attempt {}/{TX_MAX_RETRIES}), retrying in {}ms",
                            attempt + 1,
                            retry_delay.as_millis()
                        );
                        time::sleep(retry_delay).await;
                        // Exponential backoff with cap
                        retry_delay = std::cmp::min(retry_delay * 2, max_delay);
                    } else {
                        // Final attempt failed
                        error!(
                            error = ?e,
                            "Failed to send transaction {tx_index} in block {block_number} after {TX_MAX_RETRIES} retries, giving up"
                        );
                        return Err(e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Send block environment to the sidecar
    async fn send_block_env(
        &self,
        block: &alloy::rpc::types::Block,
        n_transactions: usize,
    ) -> Result<()> {
        // Get the last transaction hash if transactions exist
        let last_tx_hash = match &block.transactions {
            alloy::rpc::types::BlockTransactions::Full(txs) if !txs.is_empty() => {
                Some(format!("{:#x}", txs.last().unwrap().inner.hash()))
            }
            alloy::rpc::types::BlockTransactions::Hashes(hashes) if !hashes.is_empty() => {
                Some(format!("{:#x}", hashes.last().unwrap()))
            }
            _ => None,
        };

        let block_env_payload = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "block_env": {
                    "number": block.header.number,
                    "beneficiary": format!("{:#x}", block.header.beneficiary),
                    "timestamp": block.header.timestamp,
                    "gas_limit": block.header.gas_limit,
                    "basefee": block.header.base_fee_per_gas,
                    "difficulty": block.header.difficulty,
                    "prevrandao": format!("{:#x}", block.header.mix_hash),
                    "blob_excess_gas_and_price": json!({
                        "excess_blob_gas": 0,
                        "blob_gasprice": 1
                    }),
                },
                "n_transactions": n_transactions,
                "selected_iteration_id": 0,
                "last_tx_hash": last_tx_hash,
            },
            "id": 1
        });

        debug!("Sending block env for block {}", block.header.number);

        let response = self
            .sidecar_client
            .post(format!("{}/tx", self.sidecar_url))
            .json(&block_env_payload)
            .send()
            .await
            .context("failed to send block env request to sidecar")?;

        let response_status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "unknown error".to_string());
        if !response_status.is_success() {
            return Err(anyhow!("sidecar returned error {response_status}: {body}",));
        }

        if let Ok(error_response) = serde_json::from_str::<Response>(&body)
            && let Some(json_rpc_error) = error_response.error
        {
            return Err(anyhow!(
                "sidecar returned JSON-RPC error (code: {}): {}",
                json_rpc_error.code,
                json_rpc_error.message
            ));
        }

        debug!(
            "Successfully sent block env for block {}",
            block.header.number
        );
        Ok(())
    }

    /// Convert an Alloy transaction to REVM `TxEnv`
    fn to_revm_tx_env(tx: &alloy::rpc::types::Transaction) -> Result<TxEnv, TxEnvBuildError> {
        let mut tx_env = TxEnvBuilder::new();

        // Set caller (from address)
        tx_env = tx_env.caller(tx.inner.signer());

        // Set gas limit
        tx_env = tx_env.gas_limit(tx.inner.gas_limit());

        // Set gas price
        tx_env = tx_env.gas_price(tx.inner.gas_price().unwrap_or_default());

        // Set transaction kind (Create for contract deployment, Call for regular tx)
        let kind = if let Some(to) = tx.to() {
            revm::primitives::TxKind::Call(to)
        } else {
            revm::primitives::TxKind::Create
        };
        tx_env = tx_env.kind(kind);

        // Set value
        tx_env = tx_env.value(tx.value());

        // Set input data
        tx_env = tx_env.data(Bytes::from(tx.input().to_vec()));

        // Set nonce
        tx_env = tx_env.nonce(tx.nonce());

        // Set chain ID
        tx_env = tx_env.chain_id(tx.chain_id());

        // Set access list if present
        if let Some(access_list) = tx.access_list() {
            tx_env = tx_env.access_list(
                access_list
                    .0
                    .iter()
                    .map(|item| {
                        AccessListItem {
                            address: item.address,
                            storage_keys: item
                                .storage_keys
                                .iter()
                                .map(|key| B256::from(*key))
                                .collect(),
                        }
                    })
                    .collect::<Vec<_>>()
                    .into(),
            ); // Convert Vec<AccessListItem> to AccessList
        }

        // Handle different transaction types
        match tx.inner.tx_type() {
            // Legacy transaction
            TxType::Legacy |
            // EIP-2930 - Access list transaction
            TxType::Eip2930
            => {
                // gas_price and access_list are already set above
            }
            // EIP-1559 - Dynamic fee transaction
            TxType::Eip1559 => {
                let max_fee = tx.inner.max_fee_per_gas();
                let max_priority = tx.inner.max_priority_fee_per_gas();

                tx_env = tx_env.gas_price(max_fee);
                tx_env = tx_env.gas_priority_fee(max_priority);
            }
            // EIP-4844 - Blob transaction
            TxType::Eip4844 => {
                let max_fee = tx.inner.max_fee_per_gas();
                let max_priority = tx.inner.max_priority_fee_per_gas();

                tx_env = tx_env.gas_price(max_fee);
                tx_env = tx_env.gas_priority_fee(max_priority);

                // Set blob-related fields
                if let Some(blob_versioned_hashes) = tx.blob_versioned_hashes() {
                    tx_env = tx_env.blob_hashes(blob_versioned_hashes
                        .iter()
                        .map(|hash| B256::from(*hash))
                        .collect());
                }

                if let Some(max_fee_per_blob_gas) = tx.max_fee_per_blob_gas() {
                    tx_env = tx_env.max_fee_per_blob_gas(max_fee_per_blob_gas);
                }
            }
            // EIP-7702 - Account delegation transaction
            TxType::Eip7702 => {
                let max_fee = tx.inner.max_fee_per_gas();
                let max_priority = tx.inner.max_priority_fee_per_gas();

                tx_env = tx_env.gas_price(max_fee);
                tx_env = tx_env.gas_priority_fee(max_priority);

                if let Some(auth_list) = tx.authorization_list() {
                    tx_env = tx_env.authorization_list(auth_list
                        .iter()
                        .map(|auth| Either::Left(auth.clone()))
                        .collect());
                }
            }
        }

        tx_env.build()
    }

    /// Send a transaction to the sidecar
    async fn send_transaction(
        &self,
        tx: &alloy::rpc::types::Transaction,
        tx_index: u64,
        block_number: u64,
    ) -> Result<()> {
        // Convert Alloy transaction to REVM TxEnv
        let tx_env = Self::to_revm_tx_env(tx).map_err(|e| anyhow!("{e:?}"))?;

        // Create the transaction payload with REVM TxEnv
        let transaction_payload = TransactionPayload {
            tx_env,
            tx_execution_id: TxExecutionId {
                block_number,
                iteration_id: 0,
                tx_hash: *tx.inner.hash(),
            },
        };

        let tx_payload = json!({
            "jsonrpc": "2.0",
            "method": "sendTransactions",
            "params": {
                "transactions": [transaction_payload]
            }
        });

        debug!(
            "Sending transaction {tx_index} with hash {:#x}",
            tx.inner.hash()
        );

        let response = self
            .sidecar_client
            .post(format!("{}/tx", self.sidecar_url))
            .json(&tx_payload)
            .send()
            .await
            .context("failed to send transaction request to sidecar")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unknown error".to_string());
            return Err(anyhow!(
                "sidecar returned error {status} for transaction: {body}",
            ));
        }

        debug!(
            "Successfully sent transaction {tx_index} with hash {:#x}",
            tx.inner.hash()
        );
        Ok(())
    }

    async fn query_transactions_batch(
        &self,
        tx_hashes: &[TxHash],
        block_number: u64,
    ) -> Result<GetTransactionsResponse> {
        let tx_execution_ids: Vec<_> = tx_hashes
            .iter()
            .map(|tx_hash| {
                json!({
                    "block_number": block_number,
                    "iteration_id": 0,
                    "tx_hash": format!("{tx_hash:#x}")
                })
            })
            .collect();

        let query_payload = json!({
            "jsonrpc": "2.0",
            "method": "getTransactions",
            "params": tx_execution_ids,
            "id": 1
        });

        debug!("Querying batch of {} transaction results", tx_hashes.len());

        let response = self
            .sidecar_client
            .post(format!("{}/tx", self.sidecar_url))
            .json(&query_payload)
            .timeout(Duration::from_secs(25))
            .send()
            .await
            .context("failed to send batch query request to sidecar")?;

        let response_status = response.status();
        if !response_status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unknown error".to_string());
            return Err(anyhow!("sidecar returned error {response_status}: {body}"));
        }

        let body = response
            .text()
            .await
            .context("failed to read response body")?;

        let query_response: GetTransactionsResponse =
            serde_json::from_str(&body).context("failed to parse batch query response")?;

        if let Some(error) = query_response.error {
            return Err(anyhow!(
                "sidecar returned JSON-RPC error (code: {}): {}",
                error.code,
                error.message
            ));
        }

        Ok(query_response)
    }

    /// Simple comparison of transaction results between blockchain and sidecar
    async fn compare_transaction_status(
        &self,
        block_number: u64,
        tx_hashes: Vec<TxHash>,
    ) -> Result<()> {
        if tx_hashes.is_empty() {
            return Ok(());
        }

        // Query all transactions in a single batch
        let batch_response = match self
            .query_transactions_batch(&tx_hashes, block_number)
            .await
        {
            Ok(response) => response,
            Err(e) => {
                warn!(
                    "Failed to query sidecar for transaction batch for block {block_number}: {e:?}"
                );
                return Ok(());
            }
        };

        // Extract results and create a lookup map
        let result = batch_response.result.unwrap_or_default();
        let sidecar_results: std::collections::HashMap<TxHash, TransactionResultResponse> = result
            .results
            .into_iter()
            .map(|r| (r.tx_execution_id.tx_hash, r))
            .collect();

        // Compare each transaction
        for tx_hash in tx_hashes {
            // Get blockchain status from the receipt
            let blockchain_success = match self.provider.get_transaction_receipt(tx_hash).await {
                Ok(Some(receipt)) => receipt.status(),
                Ok(None) => {
                    warn!("No receipt found for tx {tx_hash:#x}");
                    continue;
                }
                Err(e) => {
                    warn!("Failed to get receipt for tx {tx_hash:#x}: {e:?}");
                    continue;
                }
            };

            // Get sidecar status from batch results
            let Some(sidecar_result) = sidecar_results.get(&tx_hash) else {
                warn!("Transaction {tx_hash:#x} not found in sidecar batch response");
                continue;
            };

            let sidecar_success = sidecar_result.status == "success";

            // Compare and log error if mismatch
            if blockchain_success != sidecar_success {
                error!(
                    "TX STATUS MISMATCH {tx_hash:#x}: blockchain={} sidecar={}{}",
                    if blockchain_success {
                        "SUCCESS"
                    } else {
                        "FAILED"
                    },
                    sidecar_result.status,
                    sidecar_result
                        .error
                        .as_ref()
                        .map(|e| format!(" (error: {e})"))
                        .unwrap_or_default()
                );
            }
        }

        Ok(())
    }
}
