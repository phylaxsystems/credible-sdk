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

/// Wrapper for serializing `TxEnv` with transaction hash
#[derive(Serialize)]
struct TransactionPayload {
    #[serde(rename = "txEnv")]
    tx_env: TxEnv,
    hash: String,
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

/// Coordinates block ingestion, tracing, and persistence.
pub struct Listener {
    provider: Arc<RootProvider>,
    sidecar_client: Client,
    sidecar_url: String,
    skip_txs: bool,
    last_processed_block: Option<u64>,
    starting_block: Option<u64>,
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
            sidecar_client: reqwest::Client::builder()
                .pool_idle_timeout(Duration::from_secs(POOL_IDLE_TIMEOUT_SECS))
                .pool_max_idle_per_host(POOL_MAX_IDLE_PER_HOST)
                .timeout(Duration::from_secs(request_timeout_seconds))
                .connect_timeout(Duration::from_millis(CONNECT_TIMEOUT_MS))
                .tcp_keepalive(Duration::from_secs(TCP_KEEPALIVE_SECS))
                .build()
                .expect("failed to create sidecar HTTP client"),
            sidecar_url: sidecar_url.to_string(),
            // If we set `skip_txs` to true, we enforce sending first the BlockEnv to the sidecar
            skip_txs: true,
            last_processed_block: None,
            starting_block,
        }
    }

    /// We keep retrying the subscription because websocket connections can drop in practice.
    pub async fn run(&mut self) -> Result<()> {
        let mut retry_delay = Duration::from_secs(SUBSCRIPTION_RETRY_DELAY_SECS);
        let max_delay = Duration::from_secs(MAX_RETRY_DELAY_SECS);
        loop {
            if let Err(err) = self.stream_blocks().await {
                self.skip_txs = true;
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

        if let Some(last_processed) = self.last_processed_block {
            // If we've processed blocks before and there's a gap, catch up
            if current_block > last_processed {
                let missed_count = current_block - last_processed;
                info!(
                    "Catching up on {missed_count} missed blocks (from {} to {current_block})",
                    last_processed + 1,
                );

                // Fetch and process each missed block
                self.process_block_range(last_processed + 1, current_block)
                    .await?;

                info!("Catch-up complete, now at block {current_block}");
            } else {
                debug!("No missed blocks to catch up on");
            }
        } else if let Some(start_block) = self.starting_block {
            // First time running with a specified starting block
            if start_block > current_block {
                warn!(
                    "Starting block {start_block} is ahead of current head {current_block}, will wait for blocks",
                );
                self.last_processed_block = Some(start_block - 1);
            } else {
                info!(
                    "Starting from block {start_block} and catching up to current head {current_block} ({} blocks)",
                    current_block - start_block + 1
                );

                // Catch up from starting block to current head
                self.process_block_range(start_block, current_block).await?;

                info!("Initial catch-up complete, now at block {current_block}");
            }
        } else {
            // First time running, start from the current block
            info!("Starting from current block {current_block}");
            self.last_processed_block = Some(current_block);
        }

        Ok(())
    }

    /// Fetch a specific block by number and process it
    async fn fetch_and_process_block(&mut self, block_num: u64) -> Result<()> {
        match self.provider.get_block_by_number(block_num.into()).await {
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

        self.process_block_range(missing_start, missing_end).await?;

        info!("Successfully filled all missing blocks up to {missing_end}",);
        Ok(())
    }

    async fn process_block_range(&mut self, start: u64, end: u64) -> Result<()> {
        for block_num in start..=end {
            self.fetch_and_process_block(block_num).await?;
            self.last_processed_block = Some(block_num);
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
            if let Some(last_processed) = self.last_processed_block {
                if block_number > last_processed + 1 {
                    // We've skipped blocks, fill them in
                    warn!(
                        "Block skip detected in stream: expected {}, got {block_number}",
                        last_processed + 1,
                    );

                    if let Err(e) = self.fill_missing_blocks(last_processed, block_number).await {
                        error!(error = ?e, "Failed to fill missing blocks");
                        // Return error to trigger reconnection
                        return Err(e);
                    }
                } else if block_number <= last_processed {
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

            // Update the last processed block number after successful processing
            self.last_processed_block = Some(block_number);
        }

        Err(anyhow!("block subscription completed"))
    }

    /// Process a single block: extract transactions, send them, and send block env
    async fn process_block(&mut self, block: &alloy::rpc::types::Block) -> Result<()> {
        let block_number = block.header.number;

        // Extract transactions from the block
        let transactions = match &block.transactions {
            alloy::rpc::types::BlockTransactions::Full(txs) => txs.clone(),
            alloy::rpc::types::BlockTransactions::Hashes(_) => {
                Err(anyhow!(
                    "Got hashes instead of full transactions despite using .full()"
                ))?
            }
            alloy::rpc::types::BlockTransactions::Uncle => Vec::new(),
        };

        debug!(
            "Block {block_number} has {} transactions",
            transactions.len()
        );

        // We need to skip sending the transactions if the previous BlockEnv failed / was rejected by the sidecar
        if !self.skip_txs {
            // Send each transaction to sidecar
            for (index, tx) in transactions.iter().enumerate() {
                if let Err(e) = self.send_transaction(tx, index as u64).await {
                    self.skip_txs = true;
                    error!(
                        "error = ?e, Failed to send transaction {index} in block {block_number}",
                    );
                    // Stop processing this block and return an error
                    return Err(e);
                }
            }
        }

        // Send block environment to sidecar
        if let Err(e) = self.send_block_env(block, transactions.len()).await {
            // If there is an error sending the BlockEnv, we skip sending the transactions in the next batch
            self.skip_txs = true;
            error!(
                error = ?e,
                "Failed to send block env for block {block_number}",
            );
            return Err(e);
        }
        self.skip_txs = false;

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
                "n_transactions": n_transactions,
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
    ) -> Result<()> {
        // Convert Alloy transaction to REVM TxEnv
        let tx_env = Self::to_revm_tx_env(tx).map_err(|e| anyhow!("{e:?}"))?;

        // Create the transaction payload with REVM TxEnv
        let transaction_payload = TransactionPayload {
            tx_env,
            hash: format!("{:#x}", tx.inner.hash()),
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
}
