//! Core orchestration loop that keeps Redis in sync with the execution client.
//!
//! The worker bootstraps from the last persisted block, catches up to head via
//! RPC, and then tails new blocks from the `newHeads` subscription. Each block
//! is traced with the pre-state tracer and written into Redis.

use alloy::{
    consensus::Transaction,
    rpc::types::{
        BlockNumberOrTag,
        Header,
    },
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
use revm::context_interface::block::{
    calc_blob_gasprice,
    calc_excess_blob_gas,
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
const TARGET_BLOB_GAS_PER_BLOCK: u64 = 393_216; // 3 * 131072 (3 blobs per block target)

/// Coordinates block ingestion, tracing, and persistence.
pub struct Listener {
    provider: Arc<RootProvider>,
    sidecar_client: Client,
    sidecar_url: String,
}

impl Listener {
    /// Build a worker that shares the provider/Redis client across async tasks.
    pub fn new(provider: Arc<RootProvider>, sidecar_url: &str) -> Self {
        Self {
            provider,
            sidecar_client: Client::new(),
            sidecar_url: sidecar_url.to_string(),
        }
    }

    /// We keep retrying the subscription because websocket connections can drop in practice.
    pub async fn run(&mut self) -> Result<()> {
        loop {
            if let Err(err) = self.stream_blocks().await {
                warn!(error = %err, "block subscription ended, retrying");
                time::sleep(Duration::from_secs(SUBSCRIPTION_RETRY_DELAY_SECS)).await;
            }
        }
    }

    /// Follow the `newHeads` stream and process new blocks in order, tolerating
    /// duplicate/stale headers after reconnects.
    async fn stream_blocks(&mut self) -> Result<()> {
        let subscription = self.provider.subscribe_blocks().await?;
        let mut stream = subscription.into_stream();

        info!("Started block subscription");

        while let Some(header) = stream.next().await {
            let Header { hash: _, inner, .. } = header;
            let block_number = inner.number;

            info!("Processing block {}", block_number);

            // Fetch the full block with full transaction details
            let block_id = BlockNumberOrTag::Number(block_number);
            let block = self
                .provider
                .get_block_by_number(block_id)
                .full()
                .await?
                .with_context(|| format!("block {block_number} not found"))?;

            // Extract transactions from the block (already included when using .full())
            let transactions = match &block.transactions {
                alloy::rpc::types::BlockTransactions::Full(txs) => txs.clone(),
                alloy::rpc::types::BlockTransactions::Hashes(_) => {
                    // This shouldn't happen when using .full(), but handle it just in case
                    warn!("Got hashes instead of full transactions despite using .full()");
                    Vec::new()
                }
                alloy::rpc::types::BlockTransactions::Uncle => Vec::new(),
            };

            debug!(
                "Block {} has {} transactions",
                block_number,
                transactions.len()
            );

            // Send block environment to sidecar
            if let Err(e) = self.send_block_env(&block).await {
                error!(
                    "Failed to send block env for block {}: {:?}",
                    block_number, e
                );
                continue; // Skip to next block
            }

            // Send each transaction to sidecar
            for (index, tx) in transactions.iter().enumerate() {
                if let Err(e) = self.send_transaction(tx, index as u64).await {
                    error!(
                        "Failed to send transaction {} in block {}: {:?}",
                        index, block_number, e
                    );
                    // Continue processing other transactions
                }
            }
        }

        Err(anyhow!("block subscription completed"))
    }

    /// Send block environment to the sidecar
    async fn send_block_env(&self, block: &alloy::rpc::types::Block) -> Result<()> {
        // Calculate blob gas price if blob data exists
        let blob_excess_gas_and_price = block.header.excess_blob_gas.and_then(|excess| {
            block.header.blob_gas_used.map(|used| {
                let excess_blob_gas = calc_excess_blob_gas(excess, used, TARGET_BLOB_GAS_PER_BLOCK);
                let blob_gasprice = calc_blob_gasprice(excess_blob_gas, false); // @TODO: is Prague?
                (excess_blob_gas, blob_gasprice)
            })
        });

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
                "number": format!("{:#x}", block.header.number),
                "beneficiary": format!("{:#x}", block.header.beneficiary),
                "timestamp": format!("{:#x}", block.header.timestamp),
                "gas_limit": format!("{:#x}", block.header.gas_limit),
                "basefee": block.header.base_fee_per_gas.map(|fee| format!("{fee:#x}")),
                "difficulty": format!("{:#x}", block.header.difficulty),
                "prevrandao": format!("{:#x}", block.header.mix_hash),
                "blob_excess_gas_and_price": blob_excess_gas_and_price.map(|(excess, price)| json!({
                    "excess_blob_gas": format!("{:#x}", excess),
                    "blob_gasprice": format!("{:#x}", price)
                })),
                "n_transactions": block.transactions.len(),
                "last_tx_hash": last_tx_hash,
            },
            "id": 1
        });

        debug!("Sending block env for block {}", block.header.number);

        let response = self
            .sidecar_client
            .post(format!("{}/tx", self.sidecar_url))
            .json(&block_env_payload)
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .context("failed to send block env request to sidecar")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unknown error".to_string());
            return Err(anyhow!("sidecar returned error {}: {}", status, body));
        }

        debug!(
            "Successfully sent block env for block {}",
            block.header.number
        );
        Ok(())
    }

    /// Send a transaction to the sidecar
    async fn send_transaction(
        &self,
        tx: &alloy::rpc::types::Transaction,
        tx_index: u64,
    ) -> Result<()> {
        let tx_payload = json!({
            "jsonrpc": "2.0",
            "method": "sendTransactions",
            "params": {
                "transactions": [
                    {
                        "txEnv": {
                            "caller": format!("{:#x}", tx.inner.signer()),
                            "gas_limit": tx.inner.gas_limit(),
                            "gas_price": format!("{:#x}", tx.inner.gas_price().unwrap_or_default()),
                            "transact_to": tx.to().map(|addr| format!("{addr:#x}")),
                            "value": format!("{:#x}", tx.value()),
                            "data": format!("0x{}", hex::encode(tx.input())),
                            "nonce": tx.nonce(),
                            "chain_id": tx.chain_id().unwrap(), //@TODO: unwrap
                            "access_list": tx.access_list(),
                        },
                        "hash": format!("{:#x}", tx.inner.hash())
                    }
                ]
            }
        });

        debug!(
            "Sending transaction {} with hash {:#x}",
            tx_index,
            tx.inner.hash()
        );

        let response = self
            .sidecar_client
            .post(format!("{}/tx", self.sidecar_url))
            .json(&tx_payload)
            .timeout(Duration::from_secs(30))
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
                "sidecar returned error {} for transaction: {}",
                status,
                body
            ));
        }

        debug!("Successfully sent transaction {}", tx_index);
        Ok(())
    }
}
