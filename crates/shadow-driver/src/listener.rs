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
}

impl Listener {
    /// Build a worker that shares the provider/Redis client across async tasks.
    pub fn new(
        provider: Arc<RootProvider>,
        sidecar_url: &str,
        request_timeout_seconds: u64,
    ) -> Self {
        Self {
            provider,
            sidecar_client: reqwest::Client::builder()
                .pool_idle_timeout(Duration::from_secs(90))
                .pool_max_idle_per_host(100)
                .timeout(Duration::from_secs(request_timeout_seconds))
                .connect_timeout(Duration::from_millis(500))
                .tcp_keepalive(Duration::from_secs(60))
                .build()
                .expect("failed to create sidecar HTTP client"),
            sidecar_url: sidecar_url.to_string(),
            // If we set `skip_txs` to true, we enforce sending first the BlockEnv to the sidecar
            skip_txs: true,
        }
    }

    /// We keep retrying the subscription because websocket connections can drop in practice.
    pub async fn run(&mut self) -> Result<()> {
        let mut retry_delay = Duration::from_secs(SUBSCRIPTION_RETRY_DELAY_SECS);
        let max_delay = Duration::from_secs(300); // 5 minutes
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

    /// Follow the `newHeads` stream and process new blocks in order, tolerating
    /// duplicate/stale headers after reconnects.
    async fn stream_blocks(&mut self) -> Result<()> {
        let subscription = self.provider.subscribe_full_blocks().full();
        let mut stream = subscription.into_stream().await?;

        info!("Started block subscription");

        while let Some(Ok(block)) = stream.next().await {
            let Header {
                hash: _, ref inner, ..
            } = block.header;
            let block_number = inner.number;

            info!("Processing block {}", block_number);
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

            // We need to skip sending the transactions if the previous BlockEnv failed / was rejected by the sidecar
            if !self.skip_txs {
                // Send each transaction to sidecar
                for (index, tx) in transactions.iter().enumerate() {
                    if let Err(e) = self.send_transaction(tx, index as u64).await {
                        self.skip_txs = true;
                        error!(
                            "Failed to send transaction {} in block {}: {:?}",
                            index, block_number, e
                        );
                        // Stop processing this block
                        break;
                    }
                }
            }

            // Send block environment to sidecar
            if let Err(e) = self.send_block_env(&block, transactions.len()).await {
                // If there is an error sending the BlockEnv, we skip sending the transactions in the next batch
                self.skip_txs = true;
                error!(
                    "Failed to send block env for block {}: {:?}",
                    block_number, e
                );
            } else {
                self.skip_txs = false;
            }
        }

        Err(anyhow!("block subscription completed"))
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
            return Err(anyhow!(
                "sidecar returned error {}: {}",
                response_status,
                body
            ));
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
            "Sending transaction {} with hash {:#x}",
            tx_index,
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
                "sidecar returned error {} for transaction: {}",
                status,
                body
            ));
        }

        debug!(
            "Successfully sent transaction {} with hash {:#x}",
            tx_index,
            tx.inner.hash()
        );
        Ok(())
    }
}
