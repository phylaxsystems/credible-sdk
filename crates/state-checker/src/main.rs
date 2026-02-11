#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::cast_precision_loss)]

use crate::{
    cli::Args,
    state_root::StateRootService,
};
use alloy::{
    primitives::B256,
    rpc::types::BlockNumberOrTag,
};
use alloy_provider::{
    Provider,
    ProviderBuilder,
};
use anyhow::{
    Context,
    Result,
};
use clap::Parser;
use log::error;
use mdbx::{
    Reader,
    StateReader,
    common::CircularBufferConfig,
};
use rust_tracing::trace;
use tracing::info;

mod cli;
mod state_root;

async fn fetch_state_root_from_node<P>(
    provider: &P,
    block_number: u64,
    block_hash: Option<B256>,
) -> Result<(B256, &'static str)>
where
    P: Provider,
{
    if let Some(hash) = block_hash
        && let Some(state_root) = fetch_state_root_by_hash(provider, hash).await?
    {
        return Ok((state_root, "ethereum node (hash)"));
    }

    let state_root = fetch_state_root_by_number(provider, block_number).await?;
    Ok((state_root, "ethereum node (number)"))
}

async fn fetch_state_root_by_number<P>(provider: &P, block_number: u64) -> Result<B256>
where
    P: Provider,
{
    let block = provider
        .get_block_by_number(BlockNumberOrTag::Number(block_number))
        .await
        .context("Failed to call Ethereum node")?
        .with_context(|| format!("Ethereum node returned no block for number {block_number}"))?;

    Ok(block.header.state_root)
}

async fn fetch_state_root_by_hash<P>(provider: &P, block_hash: B256) -> Result<Option<B256>>
where
    P: Provider,
{
    let block = provider
        .get_block_by_hash(block_hash)
        .await
        .context("Failed to call Ethereum node")?;

    Ok(block.map(|block| block.header.state_root))
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the rustls CryptoProvider for HTTPS support
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Install the shared tracing subscriber used across Credible services.
    trace();

    let args = Args::parse();

    let config = CircularBufferConfig {
        buffer_size: args.state_depth,
    };

    let reader = StateReader::new(&args.mdbx_path, config)?;

    let service = StateRootService::new(&reader);

    let (block_number, root) = service.calculate_latest_state_root()?;
    info!(
        "State cache (state_worker) calculated state root: 0x{} for block {block_number}",
        hex::encode(root)
    );

    let block_metadata = reader
        .get_block_metadata(block_number)?
        .expect("No block metadata in Redis");

    let (expected_state_root, expected_source) = if let Some(node_url) = args.rpc_url.as_deref() {
        let provider = ProviderBuilder::new()
            .connect_http(node_url.parse().context("Invalid Ethereum node URL")?);
        let (state_root, source) =
            fetch_state_root_from_node(&provider, block_number, Some(block_metadata.block_hash))
                .await?;
        info!(
            "Ethereum node state root: 0x{} (block {block_number}, hash 0x{})",
            hex::encode(state_root),
            hex::encode(block_metadata.block_hash)
        );
        (state_root, source)
    } else {
        info!(
            "Block metadata state root: 0x{}",
            hex::encode(block_metadata.state_root)
        );
        (block_metadata.state_root, "state store")
    };

    if root == expected_state_root {
        info!("State roots match: 0x{}", hex::encode(root));
    } else {
        error!(
            "State roots do not match: 0x{} != 0x{} (source: {expected_source})",
            hex::encode(root),
            hex::encode(expected_state_root)
        );
    }

    Ok(())
}
