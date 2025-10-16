#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::cast_precision_loss)]

use crate::{
    cli::Args,
    state_root::StateRootService,
};
use anyhow::Result;
use clap::Parser;
use log::error;
use rust_tracing::trace;
use state_store::CircularBufferConfig;
use tracing::info;

mod cli;
mod json_rpc_client;
mod state_root;

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

    let rpc_json_client =
        json_rpc_client::Client::try_new_with_rpc_url(args.rpc_url.as_str()).await?;

    let service = StateRootService::new(&args.redis_url, &args.redis_namespace, config)?;

    let (block_number, root) = service.calculate_latest_state_root()?;
    info!(
        "State cache (redis) calculated state root: 0x{} for block {block_number}",
        hex::encode(root)
    );

    let rpc_state_root = rpc_json_client.get_block_state_root(block_number).await?;
    info!("RPC client state root: 0x{}", hex::encode(rpc_state_root));

    if root == rpc_state_root {
        info!("State roots match: 0x{}", hex::encode(root));
    } else {
        error!(
            "State roots do not match: 0x{} != 0x{}",
            hex::encode(root),
            hex::encode(rpc_state_root)
        );
    }

    Ok(())
}
