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
use state_store::{
    Reader,
    mdbx::{
        StateReader,
        common::CircularBufferConfig,
    },
};
use tracing::info;

mod cli;
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

    let reader = StateReader::new(&args.mdbx_path, config)?;

    let service = StateRootService::new(&reader);

    let (block_number, root) = service.calculate_latest_state_root()?;
    info!(
        "State cache (state_worker) calculated state root: 0x{} for block {block_number}",
        hex::encode(root)
    );

    let block_number = reader.latest_block_number()?.expect("No blocks in Redis");

    let block_metadata_state_root = reader
        .get_block_metadata(block_number)?
        .expect("No block metadata in Redis")
        .state_root;
    info!(
        "Block metadata state root: 0x{}",
        hex::encode(block_metadata_state_root)
    );

    if root == block_metadata_state_root {
        info!("State roots match: 0x{}", hex::encode(root));
    } else {
        error!(
            "State roots do not match: 0x{} != 0x{}",
            hex::encode(root),
            hex::encode(block_metadata_state_root)
        );
    }

    Ok(())
}
