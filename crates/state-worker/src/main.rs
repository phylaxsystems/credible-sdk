#![recursion_limit = "1024"]
#![doc = include_str!("../README.md")]

mod cli;
use crate::cli::Args;
use anyhow::Result;
use clap::Parser;
use rust_tracing::trace;
use tracing::warn;

fn main() -> Result<()> {
    // Initialize the rustls CryptoProvider for HTTPS support
    if rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .is_err()
    {
        warn!("Failed to install rustls crypto provider; continuing without default provider");
    }

    // Install the shared tracing subscriber used across Credible services.
    let _guard = trace();

    let args = Args::parse();
    let config = state_worker::host::EmbeddedStateWorkerConfig::new(
        args.ws_url,
        args.mdbx_path,
        args.start_block,
        args.state_depth,
        args.file_to_genesis,
    );

    state_worker::host::run_embedded_worker_until_shutdown(config)
}
