#![recursion_limit = "1024"]
#![doc = include_str!("../README.md")]
#![deny(clippy::panic)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::unreachable)]
#![deny(clippy::todo)]
#![deny(clippy::unimplemented)]
#![warn(clippy::indexing_slicing)]
#![cfg_attr(test, allow(clippy::panic))]
#![cfg_attr(test, allow(clippy::unwrap_used))]
#![cfg_attr(test, allow(clippy::expect_used))]
#![cfg_attr(test, allow(clippy::indexing_slicing))]

mod cli;
mod genesis;
#[cfg(test)]
mod integration_tests;
mod macros;
mod metrics;
mod state;
mod system_calls;
mod worker;

use crate::{
    cli::Args,
    system_calls::SystemCalls,
    worker::StateWorker,
};

use rust_tracing::trace;
use tracing::{
    info,
    warn,
};

use alloy_provider::{
    Provider,
    ProviderBuilder,
    RootProvider,
    WsConnect,
};
use anyhow::{
    Context,
    Result,
};
use clap::Parser;
use state_store::{
    Writer,
    mdbx::{
        StateWriter,
        common::CircularBufferConfig,
    },
};
use std::{
    sync::Arc,
    time::Duration,
};
use tokio::sync::broadcast;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the rustls CryptoProvider for HTTPS support
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("Failed to install rustls crypto provider"))?;

    // Install the shared tracing subscriber used across Credible services.
    let _guard = trace();

    let args = Args::parse();

    let provider = connect_provider(&args.ws_url).await?;

    // Validate Geth version for prestateTracer diffMode EIP-6780 correctness
    validate_geth_version(&provider).await?;
    let writer_reader = StateWriter::new(
        args.mdbx_path.as_str(),
        CircularBufferConfig::new(args.state_depth)?,
    )
    .context("failed to initialize database client")?;
    writer_reader
        .ensure_dump_index_metadata()
        .context("failed to ensure database namespace index metadata")?;

    // Load genesis from file (required to seed initial state)
    let file_path = &args.file_to_genesis;
    info!("Loading genesis from file: {}", file_path);
    let contents = std::fs::read_to_string(file_path)
        .inspect_err(|e| warn!(error = ?e, file_path = file_path, "Failed to read genesis file"))
        .with_context(|| format!("failed to read genesis file: {file_path}"))?;
    let genesis_state = genesis::parse_from_str(&contents)
        .inspect_err(
            |e| warn!(error = ?e, file_path = file_path, "Failed to parse genesis from file"),
        )
        .with_context(|| format!("failed to parse genesis from file: {file_path}"))?;

    // Extract fork timestamps for system calls before consuming genesis
    let system_calls = SystemCalls::new(
        genesis_state.config().cancun_time,
        genesis_state.config().prague_time,
    );

    info!(
        cancun_time = ?system_calls.cancun_time,
        prague_time = ?system_calls.prague_time,
        "Configured system call fork timestamps"
    );

    // Create the trace provider based on config
    let trace_provider = state::create_trace_provider(provider.clone(), Duration::from_secs(30));

    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

    // Spawn signal handler
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        if let Err(e) = shutdown_signal().await {
            warn!("Error setting up signal handler: {}", e);
        } else {
            info!("Shutdown signal received, initiating graceful shutdown...");
            let _ = shutdown_tx_clone.send(());
        }
    });

    let mut worker = StateWorker::new(
        provider,
        trace_provider,
        writer_reader,
        Some(genesis_state),
        system_calls,
    );

    match worker.run(args.start_block, shutdown_rx).await {
        Ok(()) => {
            info!("State worker shutdown gracefully");
            Ok(())
        }
        Err(e) => Err(e).context("state worker terminated unexpectedly"),
    }
}

/// Wait for SIGTERM or SIGINT (Ctrl+C)
async fn shutdown_signal() -> Result<()> {
    use tokio::signal;

    #[cfg(unix)]
    {
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
            .context("failed to install SIGTERM handler")?;
        let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())
            .context("failed to install SIGINT handler")?;

        tokio::select! {
            _ = sigterm.recv() => {
                info!("Received SIGTERM");
            }
            _ = sigint.recv() => {
                info!("Received SIGINT");
            }
        }
    }

    #[cfg(not(unix))]
    {
        signal::ctrl_c()
            .await
            .context("failed to listen for ctrl-c")?;
        info!("Received Ctrl+C");
    }

    Ok(())
}

/// Establish a WebSocket connection to the execution node and expose the
/// underlying `RootProvider`. The root provider gives us access to the
/// subscription + debug APIs used throughout the worker.
async fn connect_provider(ws_url: &str) -> Result<Arc<RootProvider>> {
    let ws = WsConnect::new(ws_url);
    let provider = ProviderBuilder::new()
        .connect_ws(ws)
        .await
        .context("failed to connect to websocket provider")?;
    Ok(Arc::new(provider.root().clone()))
}

/// Minimum required Geth version for correct prestateTracer diffMode behavior.
///
/// Geth versions before 1.16.6 have a bug (go-ethereum#33049) where the
/// `prestateTracer` diffMode incorrectly reports post-Cancun SELFDESTRUCT
/// operations as full account deletions (pre present, post absent), even
/// though the contract still exists per EIP-6780 semantics.
///
/// This was fixed by PR #33050, merged Oct 31 2025, assigned to milestone 1.16.6.
const MIN_GETH_VERSION: (u64, u64, u64) = (1, 16, 6);

/// How often to retry version check when Geth version is incompatible.
const VERSION_CHECK_RETRY_SECS: u64 = 60;

/// Validate that the connected execution client meets version requirements.
///
/// Specifically, if the client is Geth, it must be version 1.16.6 or later
/// to ensure correct prestateTracer diffMode behavior for post-Cancun
/// SELFDESTRUCT (EIP-6780).
///
/// If Geth version is too old, this function blocks and retries periodically
/// rather than crashing, since the state-worker runs on the same pod as other
/// services that must remain available.
async fn validate_geth_version(provider: &RootProvider) -> Result<()> {
    let (min_major, min_minor, min_patch) = MIN_GETH_VERSION;

    loop {
        let client_version = match provider.get_client_version().await {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    error = %e,
                    retry_secs = VERSION_CHECK_RETRY_SECS,
                    "failed to get client version, retrying"
                );
                tokio::time::sleep(Duration::from_secs(VERSION_CHECK_RETRY_SECS)).await;
                continue;
            }
        };

        info!(client_version = %client_version, "connected to execution client");

        // Parse Geth version string format: "Geth/v1.16.5-stable-abc123/linux-amd64/go1.23"
        // or similar variations like "Geth/v1.16.5/..."
        if let Some((major, minor, patch)) = parse_geth_version(&client_version) {
            let version_ok = (major, minor, patch) >= (min_major, min_minor, min_patch);

            if version_ok {
                info!(
                    geth_version = format!("{major}.{minor}.{patch}"),
                    "Geth version validated (>= {min_major}.{min_minor}.{min_patch})"
                );
                return Ok(());
            }

            // Geth version is too old - log error and wait for upgrade
            // We don't crash because the sidecar runs on the same pod
            tracing::error!(
                geth_version = format!("{major}.{minor}.{patch}"),
                min_version = format!("{min_major}.{min_minor}.{min_patch}"),
                retry_secs = VERSION_CHECK_RETRY_SECS,
                "Geth version is below minimum required. Geth <{min_major}.{min_minor}.{min_patch} \
                 has a known prestateTracer diffMode bug (go-ethereum#33049) that incorrectly \
                 reports post-Cancun SELFDESTRUCT as account deletions, violating EIP-6780. \
                 Fixed by go-ethereum#33050. Please upgrade Geth. Waiting for upgrade..."
            );
            tokio::time::sleep(Duration::from_secs(VERSION_CHECK_RETRY_SECS)).await;
        } else {
            // Not Geth or unrecognized format - allow to proceed
            // Other clients (Erigon, Nethermind, etc.) may have their own implementations
            warn!(
                client_version = %client_version,
                "connected client is not Geth or version could not be parsed; \
                 skipping prestateTracer version validation. Ensure your client \
                 correctly implements EIP-6780 SELFDESTRUCT semantics in traces."
            );
            return Ok(());
        }
    }
}

/// Parse a Geth version string and extract the semantic version tuple.
///
/// Expected formats:
/// - `Geth/v1.16.6-stable-abc123/linux-amd64/go1.23`
/// - `Geth/v1.16.6/linux-amd64/go1.23`
/// - `Geth/v1.16.6-unstable/...`
///
/// Returns `Some((major, minor, patch))` if this is a Geth client with
/// a parseable version, `None` otherwise.
fn parse_geth_version(client_version: &str) -> Option<(u64, u64, u64)> {
    // Must start with "Geth/" (case-insensitive check for robustness)
    if !client_version.to_lowercase().starts_with("geth/") {
        return None;
    }

    // Extract the version part after "Geth/v" or "Geth/"
    let version_start = if client_version.len() > 6 && &client_version[5..6] == "v" {
        6
    } else {
        5
    };

    let remainder = &client_version[version_start..];

    // Find the end of the version number (before "-" or "/" or end of string)
    let version_end = remainder.find(['-', '/']).unwrap_or(remainder.len());

    let version_str = &remainder[..version_end];

    // Parse "major.minor.patch"
    let parts: Vec<&str> = version_str.split('.').collect();
    if parts.len() < 3 {
        return None;
    }

    let major = parts.first()?.parse().ok()?;
    let minor = parts.get(1)?.parse().ok()?;
    let patch = parts.get(2)?.parse().ok()?;

    Some((major, minor, patch))
}

#[cfg(test)]
mod version_tests {
    use super::*;

    #[test]
    fn test_parse_geth_version_standard() {
        assert_eq!(
            parse_geth_version("Geth/v1.16.6-stable-abc123/linux-amd64/go1.23"),
            Some((1, 16, 6))
        );
    }

    #[test]
    fn test_parse_geth_version_without_suffix() {
        assert_eq!(
            parse_geth_version("Geth/v1.16.6/linux-amd64/go1.23"),
            Some((1, 16, 6))
        );
    }

    #[test]
    fn test_parse_geth_version_unstable() {
        assert_eq!(
            parse_geth_version("Geth/v1.17.0-unstable-deadbeef/linux-amd64/go1.24"),
            Some((1, 17, 0))
        );
    }

    #[test]
    fn test_parse_geth_version_old() {
        assert_eq!(
            parse_geth_version("Geth/v1.16.5-stable-abc123/linux-amd64/go1.23"),
            Some((1, 16, 5))
        );
    }

    #[test]
    fn test_parse_geth_version_without_v_prefix() {
        assert_eq!(
            parse_geth_version("Geth/1.16.6-stable/linux-amd64/go1.23"),
            Some((1, 16, 6))
        );
    }

    #[test]
    fn test_parse_geth_version_not_geth() {
        assert_eq!(
            parse_geth_version("Erigon/v2.60.0/linux-amd64/go1.23"),
            None
        );
        assert_eq!(
            parse_geth_version("Nethermind/v1.25.0/linux-x64/dotnet8"),
            None
        );
    }

    #[test]
    fn test_parse_geth_version_invalid() {
        assert_eq!(parse_geth_version("Geth/invalid"), None);
        assert_eq!(parse_geth_version("Geth/v1.16"), None);
        assert_eq!(parse_geth_version(""), None);
    }

    #[test]
    fn test_version_comparison() {
        let min = MIN_GETH_VERSION;

        // Versions that should pass
        assert!((1, 16, 6) >= min);
        assert!((1, 16, 7) >= min);
        assert!((1, 17, 0) >= min);
        assert!((2, 0, 0) >= min);

        // Versions that should fail
        assert!((1, 16, 5) < min);
        assert!((1, 15, 10) < min);
        assert!((0, 99, 99) < min);
    }
}
