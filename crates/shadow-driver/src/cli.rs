//! Command-line configuration for the shadow driver.

use clap::Parser;

/// Runtime configuration flags for the shadow driver.
///
/// We expose each parameter as both a long-form CLI flag and an env var so the
/// worker can be configured through deployment manifests without shell args.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// WebSocket endpoint for the Ethereum node.
    #[arg(long, env = "SHADOW_DRIVER_WS_URL")]
    pub ws_url: String,

    /// Sidecar URL
    #[arg(long, env = "SHADOW_DRIVER_SIDECAR_URL")]
    pub sidecar_url: String,

    /// Request timeout in seconds
    #[arg(
        long,
        default_value = "2",
        env = "SHADOW_DRIVER_REQUEST_TIMEOUT_SECONDS"
    )]
    pub request_timeout_seconds: u64,
}
