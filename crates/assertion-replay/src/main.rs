mod config;
#[cfg(test)]
mod integration_tests;
mod server;
mod services;

use crate::{
    config::Config,
    server::{
        AppState,
        app_router,
    },
    services::replay::ReplayDurationTuning,
};
use alloy::{
    providers::WsConnect,
    transports::{
        RpcError,
        TransportErrorKind,
    },
};
use alloy_provider::{
    Provider,
    ProviderBuilder,
};
use credible_utils::shutdown::wait_for_sigterm;
use rust_tracing::trace;
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::AtomicU64,
    },
};
use thiserror::Error;
use tracing::info;

/// Bootstraps and runs the replaying HTTP server.
#[tokio::main]
async fn main() -> Result<(), MainError> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| MainError::RustlsProviderInstall)?;
    trace();

    let config = Arc::new(Config::load());
    let ws = WsConnect::new(config.archive_ws_url.clone());
    let head_provider = Arc::new(
        ProviderBuilder::new()
            .connect_ws(ws)
            .await
            .map_err(|source| MainError::HeadProviderConnect { source })?
            .root()
            .clone(),
    );
    let replay_duration_tuning = ReplayDurationTuning::from_config(config.as_ref());
    let state = AppState {
        head_provider,
        replay_window: Arc::new(AtomicU64::new(config.replay_window.max(1))),
        replay_duration_tuning,
        config,
    };
    let app = app_router(state.clone());
    let bind_addr = state.config.bind_addr;
    let listener = tokio::net::TcpListener::bind(state.config.bind_addr)
        .await
        .map_err(|source| MainError::BindListener { bind_addr, source })?;

    info!(bind_addr = %state.config.bind_addr, "replaying server started");

    tokio::select! {
        result = axum::serve(listener, app) => {
            result.map_err(|source| MainError::Serve { source })?;
        }
        _ = tokio::signal::ctrl_c() => {
            info!("received Ctrl+C, shutting down");
        }
        _ = wait_for_sigterm() => {
            info!("received SIGTERM, shutting down");
        }
    }

    Ok(())
}

#[derive(Debug, Error)]
enum MainError {
    #[error("failed to install rustls crypto provider")]
    RustlsProviderInstall,
    #[error("failed to bind {bind_addr}")]
    BindListener {
        bind_addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to connect head websocket provider")]
    HeadProviderConnect {
        #[source]
        source: RpcError<TransportErrorKind>,
    },
    #[error("axum server failed")]
    Serve {
        #[source]
        source: std::io::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::MainError;
    use std::net::{
        IpAddr,
        Ipv4Addr,
        SocketAddr,
    };

    #[test]
    fn main_error_formats_bind_variant() {
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
        let error = MainError::BindListener {
            bind_addr,
            source: std::io::Error::from(std::io::ErrorKind::AddrInUse),
        };
        let rendered = error.to_string();
        assert!(rendered.contains("failed to bind"));
        assert!(rendered.contains("127.0.0.1:8080"));
    }
}
