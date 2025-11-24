//! # The credible layer sidecar
#![doc = include_str!("../README.md")]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::unreadable_literal)]
#![allow(clippy::similar_names)]

use assertion_executor::{
    AssertionExecutor,
    db::overlay::OverlayDb,
};
use crossbeam::channel::unbounded;
use sidecar::{
    cache::sources::redis::RedisSource,
    config::{
        init_assertion_store,
        init_executor_config,
        init_indexer_config,
    },
    engine::{
        CoreEngine,
        queue::TransactionQueueSender,
    },
    health::HealthServer,
    transport::Transport,
    with_panic_recovery,
};
use std::{
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use sidecar::{
    args::{
        Config,
        TransportProtocol,
    },
    cache::{
        Sources,
        sources::{
            Source,
            eth_rpc_source::EthRpcSource,
        },
    },
    critical,
    event_sequencing::EventSequencing,
    indexer,
    transactions_state::TransactionsState,
    transport::{
        AnyTransport,
        grpc::{
            GrpcTransport,
            config::GrpcTransportConfig,
        },
        http::{
            HttpTransport,
            config::HttpTransportConfig,
        },
    },
    utils::ErrorRecoverability,
};
use state_store::{
    CircularBufferConfig,
    StateReader,
};
use tracing::log::info;

fn create_transport_from_args(
    config: &Config,
    tx_sender: TransactionQueueSender,
    state_results: Arc<TransactionsState>,
) -> anyhow::Result<AnyTransport> {
    match config.transport.protocol {
        TransportProtocol::Http => {
            let cfg = HttpTransportConfig::try_from(config.transport.clone())?;
            let t = HttpTransport::new(cfg, tx_sender, state_results)?;
            Ok(AnyTransport::Http(t))
        }
        TransportProtocol::Grpc => {
            let cfg = GrpcTransportConfig::try_from(config.transport.clone())?;
            let t = GrpcTransport::new(cfg, tx_sender, state_results)?;
            Ok(AnyTransport::Grpc(t))
        }
    }
}

#[allow(clippy::too_many_lines)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize the rustls CryptoProvider for HTTPS support
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let _guard = rust_tracing::trace();

    let config = Arc::new(Config::load()?);

    info!("Starting sidecar with config: {config:?}");

    let executor_config = init_executor_config(&config);
    let assertion_store = init_assertion_store(&config)?;
    let assertion_executor =
        AssertionExecutor::new(executor_config.clone(), assertion_store.clone());

    let engine_state_results = TransactionsState::new();

    let health_bind_addr: SocketAddr = config.transport.health_bind_addr.parse()?;

    loop {
        let config = config.clone();
        let assertion_store = assertion_store.clone();
        let executor_config = executor_config.clone();
        let assertion_executor = assertion_executor.clone();
        let engine_state_results = engine_state_results.clone();

        if let Ok(true) = with_panic_recovery!(async move {
            let mut sources: Vec<Arc<dyn Source>> = vec![];
            if let (Some(eth_rpc_source_ws_url), Some(eth_rpc_source_http_url)) = (
                &config.state.eth_rpc_source_ws_url,
                &config.state.eth_rpc_source_http_url,
            ) && let Ok(eth_rpc_source) = EthRpcSource::try_build(
                eth_rpc_source_ws_url.as_str(),
                eth_rpc_source_http_url.as_str(),
            )
            .await
            {
                sources.push(eth_rpc_source);
            }
            if let (Some(redis_url), Some(redis_namespace), Some(redis_depth)) = (
                config.state.redis_url.as_ref(),
                config.state.redis_namespace.as_ref(),
                config.state.redis_depth,
            ) && let Ok(redis_client) = StateReader::new(
                redis_url,
                redis_namespace,
                CircularBufferConfig::new(redis_depth)?,
            ) {
                let redis_source = Arc::new(RedisSource::new(redis_client));
                sources.push(redis_source);
            }

            // The cache is flushed on restart
            let state = Arc::new(Sources::new(sources, config.state.minimum_state_diff));
            let cache: OverlayDb<Sources> = OverlayDb::new(Some(state.clone()));

            let (transport_tx_sender, event_sequencing_tx_receiver) = unbounded();
            let (event_sequencing_tx_sender, core_engine_tx_receiver) = unbounded();
            let mut transport = create_transport_from_args(
                &config,
                transport_tx_sender,
                engine_state_results.clone(),
            )?;

            let mut event_sequencing =
                EventSequencing::new(event_sequencing_tx_receiver, event_sequencing_tx_sender);
            let mut health_server = HealthServer::new(health_bind_addr);

            let mut engine = CoreEngine::new(
                cache,
                state,
                core_engine_tx_receiver,
                assertion_executor.clone(),
                engine_state_results.clone(),
                config.credible.transaction_results_max_capacity,
                Duration::from_millis(config.state.sources_sync_timeout_ms),
                Duration::from_millis(config.state.sources_monitoring_period_ms),
                config
                    .credible
                    .overlay_cache_invalidation_every_block
                    .unwrap_or(false),
                #[cfg(feature = "cache_validation")]
                Some(&config.credible.cache_checker_ws_url),
            )
            .await;

            let indexer_cfg =
                init_indexer_config(&config, assertion_store.clone(), &executor_config).await?;

            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("Received Ctrl+C, shutting down...");
                    return Ok::<bool, anyhow::Error>(true); // Signal to break outer loop
                }
                () = wait_for_sigterm() => {
                    tracing::info!("Received SIGTERM, shutting down...");
                    return Ok(true); // Signal to break outer loop
                }
                _ = engine.run() => {}
                result = transport.run() => {
                    if let Err(e) = result {
                        if ErrorRecoverability::from(&e).is_recoverable() {
                            tracing::error!(error = ?e, "Transport exited");
                        } else {
                            critical!(error = ?e, "Transport exited");
                        }
                    }
                }
                result = event_sequencing.run() => {
                    if let Err(e) = result {
                        if ErrorRecoverability::from(&e).is_recoverable() {
                            tracing::error!(error = ?e, "Event sequencing exited");
                        } else {
                            critical!(error = ?e, "Event sequencing exited");
                        }
                    }
                }
                result = health_server.run() => {
                    if let Err(e) = result {
                        critical!(error = ?e, "Health server exited");
                    }
                }
                result = indexer::run_indexer(indexer_cfg) => {
                    if let Err(e) = result {
                        if ErrorRecoverability::from(&e).is_recoverable() {
                            tracing::error!(error = ?e, "Indexer exited");
                        } else {
                            critical!(error = ?e, "Indexer exited");
                        }
                    }
                }
            }
            transport.stop();
            health_server.stop();
            drop(transport);
            drop(health_server);
            drop(engine);
            tracing::warn!("Sidecar restarted.");
            Ok::<bool, anyhow::Error>(false) // Continue loop
        })
        .await
        {
            break;
        }
    }

    tracing::info!("Sidecar shutdown complete.");
    Ok(())
}

async fn wait_for_sigterm() {
    use tokio::signal::unix::{
        SignalKind,
        signal,
    };
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to setup SIGTERM handler");
    sigterm.recv().await;
}
