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
    cache::sources::redis::RedisCache,
    config::{
        init_assertion_store,
        init_executor_config,
        init_indexer_config,
    },
    engine::{
        CoreEngine,
        queue::TransactionQueueSender,
    },
    transport::Transport,
};
use std::{
    sync::Arc,
    time::Duration,
};

use sidecar::{
    args::{
        Config,
        TransportProtocol,
    },
    cache::{
        Cache,
        sources::{
            Source,
            besu_client::BesuClient,
            sequencer::Sequencer,
        },
    },
    critical,
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize the rustls CryptoProvider for HTTPS support
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let _guard = rust_tracing::trace();

    let config = Config::load()?;

    info!("Starting sidecar with config: {config:?}");

    let executor_config = init_executor_config(&config);
    let assertion_store = init_assertion_store(&config)?;
    let assertion_executor =
        AssertionExecutor::new(executor_config.clone(), assertion_store.clone());

    let engine_state_results = TransactionsState::new();

    loop {
        let mut sources: Vec<Arc<dyn Source>> = vec![];
        if let Some(sequencer_url) = &config.state.sequencer_url
            && let Ok(sequencer) = Sequencer::try_new(sequencer_url).await
        {
            sources.push(Arc::new(sequencer));
        }
        if let Some(besu_client_url) = &config.state.besu_client_ws_url
            && let Ok(besu_client) = BesuClient::try_build(besu_client_url).await
        {
            sources.push(besu_client);
        }
        if let Some(redis_url) = &config.state.redis_url
            && let Ok(redis_client) = StateReader::new(
                redis_url,
                &config.state.redis_namespace,
                CircularBufferConfig::new(config.state.redis_depth)?,
            )
        {
            let redis_cache = Arc::new(RedisCache::new(redis_client));
            sources.push(redis_cache);
        }

        // The cache is flushed on restart
        let cache = Arc::new(Cache::new(sources, config.state.minimum_state_diff));
        let state: OverlayDb<Cache> = OverlayDb::new(
            Some(cache.clone()),
            config.credible.overlay_cache_capacity.unwrap_or(100_000) as u64,
        );

        let (tx_sender, tx_receiver) = unbounded();
        let mut transport =
            create_transport_from_args(&config, tx_sender, engine_state_results.clone())?;

        let mut engine = CoreEngine::new(
            state,
            cache,
            tx_receiver,
            assertion_executor.clone(),
            engine_state_results.clone(),
            config.credible.transaction_results_max_capacity,
            Duration::from_millis(config.state.sources_sync_timeout_ms),
            Duration::from_millis(config.state.sources_monitoring_period_ms),
            #[cfg(feature = "cache_validation")]
            Some(&config.credible.cache_checker_ws_url),
        )
        .await;

        let indexer_cfg =
            init_indexer_config(&config, assertion_store.clone(), &executor_config).await?;

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Received Ctrl+C, shutting down...");
                break;
            }
            () = wait_for_sigterm() => {
                tracing::info!("Received SIGTERM, shutting down...");
            }
            result = engine.run() => {
                if let Err(e) = result {
                    if ErrorRecoverability::from(&e).is_recoverable() {
                        tracing::error!(error = ?e, "Engine exited");
                    } else {
                        critical!(error = ?e, "Engine exited");
                    }
                }
            }
            result = transport.run() => {
                if let Err(e) = result {
                    if ErrorRecoverability::from(&e).is_recoverable() {
                        tracing::error!(error = ?e, "Transport exited");
                    } else {
                        critical!(error = ?e, "Transport exited");
                    }
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
        drop(transport);
        drop(engine);
        tracing::warn!("Sidecar restarted.");
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
