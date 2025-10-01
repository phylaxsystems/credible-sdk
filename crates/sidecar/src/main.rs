//! # The credible layer sidecar
#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::unreadable_literal)]
#![allow(clippy::similar_names)]
#![allow(clippy::should_panic_without_expect)]

mod args;
mod cache;
mod config;
pub mod engine;
mod indexer;
mod metrics;
mod sync_service;
pub(crate) mod transactions_state;
pub mod transport;
mod utils;

use crate::{
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
    sync_service::StateSyncService,
    transport::Transport,
};
use assertion_executor::{
    AssertionExecutor,
    db::overlay::OverlayDb,
};
use crossbeam::channel::unbounded;
use std::sync::Arc;

use clap::Parser;
use rust_tracing::trace;

use crate::{
    cache::{
        Cache,
        sources::{
            Source,
            besu_client::BesuClient,
            redis::RedisClientBackend,
            sequencer::Sequencer,
        },
    },
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
use args::{
    SidecarArgs,
    TransportProtocolArg,
};

fn create_transport_from_args(
    args: &SidecarArgs,
    tx_sender: TransactionQueueSender,
    state_results: Arc<TransactionsState>,
) -> anyhow::Result<AnyTransport> {
    match args.transport_protocol {
        TransportProtocolArg::Http => {
            let cfg = HttpTransportConfig::try_from(args.transport.clone())?;
            let t = HttpTransport::new(cfg, tx_sender, state_results)?;
            Ok(AnyTransport::Http(t))
        }
        TransportProtocolArg::Grpc => {
            let cfg = GrpcTransportConfig::try_from(args.transport.clone())?;
            let t = GrpcTransport::new(cfg, tx_sender, state_results)?;
            Ok(AnyTransport::Grpc(t))
        }
    }
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> anyhow::Result<()> {
    // Initialize the rustls CryptoProvider for HTTPS support
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    trace();

    let args = SidecarArgs::parse();

    let executor_config = init_executor_config(&args);
    let assertion_store = init_assertion_store(&args)?;
    let assertion_executor =
        AssertionExecutor::new(executor_config.clone(), assertion_store.clone());

    let engine_state_results = TransactionsState::new();

    loop {
        let mut sources: Vec<Arc<dyn Source>> = vec![];
        if let Some(sequencer_url) = &args.state.sequencer_url
            && let Ok(sequencer) = Sequencer::try_new(sequencer_url).await
        {
            sources.push(Arc::new(sequencer));
        }
        if let Some(besu_client_url) = &args.state.besu_client_ws_url
            && let Ok(besu_client) = BesuClient::try_build(besu_client_url).await
        {
            sources.push(besu_client);
        }
        if let Some(redis_url) = &args.state.redis_url
            && let Ok(redis_client) = RedisClientBackend::from_url(redis_url)
        {
            let redis_cache = Arc::new(RedisCache::new(redis_client));
            sources.push(redis_cache);
        }

        // The cache is flushed on restart
        let cache = Arc::new(Cache::new(sources, args.state.minimum_state_diff));
        let state: OverlayDb<Cache> = OverlayDb::new(
            Some(cache.clone()),
            args.credible
                .overlay_cache_capacity_bytes
                .unwrap_or(1024 * 1024 * 1024) as u64,
        );

        let (transport_sender, service_receiver) = unbounded();
        let (engine_sender, engine_receiver) = unbounded();

        let mut transport =
            create_transport_from_args(&args, transport_sender, engine_state_results.clone())?;

        let sync_service_future =
            StateSyncService::new(cache.clone(), service_receiver, engine_sender).run();
        tokio::pin!(sync_service_future);

        let mut engine = CoreEngine::new(
            state,
            cache,
            engine_receiver,
            assertion_executor.clone(),
            engine_state_results.clone(),
            args.credible.transaction_results_max_capacity,
        );

        let indexer_cfg =
            init_indexer_config(&args, assertion_store.clone(), &executor_config).await?;

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Received Ctrl+C, shutting down...");
                break;
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
            result = &mut sync_service_future => {
                if let Err(e) = result {
                    if ErrorRecoverability::from(&e).is_recoverable() {
                        tracing::error!(error = ?e, "Sync service exited");
                    } else {
                        critical!(error = ?e, "Sync service exited");
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
