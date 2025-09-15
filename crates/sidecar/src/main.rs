//! # The credible layer sidecar
#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::unreadable_literal)]
#![allow(clippy::similar_names)]

mod args;
mod cache;
mod config;
pub mod engine;
mod indexer;
mod metrics;
pub(crate) mod transactions_state;
pub mod transport;
mod utils;

use crate::{
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
        if let Ok(sequencer) = Sequencer::try_new(&args.chain.rpc_url).await {
            sources.push(Arc::new(sequencer));
        }
        if let Ok(besu_client) = BesuClient::try_build(&args.chain.besu_client_ws_url).await {
            sources.push(besu_client);
        }

        // The cache is flushed on restart
        let cache = Arc::new(Cache::new(sources, args.chain.minimum_state_diff));
        let state: OverlayDb<Cache> = OverlayDb::new(
            Some(cache.clone()),
            args.credible
                .overlay_cache_capacity_bytes
                .unwrap_or(1024 * 1024 * 1024) as u64,
        );

        let (tx_sender, tx_receiver) = unbounded();
        let transport = create_transport_from_args(&args, tx_sender, engine_state_results.clone())?;

        let mut engine = CoreEngine::new(
            state,
            cache,
            tx_receiver,
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
                let Err(e) = result else {
                    continue;
                };

                if ErrorRecoverability::from(&e).is_recoverable() {
                    tracing::error!(error = %e, "Engine exited");
                } else {
                    critical!(error = %e, "Engine exited");
                }
            }
            result = transport.run() => {
                let Err(e) = result else {
                    continue;
                };

                if ErrorRecoverability::from(&e).is_recoverable() {
                    tracing::error!(error = %e, "Transport exited");
                } else {
                    critical!(error = %e, "Transport exited");
                }
            }
            result = indexer::run_indexer(indexer_cfg) => {
                let Err(e) = result else {
                    continue;
                };

                if ErrorRecoverability::from(&e).is_recoverable() {
                    tracing::error!(error = %e, "Indexer exited");
                } else {
                    critical!(error = %e, "Indexer exited");
                }
            }
        }

        tracing::warn!("Sidecar restarted.");
    }

    tracing::info!("Sidecar shutdown complete.");
    Ok(())
}
