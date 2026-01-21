//! # The credible layer sidecar
#![doc = include_str!("../README.md")]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::unreadable_literal)]
#![allow(clippy::similar_names)]
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

use assertion_executor::{
    AssertionExecutor,
    db::overlay::OverlayDb,
};
use credible_utils::shutdown::wait_for_sigterm;
use flume::unbounded;
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
            mdbx::MdbxSource,
        },
    },
    config::{
        init_assertion_store,
        init_executor_config,
        init_indexer_config,
    },
    critical,
    engine::{
        CoreEngine,
        queue::TransactionQueueSender,
    },
    event_sequencing::EventSequencing,
    health::HealthServer,
    indexer,
    transaction_observer::{
        TransactionObserver,
        TransactionObserverConfig,
    },
    transactions_state::{
        TransactionResultEvent,
        TransactionsState,
    },
    transport::{
        AnyTransport,
        Transport,
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
use state_store::mdbx::{
    StateReader,
    common::CircularBufferConfig,
};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
    thread::JoinHandle,
    time::Duration,
};
use tracing::{
    error,
    log::info,
};

/// Create transport with optional result streaming support.
///
/// For gRPC transport with `result_event_rx`, enables `SubscribeResults` streaming.
fn create_transport_from_args(
    config: &Config,
    tx_sender: TransactionQueueSender,
    state_results: Arc<TransactionsState>,
    result_event_rx: Option<flume::Receiver<TransactionResultEvent>>,
) -> anyhow::Result<AnyTransport> {
    match config.transport.protocol {
        TransportProtocol::Http => {
            let cfg = HttpTransportConfig::try_from(config.transport.clone())?;
            let t = HttpTransport::new(
                cfg,
                tx_sender,
                state_results,
                config.transport.event_id_buffer_capacity,
            )?;
            Ok(AnyTransport::Http(t))
        }
        TransportProtocol::Grpc => {
            let cfg = GrpcTransportConfig::try_from(config.transport.clone())?;
            let t = match result_event_rx {
                Some(rx) => {
                    GrpcTransport::with_result_receiver(
                        &cfg,
                        tx_sender,
                        state_results,
                        rx,
                        config.transport.event_id_buffer_capacity,
                    )?
                }
                None => {
                    GrpcTransport::new(
                        cfg,
                        tx_sender,
                        state_results,
                        config.transport.event_id_buffer_capacity,
                    )?
                }
            };
            Ok(AnyTransport::Grpc(t))
        }
    }
}

/// Holds handles to spawned threads for graceful shutdown
struct ThreadHandles {
    engine: Option<JoinHandle<Result<(), sidecar::engine::EngineError>>>,
    event_sequencing:
        Option<JoinHandle<Result<(), sidecar::event_sequencing::EventSequencingError>>>,
    transaction_observer:
        Option<JoinHandle<Result<(), sidecar::transaction_observer::TransactionObserverError>>>,
}

impl ThreadHandles {
    fn new() -> Self {
        Self {
            engine: None,
            event_sequencing: None,
            transaction_observer: None,
        }
    }

    fn join_all(&mut self) {
        if let Some(handle) = self.engine.take() {
            match handle.join() {
                Ok(Ok(())) => tracing::info!("Engine thread exited cleanly"),
                Ok(Err(e)) => tracing::error!(error = ?e, "Engine thread exited with error"),
                Err(_) => tracing::error!("Engine thread panicked"),
            }
        }
        if let Some(handle) = self.event_sequencing.take() {
            match handle.join() {
                Ok(Ok(())) => tracing::info!("Event sequencing thread exited cleanly"),
                Ok(Err(e)) => {
                    tracing::error!(error = ?e, "Event sequencing thread exited with error");
                }
                Err(_) => tracing::error!("Event sequencing thread panicked"),
            }
        }
        if let Some(handle) = self.transaction_observer.take() {
            match handle.join() {
                Ok(Ok(())) => tracing::info!("Transaction observer thread exited cleanly"),
                Ok(Err(e)) => {
                    tracing::error!(error = ?e, "Transaction observer thread exited with error");
                }
                Err(_) => tracing::error!("Transaction observer thread panicked"),
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("Failed to install rustls crypto provider"))?;

    let _guard = rust_tracing::trace();
    let config = Config::load()?;

    info!("Starting sidecar with config: {config:?}");

    let executor_config = init_executor_config(&config);
    let assertion_store = init_assertion_store(&config)?;
    let assertion_executor =
        AssertionExecutor::new(executor_config.clone(), assertion_store.clone());
    let health_bind_addr: SocketAddr = config.transport.health_bind_addr.parse()?;

    let mut should_shutdown = false;

    while !should_shutdown {
        let mut thread_handles = ThreadHandles::new();

        // Shared shutdown flag for this iteration
        let shutdown_flag = Arc::new(AtomicBool::new(false));

        let mut sources: Vec<Arc<dyn Source>> = vec![];
        if let (Some(state_worker_path), Some(state_worker_depth)) = (
            config.state.state_worker_mdbx_path.as_ref(),
            config.state.state_worker_depth,
        ) {
            match StateReader::new(
                state_worker_path,
                CircularBufferConfig::new(u8::try_from(state_worker_depth)?)?,
            ) {
                Ok(state_worker_client) => {
                    sources.push(Arc::new(MdbxSource::new(state_worker_client)));
                }
                Err(e) => {
                    error!(error = ?e, "Failed to connect MDBX");
                }
            }
        }

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

        let state = Arc::new(Sources::new(sources, config.state.minimum_state_diff));
        let cache: OverlayDb<Sources> = OverlayDb::new(Some(state.clone()));

        let transaction_observer_config = match (
            &config.credible.transaction_observer_db_path,
            &config.credible.transaction_observer_endpoint,
            &config.credible.transaction_observer_auth_token,
            config.credible.transaction_observer_endpoint_rps_max,
            config.credible.transaction_observer_poll_interval_ms,
        ) {
            (
                Some(db_path),
                Some(endpoint),
                Some(auth_token),
                Some(endpoint_rps_max),
                Some(poll_interval_ms),
            ) => {
                Some(TransactionObserverConfig {
                    db_path: db_path.clone(),
                    endpoint: endpoint.clone(),
                    auth_token: auth_token.clone(),
                    endpoint_rps_max,
                    poll_interval: Duration::from_millis(poll_interval_ms),
                })
            }
            _ => None,
        };

        // Channel: Transport -> EventSequencing
        let (transport_tx_sender, event_sequencing_rx) = unbounded();
        // Channel: EventSequencing -> CoreEngine
        let (event_sequencing_tx, engine_rx) = unbounded();
        // Channel: CoreEngine -> TransactionObserver
        let (incident_report_tx, incident_report_rx) = if transaction_observer_config.is_some() {
            let (tx, rx) = unbounded();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let (engine_state_results, result_event_rx) = match config.transport.protocol {
            TransportProtocol::Grpc => {
                let (tx, rx) = unbounded();
                let pending_requests_ttl = if config
                    .credible
                    .transaction_results_pending_requests_ttl_ms
                    .is_zero()
                {
                    Duration::from_secs(2)
                } else {
                    config.credible.transaction_results_pending_requests_ttl_ms
                };
                let accepted_txs_ttl = if config.credible.accepted_txs_ttl_ms.is_zero() {
                    Duration::from_secs(2)
                } else {
                    config.credible.accepted_txs_ttl_ms
                };
                (
                    TransactionsState::with_result_sender_and_ttls(
                        tx,
                        pending_requests_ttl,
                        accepted_txs_ttl,
                    ),
                    Some(rx),
                )
            }
            TransportProtocol::Http => {
                let pending_requests_ttl = if config
                    .credible
                    .transaction_results_pending_requests_ttl_ms
                    .is_zero()
                {
                    Duration::from_secs(2)
                } else {
                    config.credible.transaction_results_pending_requests_ttl_ms
                };
                let accepted_txs_ttl = if config.credible.accepted_txs_ttl_ms.is_zero() {
                    Duration::from_secs(2)
                } else {
                    config.credible.accepted_txs_ttl_ms
                };
                (
                    TransactionsState::new_with_ttls(pending_requests_ttl, accepted_txs_ttl),
                    None,
                )
            }
        };

        let mut transport = create_transport_from_args(
            &config,
            transport_tx_sender,
            engine_state_results.clone(),
            result_event_rx,
        )?;

        // Spawn EventSequencing on a dedicated OS thread
        let event_sequencing = EventSequencing::new(event_sequencing_rx, event_sequencing_tx);
        let (seq_handle, seq_exited) = event_sequencing.spawn(Arc::clone(&shutdown_flag))?;
        thread_handles.event_sequencing = Some(seq_handle);

        // Spawn CoreEngine on a dedicated OS thread
        let engine = CoreEngine::new(
            cache,
            state,
            engine_rx,
            assertion_executor.clone(),
            engine_state_results.clone(),
            config.credible.transaction_results_max_capacity,
            Duration::from_millis(config.state.sources_sync_timeout_ms),
            Duration::from_millis(config.state.sources_monitoring_period_ms),
            config
                .credible
                .overlay_cache_invalidation_every_block
                .unwrap_or(false),
            incident_report_tx,
            #[cfg(feature = "cache_validation")]
            Some(&config.credible.cache_checker_ws_url),
        )
        .await;
        let (engine_handle, engine_exited) = engine.spawn(Arc::clone(&shutdown_flag))?;
        thread_handles.engine = Some(engine_handle);

        let observer_exited_rx =
            if let (Some(transaction_observer_config), Some(incident_report_rx)) =
                (transaction_observer_config, incident_report_rx)
            {
                let transaction_observer =
                    TransactionObserver::new(transaction_observer_config, incident_report_rx)?;
                let (observer_handle, observer_exited) =
                    transaction_observer.spawn(Arc::clone(&shutdown_flag))?;
                thread_handles.transaction_observer = Some(observer_handle);
                Some(observer_exited)
            } else {
                tracing::info!("Transaction observer disabled: missing config");
                None
            };

        let mut health_server = HealthServer::new(health_bind_addr);

        let indexer_cfg =
            init_indexer_config(&config, assertion_store.clone(), &executor_config).await?;

        let observer_exited = async {
            if let Some(observer_exited) = observer_exited_rx {
                observer_exited.await
            } else {
                std::future::pending().await
            }
        };

        // Only async components run in tokio::select!
        // Transport, health server, and indexer need async for network I/O
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Received Ctrl+C, shutting down...");
                should_shutdown = true;
            }
            _ = wait_for_sigterm() => {
                tracing::info!("Received SIGTERM, shutting down...");
                should_shutdown = true;
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
            result = engine_exited => {
                match result {
                    Ok(Ok(())) => tracing::warn!("Engine exited unexpectedly"),
                    Ok(Err(e)) => {
                        if ErrorRecoverability::from(&e).is_recoverable() {
                            tracing::error!(error = ?e, "Engine exited with recoverable error");
                        } else {
                            critical!(error = ?e, "Engine exited with unrecoverable error");
                        }
                    }
                    Err(_) => tracing::error!("Engine notification channel dropped"),
                }
            }
            result = seq_exited => {
                match result {
                    Ok(Ok(())) => tracing::warn!("Event sequencing exited unexpectedly"),
                    Ok(Err(e)) => {
                        if ErrorRecoverability::from(&e).is_recoverable() {
                            tracing::error!(error = ?e, "Event sequencing exited with recoverable error");
                        } else {
                            critical!(error = ?e, "Event sequencing exited with unrecoverable error");
                        }
                    }
                    Err(_) => tracing::error!("Event sequencing notification channel dropped"),
                }
            }
            result = observer_exited => {
                match result {
                    Ok(Ok(())) => tracing::warn!("Transaction observer exited unexpectedly"),
                    Ok(Err(e)) => {
                        if ErrorRecoverability::from(&e).is_recoverable() {
                            tracing::error!(error = ?e, "Transaction observer exited with recoverable error");
                        } else {
                            critical!(error = ?e, "Transaction observer exited with unrecoverable error");
                        }
                    }
                    Err(_) => tracing::error!("Transaction observer notification channel dropped"),
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

        // Signal threads to stop
        tracing::info!("Signaling threads to shutdown...");
        shutdown_flag.store(true, Ordering::Relaxed);

        // Cleanup async components
        transport.stop();
        health_server.stop();
        drop(transport);
        drop(health_server);

        // Wait for threads
        thread_handles.join_all();

        if !should_shutdown {
            tracing::warn!("Sidecar restarting...");
        }
    }

    tracing::info!("Sidecar shutdown complete.");
    Ok(())
}
