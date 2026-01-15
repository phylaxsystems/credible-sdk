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
            state_worker::MdbxSource,
        },
    },
    config::{
        init_assertion_store,
        init_executor_config,
        init_indexer_config,
        init_state_worker_config,
    },
    critical,
    engine::{
        CoreEngine,
        queue::TransactionQueueSender,
    },
    event_sequencing::EventSequencing,
    health::HealthServer,
    indexer,
    state_worker::spawn_state_worker,
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
    info,
    warn,
};

/// Create transport with optional result streaming support.
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
    state_worker: Option<JoinHandle<()>>,
}

impl ThreadHandles {
    fn new() -> Self {
        Self {
            engine: None,
            event_sequencing: None,
            transaction_observer: None,
            state_worker: None,
        }
    }

    fn join_all(&mut self) {
        if let Some(handle) = self.engine.take() {
            match handle.join() {
                Ok(Ok(())) => info!("Engine thread exited cleanly"),
                Ok(Err(e)) => error!(error = ?e, "Engine thread exited with error"),
                Err(_) => error!("Engine thread panicked"),
            }
        }
        if let Some(handle) = self.event_sequencing.take() {
            match handle.join() {
                Ok(Ok(())) => info!("Event sequencing thread exited cleanly"),
                Ok(Err(e)) => error!(error = ?e, "Event sequencing thread exited with error"),
                Err(_) => error!("Event sequencing thread panicked"),
            }
        }
        if let Some(handle) = self.transaction_observer.take() {
            match handle.join() {
                Ok(Ok(())) => info!("Transaction observer thread exited cleanly"),
                Ok(Err(e)) => error!(error = ?e, "Transaction observer thread exited with error"),
                Err(_) => error!("Transaction observer thread panicked"),
            }
        }
        if let Some(handle) = self.state_worker.take() {
            if let Ok(()) = handle.join() {
                info!("State worker thread exited cleanly");
            } else {
                error!("State worker thread panicked");
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

    // Build state worker config (optional)
    let state_worker_config = init_state_worker_config(&config);
    if state_worker_config.is_some() {
        info!("State worker enabled");
    } else {
        info!("State worker disabled: missing required config fields");
    }

    let mut should_shutdown = false;

    while !should_shutdown {
        let mut thread_handles = ThreadHandles::new();

        // Shared shutdown flag for this iteration
        let shutdown_flag = Arc::new(AtomicBool::new(false));

        // Spawn state worker thread (if configured)
        let state_worker_exited_rx = if let Some(ref sw_config) = state_worker_config
            && let Ok((handle, exited_rx)) =
                spawn_state_worker(sw_config.clone(), Arc::clone(&shutdown_flag))
        {
            thread_handles.state_worker = Some(handle);
            Some(exited_rx)
        } else {
            None
        };

        // Wait a moment for state worker to initialize before starting sources
        if state_worker_config.is_some() {
            tokio::time::sleep(Duration::from_secs(2)).await;
        }

        let mut sources: Vec<Arc<dyn Source>> = vec![];
        if let (Some(state_worker_path), Some(state_worker_depth)) = (
            config.state.state_worker_mdbx_path.as_ref(),
            config.state.state_worker_depth,
        ) {
            match StateReader::new(
                state_worker_path,
                CircularBufferConfig::new(state_worker_depth)?,
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
        ) {
            match EthRpcSource::try_build(
                eth_rpc_source_ws_url.as_str(),
                eth_rpc_source_http_url.as_str(),
            )
            .await
            {
                Ok(eth_rpc_source) => {
                    sources.push(eth_rpc_source);
                }
                Err(e) => {
                    error!(error = ?e, "Failed to connect to ETH RPC source");
                }
            }
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
                (TransactionsState::with_result_sender(tx), Some(rx))
            }
            TransportProtocol::Http => (TransactionsState::new(), None),
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
                info!("Transaction observer disabled: missing config");
                None
            };

        let mut health_server = HealthServer::new(health_bind_addr);

        let indexer_cfg =
            init_indexer_config(&config, assertion_store.clone(), &executor_config).await?;

        // Create futures that wait forever if the component isn't enabled
        let observer_exited = async {
            if let Some(rx) = observer_exited_rx {
                rx.await
            } else {
                std::future::pending().await
            }
        };

        let state_worker_exited = async {
            if let Some(rx) = state_worker_exited_rx {
                rx.recv_async().await.ok()
            } else {
                std::future::pending().await
            }
        };

        // Main event loop
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received Ctrl+C, shutting down...");
                should_shutdown = true;
            }
            _ = wait_for_sigterm() => {
                info!("Received SIGTERM, shutting down...");
                should_shutdown = true;
            }
            result = transport.run() => {
                if let Err(e) = result {
                    if ErrorRecoverability::from(&e).is_recoverable() {
                        error!(error = ?e, "Transport exited");
                    } else {
                        critical!(error = ?e, "Transport exited");
                    }
                }
            }
            result = engine_exited => {
                match result {
                    Ok(Ok(())) => warn!("Engine exited unexpectedly"),
                    Ok(Err(e)) => {
                        if ErrorRecoverability::from(&e).is_recoverable() {
                            error!(error = ?e, "Engine exited with recoverable error");
                        } else {
                            critical!(error = ?e, "Engine exited with unrecoverable error");
                        }
                    }
                    Err(_) => error!("Engine notification channel dropped"),
                }
            }
            _ = state_worker_exited => {
                // State worker has its own restart loop, so if we get here
                // it means the loop exited (shutdown requested or gave up)
                warn!("State worker thread exited");
            }
            result = seq_exited => {
                match result {
                    Ok(Ok(())) => warn!("Event sequencing exited unexpectedly"),
                    Ok(Err(e)) => {
                        if ErrorRecoverability::from(&e).is_recoverable() {
                            error!(error = ?e, "Event sequencing exited with recoverable error");
                        } else {
                            critical!(error = ?e, "Event sequencing exited with unrecoverable error");
                        }
                    }
                    Err(_) => error!("Event sequencing notification channel dropped"),
                }
            }
            result = observer_exited => {
                match result {
                    Ok(Ok(())) => warn!("Transaction observer exited unexpectedly"),
                    Ok(Err(e)) => {
                        if ErrorRecoverability::from(&e).is_recoverable() {
                            error!(error = ?e, "Transaction observer exited with recoverable error");
                        } else {
                            critical!(error = ?e, "Transaction observer exited with unrecoverable error");
                        }
                    }
                    Err(_) => error!("Transaction observer notification channel dropped"),
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
                        error!(error = ?e, "Indexer exited");
                    } else {
                        critical!(error = ?e, "Indexer exited");
                    }
                }
            }
        }

        // Signal all threads to stop
        info!("Signaling threads to shutdown...");
        shutdown_flag.store(true, Ordering::Relaxed);

        // Cleanup async components
        transport.stop();
        health_server.stop();
        drop(transport);
        drop(health_server);

        // Wait for all threads to finish
        thread_handles.join_all();

        if !should_shutdown {
            warn!("Sidecar restarting...");
        }
    }

    info!("Sidecar shutdown complete.");
    Ok(())
}
