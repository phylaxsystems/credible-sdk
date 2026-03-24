//! Sidecar-owned lifecycle wrapper for the embedded state worker.

use crate::args::{
    Config,
    StateSourceConfig,
};
use anyhow::{
    Context,
    Result,
    anyhow,
};
use state_worker::host::{
    EmbeddedStateWorkerConfig,
    run_embedded_worker,
};
use std::{
    path::PathBuf,
    thread::JoinHandle,
};
use tokio::{
    runtime::Builder,
    sync::broadcast,
};
use tracing::info;

pub struct StateWorkerHostHandle {
    shutdown_tx: broadcast::Sender<()>,
    join_handle: JoinHandle<Result<()>>,
}

impl StateWorkerHostHandle {
    /// Signal the embedded worker to stop.
    pub fn signal_shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }

    /// Join the worker host thread.
    pub fn join(self) -> std::thread::Result<Result<()>> {
        self.join_handle.join()
    }
}

/// Spawn the embedded state worker on a sidecar-owned OS thread.
///
/// # Errors
///
/// Returns an error when the sidecar configuration cannot be converted into an
/// embedded worker config or when the worker thread cannot be spawned.
pub fn start_state_worker_host(config: &Config) -> Result<StateWorkerHostHandle> {
    let worker_config = embedded_state_worker_config(config)?;
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

    let join_handle = std::thread::Builder::new()
        .name("sidecar-state-worker".into())
        .spawn(move || run_worker_thread(worker_config, shutdown_rx))
        .context("failed to spawn state worker host thread")?;

    Ok(StateWorkerHostHandle {
        shutdown_tx,
        join_handle,
    })
}

pub fn embedded_state_worker_config(config: &Config) -> Result<EmbeddedStateWorkerConfig> {
    let ws_url = worker_ws_url(config)
        .ok_or_else(|| anyhow!("missing worker websocket source configuration"))?;
    let (mdbx_path, state_depth) = worker_mdbx_config(config)
        .ok_or_else(|| anyhow!("missing worker MDBX source configuration"))?;
    let genesis_path = config
        .state
        .worker_genesis_path
        .clone()
        .ok_or_else(|| anyhow!("missing embedded worker genesis path configuration"))?;

    let state_depth = u8::try_from(state_depth)
        .context("state worker depth exceeds the embedded worker limit")?;

    Ok(EmbeddedStateWorkerConfig::new(
        ws_url,
        mdbx_path,
        None,
        state_depth,
        genesis_path,
    ))
}

fn run_worker_thread(
    worker_config: EmbeddedStateWorkerConfig,
    shutdown_rx: broadcast::Receiver<()>,
) -> Result<()> {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build sidecar state worker runtime")?;

    info!(
        mdbx_path = %worker_config.mdbx_path.display(),
        genesis_path = %worker_config.genesis_path.display(),
        "starting embedded state worker host"
    );

    runtime.block_on(async move { run_embedded_worker(&worker_config, shutdown_rx).await })
}

fn worker_ws_url(config: &Config) -> Option<String> {
    config
        .state
        .sources
        .iter()
        .find_map(|source| match source {
            StateSourceConfig::EthRpc { ws_url, .. } => Some(ws_url.clone()),
            StateSourceConfig::Mdbx { .. } => None,
        })
        .or_else(|| config.state.legacy.eth_rpc_source_ws_url.clone())
}

fn worker_mdbx_config(config: &Config) -> Option<(PathBuf, usize)> {
    config
        .state
        .sources
        .iter()
        .find_map(|source| match source {
            StateSourceConfig::Mdbx { mdbx_path, depth } => {
                Some((PathBuf::from(mdbx_path), *depth))
            }
            StateSourceConfig::EthRpc { .. } => None,
        })
        .or_else(|| {
            config
                .state
                .legacy
                .state_worker_mdbx_path
                .clone()
                .zip(config.state.legacy.state_worker_depth)
                .map(|(path, depth)| (PathBuf::from(path), depth))
        })
}

#[cfg(test)]
mod tests {
    use super::embedded_state_worker_config;
    use crate::args::Config;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_config(state_json: &str) -> Config {
        let mut temp_file = NamedTempFile::new().expect("temp config file");
        write!(
            temp_file,
            r#"{{
  "chain": {{
    "spec_id": "CANCUN",
    "chain_id": 1
  }},
  "credible": {{
    "assertion_gas_limit": 30000000,
    "assertion_da_rpc_url": "http://localhost:8545",
    "event_source_url": "http://localhost:4350/graphql",
    "assertion_store_db_path": "/tmp/store.db",
    "state_oracle": "0x1234567890123456789012345678901234567890",
    "state_oracle_deployment_block": 100,
    "transaction_results_max_capacity": 10000
  }},
  "transport": {{
    "bind_addr": "127.0.0.1:3000"
  }},
  "state": {state_json}
}}"#
        )
        .expect("write config");
        temp_file.flush().expect("flush config");
        Config::from_file(temp_file.path()).expect("resolve sidecar config")
    }

    #[test]
    fn test_state_worker_host_uses_state_sources_config() {
        let config = write_config(
            r#"{
    "sources": [
      {"type": "eth-rpc", "ws_url": "ws://rpc.example:8546", "http_url": "http://rpc.example:8545"},
      {"type": "mdbx", "mdbx_path": "/data/state_worker.mdbx", "depth": 7}
    ],
    "worker_genesis_path": "/data/genesis.json",
    "minimum_state_diff": 10,
    "sources_sync_timeout_ms": 30000,
    "sources_monitoring_period_ms": 1000
  }"#,
        );

        let worker_config =
            embedded_state_worker_config(&config).expect("embedded worker config from sources");

        assert_eq!(worker_config.ws_url, "ws://rpc.example:8546");
        assert_eq!(worker_config.mdbx_path.as_os_str(), "/data/state_worker.mdbx");
        assert_eq!(worker_config.state_depth, 7);
        assert_eq!(worker_config.genesis_path.as_os_str(), "/data/genesis.json");
        assert_eq!(worker_config.start_block, None);
    }

    #[test]
    fn test_state_worker_host_uses_legacy_state_fields() {
        let config = write_config(
            r#"{
    "worker_genesis_path": "/data/legacy-genesis.json",
    "eth_rpc_source_ws_url": "ws://legacy.example:8546",
    "eth_rpc_source_http_url": "http://legacy.example:8545",
    "state_worker_mdbx_path": "/data/legacy-state-worker.mdbx",
    "state_worker_depth": 5,
    "minimum_state_diff": 10,
    "sources_sync_timeout_ms": 30000,
    "sources_monitoring_period_ms": 1000
  }"#,
        );

        let worker_config =
            embedded_state_worker_config(&config).expect("embedded worker config from legacy");

        assert_eq!(worker_config.ws_url, "ws://legacy.example:8546");
        assert_eq!(
            worker_config.mdbx_path.as_os_str(),
            "/data/legacy-state-worker.mdbx"
        );
        assert_eq!(worker_config.state_depth, 5);
        assert_eq!(
            worker_config.genesis_path.as_os_str(),
            "/data/legacy-genesis.json"
        );
    }
}
