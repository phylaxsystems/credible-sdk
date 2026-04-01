use anyhow::Context;
use mdbx::{
    Reader,
    StateWriter,
    common::CircularBufferConfig,
};
use sidecar::cache::sources::{
    Source,
    mdbx::MdbxSource,
};
use state_worker::{
    DEFAULT_TRACE_TIMEOUT as STATE_WORKER_DEFAULT_TRACE_TIMEOUT,
    FlushControl,
    WorkerConfig as StateWorkerConfig,
    run_supervisor_loop,
};
use std::{
    path::Path,
    sync::Arc,
    thread::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use url::Url;

pub(crate) struct IntegratedStateWorker {
    pub(crate) commit_control: Arc<FlushControl>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) handle: JoinHandle<anyhow::Result<()>>,
}

fn spawn_integrated_state_worker(
    config: StateWorkerConfig,
    shutdown: CancellationToken,
    commit_control: Arc<FlushControl>,
) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
    Ok(std::thread::Builder::new()
        .name("sidecar-state-worker".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(run_supervisor_loop(config, Some(commit_control), shutdown))
        })?)
}

pub(crate) fn build_integrated_mdbx_source(
    mdbx_path: &Path,
    buffer_capacity: usize,
    ws_url: &Url,
    genesis_file: &Path,
    start_block: Option<u64>,
) -> anyhow::Result<(Arc<dyn Source>, Option<IntegratedStateWorker>)> {
    build_integrated_mdbx_source_with(
        mdbx_path,
        buffer_capacity,
        ws_url,
        genesis_file,
        start_block,
        spawn_integrated_state_worker,
    )
}

pub(crate) fn build_integrated_mdbx_source_with<F>(
    mdbx_path: &Path,
    buffer_capacity: usize,
    ws_url: &Url,
    genesis_file: &Path,
    start_block: Option<u64>,
    spawn_worker: F,
) -> anyhow::Result<(Arc<dyn Source>, Option<IntegratedStateWorker>)>
where
    F: FnOnce(
        StateWorkerConfig,
        CancellationToken,
        Arc<FlushControl>,
    ) -> anyhow::Result<JoinHandle<anyhow::Result<()>>>,
{
    let writer = StateWriter::new(mdbx_path, CircularBufferConfig::new(1)?).with_context(|| {
        format!(
            "failed to initialize integrated MDBX database at {}",
            mdbx_path.display()
        )
    })?;
    let reader = writer.reader().clone();

    let commit_control = FlushControl::new();
    if let Some(block_number) = writer.latest_block_number().with_context(|| {
        format!(
            "failed to read integrated MDBX head at {}",
            mdbx_path.display()
        )
    })? {
        commit_control.record_committed_block(block_number);
    }
    drop(writer);
    let source = Arc::new(MdbxSource::new_with_flush_control(
        reader,
        commit_control.clone(),
    ));

    let shutdown = CancellationToken::new();
    let worker_config = StateWorkerConfig {
        ws_url: ws_url.clone(),
        mdbx_path: mdbx_path.to_path_buf(),
        start_block,
        end_block: None,
        mdbx_depth: 1,
        buffer_capacity,
        genesis_file: genesis_file.to_path_buf(),
        trace_timeout: STATE_WORKER_DEFAULT_TRACE_TIMEOUT,
    };
    let worker = match spawn_worker(worker_config, shutdown.clone(), commit_control.clone()) {
        Ok(handle) => {
            Some(IntegratedStateWorker {
                commit_control,
                shutdown,
                handle,
            })
        }
        Err(err) => {
            tracing::error!(
                error = ?err,
                ?mdbx_path,
                "failed to start integrated state worker; continuing with existing MDBX state"
            );
            None
        }
    };

    Ok((source, worker))
}

#[cfg(test)]
mod tests {
    use super::build_integrated_mdbx_source_with;
    use alloy::primitives::{
        B256,
        U256,
    };
    use anyhow::anyhow;
    use mdbx::{
        BlockStateUpdate,
        StateWriter,
        Writer,
        common::CircularBufferConfig,
    };
    use std::path::Path;
    use tempfile::tempdir;
    use url::Url;

    fn commit_empty_block(writer: &StateWriter, block_number: u64) -> anyhow::Result<()> {
        let block_hash_byte = u8::try_from(block_number).unwrap_or(u8::MAX);
        writer.commit_block(&BlockStateUpdate {
            block_number,
            block_hash: B256::repeat_byte(block_hash_byte),
            state_root: B256::repeat_byte(block_hash_byte.saturating_add(1)),
            accounts: Vec::new(),
        })?;
        Ok(())
    }

    #[test]
    fn integrated_source_keeps_existing_mdbx_state_when_worker_start_fails() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("state");
        let writer = StateWriter::new(&path, CircularBufferConfig::new(1)?)?;
        commit_empty_block(&writer, 5)?;
        drop(writer);

        let ws_url = Url::parse("ws://127.0.0.1:8546").unwrap();
        let genesis = Path::new("/tmp/genesis.json");
        let (source, worker) = build_integrated_mdbx_source_with(
            &path,
            3,
            &ws_url,
            genesis,
            None,
            |_config, _shutdown, _commit_control| Err(anyhow!("spawn failed")),
        )?;

        assert!(worker.is_none());
        assert!(source.is_synced(U256::from(5), U256::from(9)));
        Ok(())
    }

    #[test]
    fn integrated_source_fresh_path_still_builds_source_when_worker_start_fails()
    -> anyhow::Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("fresh-state");
        let ws_url = Url::parse("ws://127.0.0.1:8546").unwrap();
        let genesis = Path::new("/tmp/genesis.json");

        let (source, worker) = build_integrated_mdbx_source_with(
            &path,
            3,
            &ws_url,
            genesis,
            None,
            |_config, _shutdown, _commit_control| Err(anyhow!("spawn failed")),
        )?;

        assert!(path.exists());
        assert!(worker.is_none());
        assert!(!source.is_synced(U256::ZERO, U256::ZERO));
        Ok(())
    }
}
