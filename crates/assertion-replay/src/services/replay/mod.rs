mod runtime;

use crate::{
    config::Config,
    server::models::replay::ReplayRequest,
};
use runtime::{
    ReplayRuntime,
    RuntimeError,
};
use thiserror::Error;
use tracing::debug;

/// Runs one full replay pass from configured `start_block` to current head.
pub async fn run_replay(config: &Config, request: &ReplayRequest) -> Result<(), ReplayError> {
    let runtime = ReplayRuntime::new(config)
        .await
        .map_err(|source| ReplayError::RuntimeInitialization { source })?;
    let head = runtime
        .head_block_number()
        .await
        .map_err(|source| ReplayError::HeadBlockQuery { source })?;

    if config.start_block > head {
        return Err(ReplayError::StartBlockAhead {
            start_block: config.start_block,
            head_block: head,
        });
    }

    runtime
        .send_initial_commit(config.start_block)
        .await
        .map_err(|source| ReplayError::InitialCommit { source })?;
    runtime
        .process_block_range(config.start_block, head)
        .await
        .map_err(|source| ReplayError::BlockRangeProcessing { source })?;
    runtime
        .shutdown()
        .await
        .map_err(|source| ReplayError::RuntimeShutdown { source })?;

    debug!(
        assertion_ids_count = request.assertion_ids.len(),
        "replay run completed; assertion results dispatch is not implemented yet"
    );
    Ok(())
}

#[derive(Debug, Error)]
pub enum ReplayError {
    #[error("configured start_block {start_block} is ahead of the current head block {head_block}")]
    StartBlockAhead { start_block: u64, head_block: u64 },
    #[error("failed to initialize replay runtime")]
    RuntimeInitialization {
        #[source]
        source: RuntimeError,
    },
    #[error("failed to query current head block")]
    HeadBlockQuery {
        #[source]
        source: RuntimeError,
    },
    #[error("failed to send initial commit to sidecar engine")]
    InitialCommit {
        #[source]
        source: RuntimeError,
    },
    #[error("failed to process requested block range")]
    BlockRangeProcessing {
        #[source]
        source: RuntimeError,
    },
    #[error("failed to shutdown replay runtime")]
    RuntimeShutdown {
        #[source]
        source: RuntimeError,
    },
}

#[cfg(test)]
mod tests {
    use super::ReplayError;

    #[test]
    fn start_block_ahead_error_has_expected_message() {
        let error = ReplayError::StartBlockAhead {
            start_block: 10,
            head_block: 9,
        };
        let rendered = error.to_string();
        assert!(rendered.contains("start_block 10"));
        assert!(rendered.contains("head block 9"));
    }
}
