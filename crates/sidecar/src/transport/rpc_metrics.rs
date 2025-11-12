use crate::execution_ids::TxExecutionId;
use metrics::histogram;
use std::time::Instant;

/// Records the execution time of a transport RPC and attaches iteration/tx labels when available.
pub(crate) struct RpcRequestDuration {
    metric_name: &'static str,
    start: Instant,
    iteration_id: Option<u64>,
    tx_hash: Option<String>,
}

impl RpcRequestDuration {
    pub(crate) fn new(metric_name: &'static str) -> Self {
        Self {
            metric_name,
            start: Instant::now(),
            iteration_id: None,
            tx_hash: None,
        }
    }

    pub(crate) fn set_iteration_id(&mut self, iteration_id: u64) {
        self.iteration_id = Some(iteration_id);
    }

    pub(crate) fn set_tx_hash<S: Into<String>>(&mut self, tx_hash: S) {
        self.tx_hash = Some(tx_hash.into());
    }

    pub(crate) fn set_tx_execution_id(&mut self, tx_execution_id: &TxExecutionId) {
        self.set_iteration_id(tx_execution_id.iteration_id);
        self.set_tx_hash(tx_execution_id.tx_hash_hex());
    }
}

impl Drop for RpcRequestDuration {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        let iteration = self.iteration_id.map(|id| id.to_string());
        match (iteration, self.tx_hash.clone()) {
            (Some(iteration_id), Some(tx_hash)) => {
                histogram!(self.metric_name, "iteration_id" => iteration_id, "txhash" => tx_hash)
                    .record(duration)
            }
            (Some(iteration_id), None) => {
                histogram!(self.metric_name, "iteration_id" => iteration_id).record(duration)
            }
            (None, Some(tx_hash)) => {
                histogram!(self.metric_name, "txhash" => tx_hash).record(duration)
            }
            (None, None) => histogram!(self.metric_name).record(duration),
        }
    }
}
