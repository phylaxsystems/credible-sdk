use metrics::histogram;
use std::time::Instant;

/// Records the execution time of a transport RPC request.
pub(crate) struct RpcRequestDuration {
    metric_name: &'static str,
    start: Instant,
}

impl RpcRequestDuration {
    pub(crate) fn new(metric_name: &'static str) -> Self {
        Self {
            metric_name,
            start: Instant::now(),
        }
    }
}

impl Drop for RpcRequestDuration {
    fn drop(&mut self) {
        histogram!(self.metric_name).record(self.start.elapsed());
    }
}
