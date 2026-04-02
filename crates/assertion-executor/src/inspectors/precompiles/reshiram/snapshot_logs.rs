use crate::{
    inspectors::{
        CallTracer,
        sol_primitives::PhEvm::ForkId as SolForkId,
    },
    primitives::U256,
};

use alloy_primitives::Log;

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ForkWindowError {
    #[error("Invalid fork type: {fork_type}")]
    InvalidForkType { fork_type: u8 },
    #[error("Call ID {call_id} is too large to be a valid index")]
    CallIdOverflow { call_id: U256 },
    #[error("Cannot query logs at call {call_id}: call is inside a reverted subtree")]
    CallInsideRevertedSubtree { call_id: usize },
    #[error("Pre-call checkpoint missing for call {call_id}")]
    PreCallCheckpointMissing { call_id: usize },
    #[error("Post-call checkpoint missing for call {call_id}")]
    PostCallCheckpointMissing { call_id: usize },
}

/// Returns the slice of transaction logs visible at the given fork snapshot.
///
/// - `PreTx` (0) yields an empty slice; `PostTx` (1) yields all logs.
/// - `PreCall` (2) / `PostCall` (3) use the call tracer's checkpoints to bound the window.
/// - Errors if the fork type is invalid, the call is inside a reverted subtree, or checkpoints are missing.
pub(crate) fn logs_for_fork<'a>(
    tx_logs: &'a [Log],
    call_tracer: &CallTracer,
    fork: &SolForkId,
) -> Result<&'a [Log], ForkWindowError> {
    let end = match fork.forkType {
        0 => 0,
        1 => tx_logs.len(),
        2 | 3 => {
            let call_id = fork.callIndex;
            let call_id_usize = call_id
                .try_into()
                .map_err(|_| ForkWindowError::CallIdOverflow { call_id })?;

            if !call_tracer.is_call_forkable(call_id_usize) {
                return Err(ForkWindowError::CallInsideRevertedSubtree {
                    call_id: call_id_usize,
                });
            }

            let checkpoint = if fork.forkType == 2 {
                call_tracer.get_pre_call_checkpoint(call_id_usize).ok_or(
                    ForkWindowError::PreCallCheckpointMissing {
                        call_id: call_id_usize,
                    },
                )?
            } else {
                call_tracer.get_post_call_checkpoint(call_id_usize).ok_or(
                    ForkWindowError::PostCallCheckpointMissing {
                        call_id: call_id_usize,
                    },
                )?
            };

            checkpoint.log_i.min(tx_logs.len())
        }
        fork_type => return Err(ForkWindowError::InvalidForkType { fork_type }),
    };

    Ok(&tx_logs[..end])
}
