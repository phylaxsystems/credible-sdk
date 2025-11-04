use super::block_context::BlockContext;
use crate::engine::queue::TxQueueContents;
use axum::{
    extract::{
        Request,
        State,
    },
    middleware::Next,
    response::Response,
};
use tracing::{
    Instrument,
    field::display,
    info_span,
};

/// Lightweight tracing middleware that only adds block context to spans
pub async fn tracing_middleware(
    State(block_context): State<BlockContext>,
    request: Request,
    next: Next,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();

    // Get the current block number for the span
    let current_block = block_context.current_block_number();

    // Create span with block context
    let span = info_span!(
        "json_rpc_request",
        method = %method,
        path = %path,
        block.number = current_block,
        tx.hash = tracing::field::Empty,
    );

    async move { next.run(request).await }
        .instrument(span)
        .await
}

/// Update the tracing span with the corresponding contents of the tx queue
pub fn trace_tx_queue_contents(block_context: &BlockContext, tx_queue_contents: &TxQueueContents) {
    match tx_queue_contents {
        // If we receive a tx, add the tx hash to the current span
        TxQueueContents::Tx(tx, span) => {
            span.record("tx.hash", display(tx.tx_execution_id));
        }
        // Record the tx hash of the reorg
        TxQueueContents::Reorg(tx_execution_id, span) => {
            span.record("tx.hash", display(tx_execution_id));
        }
        // CommitHead events do not carry block env data, so there is nothing to trace.
        TxQueueContents::CommitHead(_, _) => {}
        // A new iteration indicates the next block env we should build on.
        TxQueueContents::NewIteration(new_iteration, _) => {
            block_context.update(&new_iteration.block_env);
        }
    }
}
