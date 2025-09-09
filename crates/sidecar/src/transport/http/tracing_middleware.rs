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
        // If we receive a block, update the block context
        TxQueueContents::Block(block, _) => block_context.update(block),
        // If we receive a tx, add the tx hash to the current span
        TxQueueContents::Tx(tx, span) => {
            span.record("tx.hash", tx.tx_hash.to_string());
        }
    }
}
