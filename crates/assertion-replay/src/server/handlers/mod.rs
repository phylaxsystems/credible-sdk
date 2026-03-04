use crate::http_error;

http_error!(INVALID_PAYLOAD_ERROR, BAD_REQUEST, "Invalid payload");
http_error!(REPLAY_FAILED_ERROR, INTERNAL_SERVER_ERROR, "Replay failed");
http_error!(
    START_BLOCK_QUERY_FAILED_ERROR,
    INTERNAL_SERVER_ERROR,
    "Failed to fetch replay start block"
);

pub mod health;
pub mod replay;
pub mod start_block;
