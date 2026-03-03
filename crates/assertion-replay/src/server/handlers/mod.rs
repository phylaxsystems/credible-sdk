use crate::http_error;

http_error!(INVALID_PAYLOAD_ERROR, BAD_REQUEST, "Invalid payload");
http_error!(REPLAY_FAILED_ERROR, INTERNAL_SERVER_ERROR, "Replay failed");

pub mod health;
pub mod replay;
