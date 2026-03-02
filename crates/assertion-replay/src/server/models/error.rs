use serde::Serialize;

/// Generic error response body for non-2xx handler responses.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorResponse {
    /// Human-readable error message.
    pub error: String,
}
