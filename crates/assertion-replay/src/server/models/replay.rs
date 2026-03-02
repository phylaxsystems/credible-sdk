use serde::{
    Deserialize,
    Serialize,
};

/// Canonical assertion identifier.
///
/// This wraps the executor's `B256` type, which represents:
/// `keccak256(assertion deployment bytecode)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AssertionId(pub assertion_executor::primitives::B256);

/// Request body for `POST /replay`.
///
/// All fields are optional overrides to keep payload ergonomics simple
/// while still allowing per-request tuning.
#[derive(Debug, Default, Clone, Deserialize)]
pub struct ReplayRequest {
    /// Assertion IDs to scope replaying.
    /// Each ID is a typed `AssertionId` (`B256`).
    #[serde(default)]
    pub assertion_ids: Vec<AssertionId>,
}

/// Response body for successful `POST /replay`.
#[derive(Debug, Clone, Serialize)]
pub struct ReplayResponse {
    /// Uppercase operation status.
    pub status: &'static str,
}

impl ReplayResponse {
    /// Builds a successful replay response.
    pub const fn ok() -> Self {
        Self { status: "OK" }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AssertionId,
        ReplayRequest,
        ReplayResponse,
    };
    use assertion_executor::primitives::B256;

    #[test]
    fn assertion_id_deserializes_from_hex() {
        let json = r#""0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa""#;
        let id: AssertionId = serde_json::from_str(json).expect("assertion id should parse");
        assert_eq!(id.0, B256::repeat_byte(0xaa));
    }

    #[test]
    fn replay_request_defaults_assertion_ids_to_empty() {
        let request: ReplayRequest =
            serde_json::from_str("{}").expect("empty request should parse");
        assert!(request.assertion_ids.is_empty());
    }

    #[test]
    fn replay_response_ok_serializes_status() {
        let response = ReplayResponse::ok();
        let json = serde_json::to_string(&response).expect("response should serialize");
        assert_eq!(json, r#"{"status":"OK"}"#);
    }
}
