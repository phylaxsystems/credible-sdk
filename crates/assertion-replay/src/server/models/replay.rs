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
pub struct AssertionId(pub alloy::primitives::B256);

/// Request body for `POST /replay`.
///
/// All fields are optional to keep payload ergonomics simple
/// while still allowing per-request tuning.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayRequest {
    /// Assertions to inject into replay.
    ///
    /// These assertions are applied in-memory before replay begins and do not
    /// require historical indexer/DA records.
    #[serde(default)]
    pub assertions: Vec<Assertion>,
}

/// Assertion injected into replay state.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Assertion {
    /// Adopter address for this assertion.
    pub adopter: alloy::primitives::Address,
    /// Full deployment bytecode (creation code + encoded constructor args).
    pub deployment_bytecode: alloy::primitives::Bytes,
    /// Expected id (keccak256 of deployment bytecode).
    pub id: AssertionId,
}

/// Response body for current replay start preview.
#[derive(Debug, Clone, Serialize)]
pub struct ReplayStartBlockResponse {
    pub start_block: u64,
    pub head_block: u64,
    pub replay_window: u64,
}

#[cfg(test)]
mod tests {
    use super::{
        Assertion,
        AssertionId,
        ReplayRequest,
    };
    use alloy::primitives::{
        Address,
        B256,
        Bytes,
    };

    #[test]
    fn assertion_id_deserializes_from_hex() {
        let json = r#""0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa""#;
        let id: AssertionId = serde_json::from_str(json).expect("assertion id should parse");
        assert_eq!(id.0, B256::repeat_byte(0xaa));
    }

    #[test]
    fn replay_request_defaults_assertions_to_empty() {
        let request: ReplayRequest =
            serde_json::from_str("{}").expect("empty request should parse");
        assert!(request.assertions.is_empty());
    }

    #[test]
    fn replay_request_rejects_unknown_fields() {
        let payload = r#"{
            "assertion_ids":["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
        }"#;
        let error = serde_json::from_str::<ReplayRequest>(payload)
            .expect_err("unknown request fields should be rejected");
        assert!(
            error.to_string().contains("unknown field"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn assertion_deserializes() {
        let payload = r#"{
            "adopter":"0x1111111111111111111111111111111111111111",
            "deployment_bytecode":"0x6001600055",
            "id":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        }"#;
        let assertion_entry: Assertion =
            serde_json::from_str(payload).expect("assertion should parse");

        assert_eq!(assertion_entry.adopter, Address::repeat_byte(0x11));
        assert_eq!(
            assertion_entry.deployment_bytecode,
            Bytes::from(vec![0x60, 0x01, 0x60, 0x00, 0x55])
        );
        assert_eq!(assertion_entry.id.0, B256::repeat_byte(0xaa));
    }
}
