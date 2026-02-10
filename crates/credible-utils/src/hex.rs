pub fn encode_hex_prefixed(bytes: impl AsRef<[u8]>) -> String {
    format!("0x{}", hex::encode(bytes.as_ref()))
}

/// Decode a hex string, ignoring a leading "0x" prefix if present.
///
/// # Errors
///
/// Returns an error if the input (after trimming a leading "0x") is not valid hex.
pub fn decode_hex_trimmed_0x(value: &str) -> Result<Vec<u8>, hex::FromHexError> {
    hex::decode(value.trim_start_matches("0x"))
}
