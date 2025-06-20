use alloy::primitives::Bytes;
use alloy_dyn_abi::{
    DynSolType,
    DynSolValue,
    JsonAbiExt,
};
use alloy_json_abi::{
    Function,
    Param,
};

#[derive(thiserror::Error, Debug)]
pub enum EncodeArgsError {
    #[error("Signature not a constructor: {0}")]
    SignatureNotAConstructor(String),
    #[error("Dynamic ABI Error: {0}")]
    DynAbiError(#[from] alloy_dyn_abi::Error),
    #[error("Error parsing ABI types: {0}")]
    ParseAbiError(#[from] alloy_dyn_abi::parser::Error),
}

type Result<T> = std::result::Result<T, EncodeArgsError>;

pub fn encode_constructor_args(sig: &str, args: Vec<String>) -> Result<Bytes> {
    let trimmed_sig = sig.trim();
    if trimmed_sig.is_empty() || trimmed_sig == "constructor()" {
        return Ok(Bytes::new());
    }
    if !trimmed_sig.starts_with("constructor(") {
        return Err(EncodeArgsError::SignatureNotAConstructor(
            "signature must start with 'constructor('".to_string(),
        ));
    }
    let func = Function::parse(sig)?;
    let sol_values = encode_args(&func.inputs, args)?;

    let encoded = Function::abi_encode_input_raw(&func, &sol_values)?;
    Ok(Bytes::from(encoded))
}

pub fn encode_args<I, S>(inputs: &[Param], args: I) -> Result<Vec<DynSolValue>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    std::iter::zip(inputs, args)
        .map(|(input, arg)| coerce_value(&input.selector_type(), arg.as_ref()))
        .collect()
}

/// Helper function to coerce a value to a [DynSolValue] given a type string
pub fn coerce_value(ty: &str, arg: &str) -> Result<DynSolValue> {
    let ty = DynSolType::parse(ty)?;
    Ok(DynSolType::coerce_str(&ty, arg)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_constructor_args() {
        let sig = "constructor(uint256, string, uint256[])";
        let args = vec![
            "0".to_string(),
            "Hello".to_string(),
            "[1, 2, 3]".to_string(),
        ];
        let encoded = encode_constructor_args(sig, args).unwrap();
        assert_eq!(
            hex::encode(encoded),
            "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000a0000000000000000000000000000000000000000000000000000000000000000548656c6c6f0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000003"
        );
    }

    #[test]
    fn test_encode_constructor_args_errors() {
        // Test invalid signature
        let result = encode_constructor_args("not_a_constructor", vec!["0".to_string()]);
        assert!(result.is_err());

        // Test type mismatch
        let result =
            encode_constructor_args("constructor(uint256)", vec!["not_a_number".to_string()]);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_value_uint() {
        let result = coerce_value("uint256", "42");
        assert!(result.is_ok());
    }

    #[test]
    fn test_coerce_value_bool() {
        let result = coerce_value("bool", "true");
        assert!(result.is_ok());
    }

    #[test]
    fn test_coerce_value_address() {
        // Use a properly formatted Ethereum address
        let result = coerce_value("address", "0x0000000000000000000000000000000000000001");
        assert!(result.is_ok());
    }

    #[test]
    fn test_coerce_value_address_no_prefix() {
        // Use a properly formatted Ethereum address
        let result = coerce_value("address", "0000000000000000000000000000000000000001");
        assert!(result.is_ok());
    }

    #[test]
    fn test_coerce_value_bytes32() {
        // Use a properly formatted 32-byte hex string
        let result = coerce_value(
            "bytes32",
            "0x0000000000000000000000000000000000000000000000000000000000000001",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_coerce_value_string() {
        let result = coerce_value("string", "Hello");
        assert!(result.is_ok());
    }

    #[test]
    fn test_coerce_value() {
        // Test basic type coercion
        let _ = coerce_value("uint256", "42").unwrap();

        // Test with valid values for each type
        let types = ["bool", "address", "bytes32", "string"];
        let values = [
            "true",
            "0x0000000000000000000000000000000000000001",
            "0x0000000000000000000000000000000000000000000000000000000000000001",
        ];

        for (ty, val) in types.iter().zip(values.iter()) {
            let result = coerce_value(ty, val);
            assert!(result.is_ok());
        }
    }
}
