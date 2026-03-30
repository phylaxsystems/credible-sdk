use crate::{
    credible_config::{
        CredibleConfigError,
        CredibleToml,
        assertion_contract_name,
    },
    error::VerifyError,
};
use alloy_dyn_abi::{
    DynSolType,
    DynSolValue,
};
use alloy_json_abi::JsonAbi;
use alloy_primitives::{
    Bytes,
    hex,
};
use assertion_executor::ExecutorConfig;
use assertion_verification::{
    VerificationResult,
    VerificationStatus,
    verify_assertion,
};
use clap::ValueHint;
use pcl_common::args::CliArgs;
use pcl_phoundry::build_and_flatten::BuildAndFlattenArgs;
use serde::Serialize;
use std::path::{
    Path,
    PathBuf,
};

#[derive(clap::Parser, Debug)]
#[command(name = "verify", about = "Verify assertions locally before deployment")]
pub struct VerifyArgs {
    /// Assertion to verify (contract name or `file:contract`).
    /// Verifies all assertions from `credible.toml` when omitted.
    #[arg()]
    pub assertion: Option<String>,

    #[arg(
        long,
        value_hint = ValueHint::DirPath,
        default_value = ".",
        help = "Project root directory"
    )]
    pub root: PathBuf,

    #[arg(
        short = 'c',
        long = "config",
        value_hint = ValueHint::FilePath,
        default_value = "assertions/credible.toml",
        help = "Path to credible.toml, relative to root or absolute"
    )]
    pub config: PathBuf,

    #[arg(long, num_args = 1.., help = "Constructor arguments for the assertion")]
    pub args: Vec<String>,

    #[arg(long, help = "Emit machine-readable JSON output")]
    pub json: bool,
}

struct VerifyInput {
    name: String,
    display_name: String,
    bytecode: Bytes,
}

#[derive(Serialize)]
struct VerifyJsonOutput {
    status: &'static str,
    assertions: Vec<VerifyJsonAssertion>,
    total: usize,
    passed: usize,
    failed: usize,
}

#[derive(Serialize)]
struct VerifyJsonAssertion {
    name: String,
    #[serde(flatten)]
    result: VerificationResult,
}

impl VerifyArgs {
    pub fn run(&self, cli_args: &CliArgs) -> Result<(), VerifyError> {
        let json_output = cli_args.json_output() || self.json;
        let root = std::fs::canonicalize(&self.root).map_err(|e| {
            VerifyError::Io {
                message: format!("Project root not found: {}", self.root.display()),
                source: e,
            }
        })?;

        if self.assertion.is_none() && !self.args.is_empty() {
            return Err(VerifyError::Config(CredibleConfigError::Invalid(
                "--args can only be used when verifying a specific assertion".to_string(),
            )));
        }

        let inputs = match &self.assertion {
            Some(assertion) => self.build_single(assertion, &root)?,
            None => Self::build_from_toml(&root, &self.config)?,
        };

        let executor_config = ExecutorConfig::default();
        let outputs: Vec<(String, String, VerificationResult)> = inputs
            .into_iter()
            .map(|input| {
                let result = verify_assertion(&input.bytecode, &executor_config);
                (input.name, input.display_name, result)
            })
            .collect();

        let total = outputs.len();
        let failed = outputs
            .iter()
            .filter(|(_, _, r)| r.status != VerificationStatus::Success)
            .count();
        let passed = total - failed;

        if json_output {
            let output = VerifyJsonOutput {
                status: if failed == 0 { "success" } else { "failure" },
                assertions: outputs
                    .iter()
                    .map(|(name, _, result)| {
                        VerifyJsonAssertion {
                            name: name.clone(),
                            result: result.clone(),
                        }
                    })
                    .collect(),
                total,
                passed,
                failed,
            };
            println!("{}", serde_json::to_string_pretty(&output)?);
        } else {
            println!("pcl verify \u{2014} Assertion Verification\n");
            for (_, display_name, result) in &outputs {
                print_human_result(display_name, result);
            }
            if failed == 0 {
                println!(
                    "All {} assertion{} verified successfully.",
                    total,
                    if total == 1 { "" } else { "s" }
                );
            } else {
                println!(
                    "{failed} of {total} assertion{} failed verification.",
                    if total == 1 { "" } else { "s" }
                );
            }
        }

        if failed > 0 {
            std::process::exit(1);
        }

        Ok(())
    }

    fn build_single(&self, assertion: &str, root: &Path) -> Result<Vec<VerifyInput>, VerifyError> {
        let contract_name = parse_assertion_name(assertion);
        let output = BuildAndFlattenArgs {
            root: Some(root.to_path_buf()),
            assertion_contract: contract_name.clone(),
        }
        .run()
        .map_err(VerifyError::BuildFailed)?;

        let bytecode = build_deployment_bytecode(&output.bytecode, &output.abi, &self.args)?;
        let display_name = format_display_name(&contract_name, &self.args);

        Ok(vec![VerifyInput {
            name: contract_name,
            display_name,
            bytecode,
        }])
    }

    fn build_from_toml(root: &Path, config: &Path) -> Result<Vec<VerifyInput>, VerifyError> {
        let config_path = root.join(config);
        let credible = CredibleToml::from_path(&config_path)?;

        let mut inputs = Vec::new();
        for contract in credible.contracts.values() {
            for assertion in &contract.assertions {
                let contract_name = assertion_contract_name(&assertion.file)?;
                let output = BuildAndFlattenArgs {
                    root: Some(root.to_path_buf()),
                    assertion_contract: contract_name.clone(),
                }
                .run()
                .map_err(VerifyError::BuildFailed)?;

                let bytecode =
                    build_deployment_bytecode(&output.bytecode, &output.abi, &assertion.args)?;
                let display_name = format_display_name(&contract_name, &assertion.args);

                inputs.push(VerifyInput {
                    name: contract_name,
                    display_name,
                    bytecode,
                });
            }
        }

        if inputs.is_empty() {
            return Err(VerifyError::Config(CredibleConfigError::Invalid(
                "No assertions found in credible.toml".to_string(),
            )));
        }

        Ok(inputs)
    }
}

/// Parses a CLI assertion argument into a contract name.
///
/// - `ContractName` -> `ContractName`
/// - `file.sol:ContractName` -> `ContractName`
fn parse_assertion_name(arg: &str) -> String {
    if let Some((_, contract_name)) = arg.rsplit_once(':') {
        contract_name.to_string()
    } else {
        arg.to_string()
    }
}

fn build_deployment_bytecode(
    bytecode_hex: &str,
    abi: &JsonAbi,
    args: &[String],
) -> Result<Bytes, VerifyError> {
    let mut bytecode = hex::decode(bytecode_hex)
        .map_err(|e| VerifyError::AbiEncode(format!("invalid bytecode hex: {e}")))?;

    if !args.is_empty() {
        let encoded = encode_constructor_args(abi, args)?;
        bytecode.extend_from_slice(&encoded);
    }

    Ok(Bytes::from(bytecode))
}

fn encode_constructor_args(abi: &JsonAbi, args: &[String]) -> Result<Vec<u8>, VerifyError> {
    let constructor = abi.constructor.as_ref().ok_or_else(|| {
        VerifyError::AbiEncode(
            "contract has no constructor but arguments were provided".to_string(),
        )
    })?;

    if constructor.inputs.len() != args.len() {
        return Err(VerifyError::AbiEncode(format!(
            "expected {} constructor argument{}, got {}",
            constructor.inputs.len(),
            if constructor.inputs.len() == 1 {
                ""
            } else {
                "s"
            },
            args.len()
        )));
    }

    let values: Vec<DynSolValue> = constructor
        .inputs
        .iter()
        .zip(args.iter())
        .map(|(param, arg)| {
            let sol_type: DynSolType = param.ty.parse().map_err(|e| {
                VerifyError::AbiEncode(format!("unsupported type '{}': {e}", param.ty))
            })?;
            sol_type.coerce_str(arg).map_err(|e| {
                VerifyError::AbiEncode(format!("failed to parse '{}' as {}: {e}", arg, param.ty))
            })
        })
        .collect::<Result<_, _>>()?;

    Ok(DynSolValue::Tuple(values).abi_encode_params())
}

fn format_display_name(name: &str, args: &[String]) -> String {
    if args.is_empty() {
        name.to_string()
    } else {
        let args_display: Vec<_> = args.iter().map(|a| abbreviate_arg(a)).collect();
        format!("{}({})", name, args_display.join(", "))
    }
}

fn abbreviate_arg(arg: &str) -> String {
    if arg.len() > 10 && arg.starts_with("0x") {
        format!("{}...{}", &arg[..6], &arg[arg.len() - 4..])
    } else {
        arg.to_string()
    }
}

fn status_str(status: VerificationStatus) -> &'static str {
    match status {
        VerificationStatus::Success => "success",
        VerificationStatus::DeploymentFailure => "deployment_failure",
        VerificationStatus::NoTriggers => "no_triggers",
        VerificationStatus::MissingAssertionSpec => "missing_assertion_spec",
        VerificationStatus::InvalidAssertionSpec => "invalid_assertion_spec",
    }
}

fn print_human_result(display_name: &str, result: &VerificationResult) {
    if result.status == VerificationStatus::Success {
        println!("  \u{2713} {display_name}");
        if let Some(triggers) = &result.triggers {
            println!("    triggers:");
            for (selector, trigger_types) in triggers {
                println!("      {selector} \u{2192} {trigger_types}");
            }
        }
    } else {
        println!("  \u{2717} {display_name}");
        println!("    status: {}", status_str(result.status));
        if let Some(error) = &result.error {
            println!("    error: {error}");
        }
    }
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_json_abi::{
        Constructor,
        Param,
        StateMutability,
    };

    #[test]
    fn parse_assertion_name_bare() {
        assert_eq!(parse_assertion_name("MyContract"), "MyContract");
    }

    #[test]
    fn parse_assertion_name_qualified() {
        assert_eq!(
            parse_assertion_name("MyContract.a.sol:MyContract"),
            "MyContract"
        );
    }

    #[test]
    fn format_display_name_no_args() {
        assert_eq!(format_display_name("Foo", &[]), "Foo");
    }

    #[test]
    fn format_display_name_with_args() {
        let args = vec!["0x1234567890abcdef".to_string(), "42".to_string()];
        assert_eq!(format_display_name("Foo", &args), "Foo(0x1234...cdef, 42)");
    }

    #[test]
    fn abbreviate_short_arg() {
        assert_eq!(abbreviate_arg("42"), "42");
        assert_eq!(abbreviate_arg("0xshort"), "0xshort");
    }

    #[test]
    fn abbreviate_long_hex_arg() {
        let arg = "0x1234567890abcdef";
        assert_eq!(abbreviate_arg(arg), "0x1234...cdef");
    }

    #[test]
    fn encode_constructor_args_rejects_missing_constructor() {
        let abi = JsonAbi {
            constructor: None,
            ..Default::default()
        };
        let err = encode_constructor_args(&abi, &["42".to_string()]).unwrap_err();
        assert!(err.to_string().contains("no constructor"));
    }

    #[test]
    fn encode_constructor_args_rejects_wrong_count() {
        let abi = JsonAbi {
            constructor: Some(Constructor {
                inputs: vec![Param {
                    ty: "uint256".to_string(),
                    name: "x".to_string(),
                    components: vec![],
                    internal_type: None,
                }],
                state_mutability: StateMutability::NonPayable,
            }),
            ..Default::default()
        };
        let err = encode_constructor_args(&abi, &["1".to_string(), "2".to_string()]).unwrap_err();
        assert!(err.to_string().contains("expected 1"));
    }

    #[test]
    fn encode_constructor_args_encodes_address() {
        let abi = JsonAbi {
            constructor: Some(Constructor {
                inputs: vec![Param {
                    ty: "address".to_string(),
                    name: "addr".to_string(),
                    components: vec![],
                    internal_type: None,
                }],
                state_mutability: StateMutability::NonPayable,
            }),
            ..Default::default()
        };
        let result = encode_constructor_args(
            &abi,
            &["0x0000000000000000000000000000000000000001".to_string()],
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 32); // ABI-encoded address is 32 bytes
    }

    #[test]
    fn build_deployment_bytecode_no_args() {
        let abi = JsonAbi::default();
        let result = build_deployment_bytecode("6001", &abi, &[]).unwrap();
        assert_eq!(result.as_ref(), &[0x60, 0x01]);
    }

    #[test]
    fn build_deployment_bytecode_with_0x_prefix() {
        let abi = JsonAbi::default();
        let result = build_deployment_bytecode("0x6001", &abi, &[]).unwrap();
        assert_eq!(result.as_ref(), &[0x60, 0x01]);
    }
}
