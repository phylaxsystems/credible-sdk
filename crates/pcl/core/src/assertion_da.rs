//! Module for handling assertion submission to the Data Availability (DA) layer.
//!
//! This module provides functionality to submit assertions to a Data Availability layer,
//! which is a crucial component of the Credible Layer system. It handles the process
//! of building, flattening, and submitting assertions along with their source code
//! to be stored in the DA layer.

use clap::{
    Parser,
    ValueHint,
};
use colored::Colorize;
use indicatif::{
    ProgressBar,
    ProgressStyle,
};
use pcl_common::args::CliArgs;
use pcl_phoundry::build_and_flatten::{
    BuildAndFlatOutput,
    BuildAndFlattenArgs,
};
use serde_json::json;
use tokio::time::Duration;

use assertion_da_client::{
    DaClient,
    DaClientError,
    DaSubmissionResponse,
};

use crate::{
    config::{
        AssertionForSubmission,
        AssertionKey,
        CliConfig,
    },
    error::DaSubmitError,
};

/// Macro that defines the default DA URL - can be used in concat! macros
#[macro_export]
macro_rules! default_da_url {
    () => {
        "https://demo-21-assertion-da.phylax.systems"
    };
}

pub const DEFAULT_DA_URL: &str = default_da_url!();

/// Command-line arguments for storing assertions in the Data Availability layer.
///
/// This struct handles the configuration needed to submit assertions to the DA layer,
/// including the DA server URL and build arguments for the assertion.
#[derive(Parser)]
#[clap(
    name = "store",
    about = "Submit the Assertion bytecode and source code to be stored by the Assertion DA of the Credible Layer"
)]
pub struct DaStoreArgs {
    /// URL of the assertion-DA server
    #[clap(
        long = "da-url",
        short = 'u',
        env = "PCL_DA_URL",
        value_hint = ValueHint::Url,
        default_value = DEFAULT_DA_URL
    )]
    pub da_url: String,

    /// Build and flatten arguments for the assertion
    #[clap(flatten)]
    pub args: BuildAndFlattenArgs,

    /// Constructor arguments for the assertion contract
    #[clap(help = "Constructor arguments for the assertion contract.
                         Format: <ARG0> <ARG1> <ARG2>")]
    pub constructor_args: Vec<String>,
}

impl DaStoreArgs {
    /// Creates and configures a progress spinner for displaying operation status.
    ///
    /// Returns a configured `ProgressBar` instance with a custom spinner style.
    fn create_spinner() -> ProgressBar {
        let spinner = ProgressBar::new_spinner();
        spinner.set_style(
            ProgressStyle::default_spinner()
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
                .template("{spinner} {msg}")
                .expect("Failed to set spinner style"),
        );
        spinner.enable_steady_tick(Duration::from_millis(80));
        spinner
    }

    /// Handles HTTP error responses from the DA layer.
    ///
    /// # Arguments
    /// * `status_code` - The HTTP status code received from the DA layer
    /// * `spinner` - The progress spinner to update with error messages
    ///
    /// # Returns
    /// * `Result<(), Box<DaSubmitError>>` - Ok if the error was handled, Err otherwise
    fn handle_http_error(
        status_code: u16,
        spinner: &ProgressBar,
    ) -> Result<(), Box<DaSubmitError>> {
        match status_code {
            401 => {
                spinner.finish_with_message(
                    "❌ Assertion submission failed! Unauthorized. Please run `pcl auth login`.",
                );
                Ok(())
            }
            _ => Err(Box::new(DaSubmitError::HttpError(status_code))),
        }
    }

    /// Displays the assertion information and next steps after successful submission.
    ///
    /// # Arguments
    /// * `assertion` - The assertion that was successfully submitted
    /// * `json_output` - Whether to output in JSON format
    fn display_success_info(&self, assertion: &AssertionForSubmission, json_output: bool) {
        if json_output {
            let json_output = json!({
                "status": "success",
                "assertion_contract": assertion.assertion_contract,
                "assertion_id": assertion.assertion_id,
                "signature": assertion.signature,
                "constructor_args": assertion.constructor_args,
            });
            println!("{}", serde_json::to_string_pretty(&json_output).unwrap());
        } else {
            println!("\n\n{}", "Assertion Information".bold().green());
            println!("{}", "===================".green());
            println!("{assertion}");
            println!("\nSubmitted to assertion DA: {}", self.da_url);

            println!("\n{}", "Next Steps:".bold());
            println!("Submit this assertion to a project with:");

            let assertion_key = AssertionKey {
                assertion_name: assertion.assertion_contract.clone(),
                constructor_args: assertion.constructor_args.clone(),
            };

            println!(
                "  {} submit -a '{}' -p <project_name>",
                "pcl".cyan().bold(),
                assertion_key
            );
            println!(
                "Visit the Credible Layer DApp to link the assertion on-chain and enforce it:"
            );
            println!("  {}", "https://dapp.phylax.systems".cyan().bold());
        }
    }

    /// Builds and flattens the assertion source code.
    ///
    /// # Returns
    /// * `Result<BuildAndFlatOutput, DaSubmitError>` - The build output or error
    fn build_and_flatten_assertion(&self) -> Result<BuildAndFlatOutput, DaSubmitError> {
        self.args
            .run()
            .map_err(|e| DaSubmitError::PhoundryError(Box::new(*e)))
    }

    /// Creates a DA client with appropriate authentication.
    ///
    /// # Arguments
    /// * `config` - Configuration containing authentication details
    ///
    /// # Returns
    /// * `Result<DaClient, DaClientError>` - The configured client or error
    fn create_da_client(&self, config: &CliConfig) -> Result<DaClient, DaClientError> {
        match &config.auth {
            Some(auth) => {
                DaClient::new_with_auth(&self.da_url, &format!("Bearer {}", auth.access_token))
            }
            None => DaClient::new(&self.da_url),
        }
    }

    /// Submits the assertion to the DA layer.
    ///
    /// # Arguments
    /// * `client` - The DA client to use for submission
    /// * `build_output` - The build output containing flattened source
    /// * `spinner` - The progress spinner to update
    ///
    /// # Returns
    /// * `Result<(), DaSubmitError>` - Success or error
    async fn submit_to_da(
        &self,
        client: &DaClient,
        build_output: &BuildAndFlatOutput,
        spinner: &ProgressBar,
    ) -> Result<DaSubmissionResponse, DaSubmitError> {
        let constructor_inputs = build_output
            .abi
            .constructor()
            .map(|constructor| constructor.inputs.clone())
            .unwrap_or_default();

        if constructor_inputs.len() != self.constructor_args.len() {
            return Err(DaSubmitError::InvalidConstructorArgs(
                constructor_inputs.len(),
                self.constructor_args.len(),
            ));
        }

        let joined_inputs = constructor_inputs
            .iter()
            .map(|input| input.selector_type().clone())
            .collect::<Vec<_>>()
            .join(",");

        let constructor_signature = format!("constructor({joined_inputs})");

        match client
            .submit_assertion_with_args(
                self.args.assertion_contract.clone(),
                build_output.flattened_source.clone(),
                build_output.compiler_version.clone(),
                constructor_signature,
                self.constructor_args.clone(),
            )
            .await
        {
            Ok(res) => Ok(res),
            Err(err) => {
                match &err {
                    DaClientError::Reqwest(reqwest_err)
                    | DaClientError::ReqwestResponse(reqwest_err)
                    | DaClientError::Build(reqwest_err) => {
                        if let Some(status) = reqwest_err.status() {
                            Self::handle_http_error(status.as_u16(), spinner)?;
                        }
                    }
                    DaClientError::UrlParse(_) => {
                        spinner.finish_with_message("❌ Invalid DA server URL");
                    }
                    DaClientError::JsonError(_) => {
                        spinner.finish_with_message("❌ Failed to parse server response");
                    }
                    DaClientError::JsonRpcError { code, message } => {
                        spinner.finish_with_message(format!(
                            "❌ Server error (code {code}): {message}"
                        ));
                    }
                    DaClientError::InvalidResponse(msg) => {
                        spinner.finish_with_message(format!("❌ Invalid server response: {msg}"));
                    }
                }
                Err(DaSubmitError::DaClientError(err))
            }
        }
    }

    /// Updates the configuration with the submission result.
    ///
    /// # Arguments
    /// * `config` - The configuration to update
    /// * `spinner` - The progress spinner to update
    /// * `json_output` - Whether to output in JSON format
    fn update_config<A: ToString + ?Sized, S: ToString + ?Sized>(
        &self,
        config: &mut CliConfig,
        assertion_id: &A,
        signature: &S,
        spinner: &ProgressBar,
        json_output: bool,
    ) {
        let assertion_for_submission = AssertionForSubmission {
            assertion_contract: self.args.assertion_contract.clone(),
            assertion_id: assertion_id.to_string(),
            signature: signature.to_string(),
            constructor_args: self.constructor_args.clone(),
        };

        config.add_assertion_for_submission(assertion_for_submission.clone());

        if !json_output {
            spinner.finish_with_message("✅ Assertion successfully submitted!");
        }

        self.display_success_info(&assertion_for_submission, json_output);
    }

    /// Executes the assertion storage process.
    ///
    /// This method:
    /// 1. Sets up dependencies
    /// 2. Stores the assertions
    /// 3. Submits the selected assertions to the Dapp from the CLI
    ///
    /// # Arguments
    /// * `cli_args` - General CLI arguments
    /// * `config` - Configuration containing assertions and auth details
    ///
    /// # Returns
    /// * `Result<(), DaSubmitError>` - Success or specific error
    ///
    /// # Errors
    /// * Returns `DaSubmitError` if the build process fails
    /// * Returns `DaSubmitError` if the submission to DA layer fails
    /// * Returns `DaSubmitError` if there are authentication issues
    pub async fn run(
        &self,
        cli_args: &CliArgs,
        config: &mut CliConfig,
    ) -> Result<(), DaSubmitError> {
        let json_output = cli_args.json_output();
        let spinner = if json_output {
            ProgressBar::hidden()
        } else {
            Self::create_spinner()
        };

        if !json_output {
            spinner.set_message("Submitting assertion to DA...");
        }

        let build_output = self.build_and_flatten_assertion()?;
        let client = self
            .create_da_client(config)
            .map_err(DaSubmitError::DaClientError)?;
        let submission_response = self.submit_to_da(&client, &build_output, &spinner).await?;
        self.update_config(
            config,
            &submission_response.id,
            &submission_response.prover_signature,
            &spinner,
            json_output,
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::cast_possible_wrap)]
    use super::*;
    use crate::config::UserAuth;
    use alloy_primitives::Address;
    use chrono::DateTime;
    use clap::Parser;
    use mockito::Server;
    use std::{
        io::Write,
        time::{
            SystemTime,
            UNIX_EPOCH,
        },
    };

    /// Creates a test configuration with authentication
    fn create_test_config() -> CliConfig {
        CliConfig {
            auth: Some(UserAuth {
                access_token: "test_token".to_string(),
                refresh_token: "test_refresh".to_string(),
                user_address: Address::from_slice(&[0; 20]),
                expires_at: DateTime::from_timestamp(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64,
                    0,
                )
                .unwrap(),
            }),
            ..Default::default()
        }
    }

    /// Creates test build and flatten arguments
    fn create_test_build_args() -> BuildAndFlattenArgs {
        BuildAndFlattenArgs {
            assertion_contract: "MockAssertion".to_string(),
            root: Some("../../../testdata/mock-protocol".parse().unwrap()),
        }
    }

    /// Helper to capture stdout for testing
    #[allow(dead_code, unused_variables, unused_mut)]
    fn capture_stdout<F>(f: F) -> String
    where
        F: FnOnce(),
    {
        let mut output = Vec::new();
        {
            let mut writer = std::io::BufWriter::new(&mut output);
            let original_stdout = std::io::stdout();
            let mut handle = original_stdout.lock();
            let _ = handle.write_all(b"");
            f();
        }
        String::from_utf8(output).unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_run_with_malformed_response() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .with_status(200)
            .with_body("invalid json")
            .create();

        let mut config = create_test_config();
        let args = DaStoreArgs {
            da_url: server.url(),
            args: create_test_build_args(),
            constructor_args: vec![Address::random().to_string()],
        };

        let cli_args = CliArgs::default();
        let result = args.run(&cli_args, &mut config).await;
        assert!(result.is_err());
        mock.assert();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_json_output_structure() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .with_status(200)
            .with_body(
                r#"{
  "jsonrpc": "2.0",
  "result": {
    "prover_signature": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "id": "0x0000000000000000000000000000000000000000000000000000000000000000"
  },
  "id": 1
            }"#,
            )
            .with_header("content-type", "application/json")
            .create();

        let mut config = create_test_config();
        let args = DaStoreArgs {
            da_url: server.url(),
            args: create_test_build_args(),
            constructor_args: vec![Address::random().to_string()],
        };

        let cli_args = CliArgs::parse_from(["test", "--json"]);

        // Run the command and capture the output
        let result = args.run(&cli_args, &mut config).await;
        assert!(result.is_ok(), "{result:#?}");

        // Verify the config was updated correctly
        let assertion = config.assertions_for_submission.values().next().unwrap();
        assert_eq!(assertion.assertion_contract, "MockAssertion");
        assert_eq!(
            assertion.assertion_id,
            "0x0000000000000000000000000000000000000000000000000000000000000000"
        );
        assert_eq!(
            assertion.signature,
            "0x0000000000000000000000000000000000000000000000000000000000000000"
        );
        mock.assert();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_invalid_constructor_args() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .with_status(400)
            .with_body(
                r#"{
  "jsonrpc": "2.0",
  "error": {
    "code": -32602,
    "message": "Invalid constructor arguments"
  },
  "id": 0
            }"#,
            )
            .with_header("content-type", "application/json")
            .create();

        let mut config = create_test_config();
        let args = DaStoreArgs {
            da_url: server.url(),
            args: create_test_build_args(),
            constructor_args: vec!["invalid_arg".to_string()],
        };

        let cli_args = CliArgs::default();
        let result = args.run(&cli_args, &mut config).await;
        assert!(result.is_err());
        mock.assert();
    }

    #[tokio::test]
    async fn test_handle_http_error_unauthorized() {
        let spinner = DaStoreArgs::create_spinner();
        let result = DaStoreArgs::handle_http_error(401, &spinner);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_http_error_other() {
        let spinner = DaStoreArgs::create_spinner();
        let result = DaStoreArgs::handle_http_error(500, &spinner);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_display_success_info() {
        let args = DaStoreArgs {
            da_url: "https://demo-21-assertion-da.phylax.systems".to_string(),
            args: create_test_build_args(),
            constructor_args: vec!["arg1".to_string(), "arg2".to_string()],
        };

        let assertion = AssertionForSubmission {
            assertion_contract: "test_assertion".to_string(),
            assertion_id: "test_id".to_string(),
            signature: "test_signature".to_string(),
            constructor_args: vec!["arg1".to_string(), "arg2".to_string()],
        };

        // This test just ensures the function doesn't panic
        args.display_success_info(&assertion, false);
        args.display_success_info(&assertion, true);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_run_with_auth() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .with_status(200)
            .with_body(
                r#"{
  "jsonrpc": "2.0",
  "result": {
    "prover_signature": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "id": "0x0000000000000000000000000000000000000000000000000000000000000000"
  },
  "id": 1
            }"#,
            )
            .with_header("content-type", "application/json")
            .create();

        let mut config = create_test_config();
        let args = create_test_build_args();

        let args = DaStoreArgs {
            da_url: server.url(),
            args,
            constructor_args: vec![Address::random().to_string()],
        };

        let cli_args = CliArgs::default();
        let result = args.run(&cli_args, &mut config).await;
        assert!(result.is_ok(), "Expected success but got: {result:?}");
        mock.assert();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_run_with_auth_json_output() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .with_status(200)
            .with_body(
                r#"{
  "jsonrpc": "2.0",
  "result": {
    "prover_signature": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "id": "0x0000000000000000000000000000000000000000000000000000000000000000"
  },
  "id": 1
            }"#,
            )
            .with_header("content-type", "application/json")
            .create();

        let mut config = create_test_config();
        let args = DaStoreArgs {
            da_url: server.url(),
            args: create_test_build_args(),
            constructor_args: vec![Address::random().to_string()],
        };

        // Create CLI args with JSON output enabled
        let cli_args = CliArgs::parse_from(["test", "--json"]);
        assert!(cli_args.json_output());

        let result = args.run(&cli_args, &mut config).await;
        assert!(result.is_ok(), "Expected success but got: {result:?}");
        mock.assert();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_run_unauthorized() {
        let mut server = Server::new_async().await;
        let mock = server.mock("POST", "/").with_status(401).create();

        let mut config = create_test_config();
        config.auth = None; // Simulate no auth
        let args = DaStoreArgs {
            da_url: server.url(),
            args: create_test_build_args(),
            constructor_args: vec![Address::random().to_string()],
        };

        let cli_args = CliArgs::default();
        let result = args.run(&cli_args, &mut config).await;

        assert!(result.is_err());
        mock.assert();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_run_server_error() {
        let mut server = Server::new_async().await;
        let mock = server.mock("POST", "/").with_status(500).create();

        let mut config = create_test_config();
        let args = DaStoreArgs {
            da_url: server.url(),
            args: create_test_build_args(),
            constructor_args: vec![Address::random().to_string()],
        };

        let cli_args = CliArgs::default();
        let result = args.run(&cli_args, &mut config).await;
        assert!(result.is_err(), "Expected error but got: {result:?}");
        mock.assert();
    }

    #[tokio::test]
    async fn test_create_spinner() {
        let spinner = DaStoreArgs::create_spinner();
        assert_eq!(spinner.message(), "");
        spinner.set_message("test");
        assert_eq!(spinner.message(), "test");
    }

    #[tokio::test]
    async fn test_create_da_client_with_auth() {
        let args = DaStoreArgs {
            da_url: "https://demo-21-assertion-da.phylax.systems".to_string(),
            args: BuildAndFlattenArgs::default(),
            constructor_args: vec!["arg1".to_string(), "arg2".to_string()],
        };

        let config = CliConfig {
            auth: Some(UserAuth {
                access_token: "test_token".to_string(),
                refresh_token: "test_refresh".to_string(),
                user_address: Address::from_slice(&[0; 20]),
                expires_at: DateTime::from_timestamp(1672502400, 0).unwrap(),
            }),
            ..Default::default()
        };

        let client = args.create_da_client(&config);
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_create_da_client_without_auth() {
        let args = DaStoreArgs {
            da_url: "https://demo-21-assertion-da.phylax.systems".to_string(),
            args: BuildAndFlattenArgs::default(),
            constructor_args: vec!["arg1".to_string(), "arg2".to_string()],
        };

        let config = CliConfig::default();
        let client = args.create_da_client(&config);
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_update_config() {
        let args = DaStoreArgs {
            da_url: "https://demo-21-assertion-da.phylax.systems".to_string(),
            args: BuildAndFlattenArgs {
                assertion_contract: "test_assertion".to_string(),
                ..BuildAndFlattenArgs::default()
            },
            constructor_args: vec!["arg1".to_string(), "arg2".to_string()],
        };

        let mut config = CliConfig::default();
        let spinner = DaStoreArgs::create_spinner();

        args.update_config(&mut config, "test_id", "test_signature", &spinner, false);

        assert_eq!(config.assertions_for_submission.len(), 1);

        let expected_key = "test_assertion(arg1,arg2)".to_string().into();

        let assertion = config.assertions_for_submission.get(&expected_key).unwrap();
        assert_eq!(assertion.assertion_contract, "test_assertion");
        assert_eq!(assertion.assertion_id, "test_id");
        assert_eq!(assertion.signature, "test_signature");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_run_with_expired_auth() {
        let mut server = Server::new_async().await;
        let mock = server.mock("POST", "/").with_status(401).create();

        let args = DaStoreArgs {
            da_url: server.url(),
            args: create_test_build_args(),
            constructor_args: vec![Address::random().to_string()],
        };

        let cli_args = CliArgs::default();

        let mut config = CliConfig {
            auth: Some(UserAuth {
                access_token: "expired_token".to_string(),
                refresh_token: "expired_refresh".to_string(),
                user_address: Address::from_slice(&[0; 20]),
                expires_at: DateTime::from_timestamp(0, 0).unwrap(), // Expired token
            }),
            ..Default::default()
        };

        let result = args.run(&cli_args, &mut config).await;
        assert!(result.is_err(), "Expected error but got: {result:?}");
        mock.assert();
    }

    #[tokio::test]
    async fn test_run_with_invalid_url() {
        let args = DaStoreArgs {
            da_url: "invalid-url".to_string(),
            args: BuildAndFlattenArgs::default(),
            constructor_args: vec!["arg1".to_string(), "arg2".to_string()],
        };

        let mut config = CliConfig::default();
        let cli_args = CliArgs::default();

        let result = args.run(&cli_args, &mut config).await;
        assert!(result.is_err());
    }
}
