//! Module for handling assertion submission to the Data Availability (DA) layer.
//!
//! This module provides functionality to submit assertions to a Data Availability layer,
//! which is a crucial component of the Credible Layer system. It handles the process
//! of building, flattening, and submitting assertions along with their source code
//! to be stored in the DA layer.

use crate::DEFAULT_DA_URL;
use clap::{
    Parser,
    ValueHint,
};
use colored::Colorize;
use indicatif::ProgressBar;
#[cfg(not(test))]
use indicatif::ProgressStyle;
use pcl_common::args::CliArgs;
use pcl_phoundry::build_and_flatten::{
    BuildAndFlatOutput,
    BuildAndFlattenArgs,
};
use serde_json::json;
use std::path::PathBuf;
#[cfg(not(test))]
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

const STORE_DOCS_URL: &str = "https://docs.phylax.systems/credible/store-submit-assertions";
const STORE_AFTER_HELP: &str = "Store assertions on the Assertion DA before linking them to a project with `pcl submit`.\n\
Learn more about the workflow: https://docs.phylax.systems/credible/store-submit-assertions";

/// Command-line arguments for storing assertions in the Data Availability layer.
///
/// This struct handles the configuration needed to submit assertions to the DA layer,
/// including the DA server URL and build arguments for the assertion.
#[derive(Parser)]
#[clap(
    name = "store",
    arg_required_else_help = true,
    about = "Store assertion bytecode and source on the Credible Assertion DA.",
    long_about = "Store assertion bytecode and source on the Credible Assertion Data Availability (DA) service. Run this before `pcl submit` so the dApp can reference the assertion in a project.",
    after_help = STORE_AFTER_HELP
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

    /// Root directory where assertions live
    #[clap(
        long,
        value_hint = ValueHint::DirPath,
        help = "Root directory of your assertion project (defaults to the nearest Foundry project)."
    )]
    pub root: Option<PathBuf>,

    /// Assertions to store using the formatted flag
    #[clap(
        long = "assertion",
        short = 'a',
        value_name = "ASSERTION",
        value_hint = ValueHint::Other,
        value_parser,
        help = "Assertion contract in the format 'ContractName' or 'ContractName(arg0,arg1,...)'. Array arguments are supported, e.g. 'ContractName([addr1,addr2])'. Repeat the flag to store multiple assertions (wrap the value in quotes to avoid shell parsing)."
    )]
    pub assertion_specs: Vec<AssertionKey>,

    /// Assertions provided as positional arguments when not using --assertion
    #[clap(
        value_name = "ASSERTION",
        value_hint = ValueHint::Other,
        help = "Assertion spec(s) in the format 'ContractName' or 'ContractName(arg0,arg1,...)'. Array arguments are supported, e.g. 'ContractName([addr1,addr2])'. Multiple specs can be separated by whitespace or commas.",
        required_unless_present = "assertion_specs",
        trailing_var_arg = true
    )]
    pub positional_assertions: Vec<String>,
}

impl DaStoreArgs {
    /// Creates and configures a progress spinner for displaying operation status.
    #[cfg(not(test))]
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

    /// Creates a spinner for tests without spawning background threads.
    #[cfg(test)]
    fn create_spinner() -> ProgressBar {
        ProgressBar::hidden()
    }

    /// Returns the assertions that should be stored for this invocation.
    fn assertions_to_store(&self) -> Vec<AssertionKey> {
        let mut assertions = self.assertion_specs.clone();
        assertions.extend(self.positional_assertions_to_keys());
        assertions
    }

    /// Parses positional assertions into `AssertionKey` entries.
    fn positional_assertions_to_keys(&self) -> Vec<AssertionKey> {
        let specs = Self::parse_positional_specs(&self.positional_assertions);
        specs
            .into_iter()
            .map(|spec| AssertionKey::from(spec.as_str()))
            .collect()
    }

    fn parse_positional_specs(positional: &[String]) -> Vec<String> {
        if positional.is_empty() {
            return vec![];
        }

        let mut specs = Vec::new();
        let mut current = String::new();
        let mut paren_balance: i32 = 0;

        for token in positional {
            if !current.is_empty() {
                current.push(' ');
            }
            current.push_str(token);
            paren_balance = Self::update_paren_balance(paren_balance, token);

            if paren_balance == 0 {
                specs.extend(Self::split_top_level_commas(&current));
                current.clear();
            }
        }

        if !current.trim().is_empty() {
            specs.extend(Self::split_top_level_commas(&current));
        }

        specs
            .into_iter()
            .map(|spec| spec.trim().to_string())
            .filter(|spec| !spec.is_empty())
            .collect()
    }

    fn split_top_level_commas(input: &str) -> Vec<String> {
        let mut parts = Vec::new();
        let mut current = String::new();
        let mut paren_depth: i32 = 0;
        let mut bracket_depth: i32 = 0;

        for ch in input.chars() {
            match ch {
                '(' => {
                    paren_depth += 1;
                    current.push(ch);
                }
                ')' => {
                    if paren_depth > 0 {
                        paren_depth -= 1;
                    }
                    current.push(ch);
                }
                '[' => {
                    bracket_depth += 1;
                    current.push(ch);
                }
                ']' => {
                    if bracket_depth > 0 {
                        bracket_depth -= 1;
                    }
                    current.push(ch);
                }
                ',' if paren_depth == 0 && bracket_depth == 0 => {
                    parts.push(current.clone());
                    current.clear();
                }
                _ => current.push(ch),
            }
        }

        if !current.is_empty() {
            parts.push(current);
        }

        parts
    }

    fn update_paren_balance(balance: i32, token: &str) -> i32 {
        let mut updated = balance;
        for ch in token.chars() {
            match ch {
                '(' | '[' => updated += 1,
                ')' | ']' => {
                    if updated > 0 {
                        updated -= 1;
                    }
                }
                _ => {}
            }
        }
        updated
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
            let app_url = self.da_url.replace("://da.", "://app.");
            println!("Visit the Credible Layer App to link the assertion on-chain and enforce it:");
            println!("  {}", app_url.cyan().bold());
            println!(
                "Tip: use `pcl store` to send assertions to the DA and `pcl submit` to begin enforcement."
            );
            println!("Docs: {}", STORE_DOCS_URL.cyan().bold());
        }
    }

    /// Builds and flattens the assertion source code.
    ///
    /// # Returns
    /// * `Result<BuildAndFlatOutput, DaSubmitError>` - The build output or error
    fn build_and_flatten_assertion(
        &self,
        assertion_name: &str,
    ) -> Result<BuildAndFlatOutput, DaSubmitError> {
        BuildAndFlattenArgs {
            assertion_contract: assertion_name.to_string(),
            root: self.root.clone(),
        }
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
        assertion_name: &str,
        constructor_args: &[String],
        build_output: &BuildAndFlatOutput,
        spinner: &ProgressBar,
    ) -> Result<DaSubmissionResponse, DaSubmitError> {
        let constructor_inputs = build_output
            .abi
            .constructor()
            .map(|constructor| constructor.inputs.clone())
            .unwrap_or_default();

        if constructor_inputs.len() != constructor_args.len() {
            return Err(DaSubmitError::InvalidConstructorArgs(
                constructor_inputs.len(),
                constructor_args.len(),
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
                assertion_name.to_string(),
                build_output.flattened_source.clone(),
                build_output.compiler_version.clone(),
                constructor_signature,
                constructor_args.to_vec(),
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
        assertion_key: &AssertionKey,
        assertion_id: &A,
        signature: &S,
        spinner: &ProgressBar,
        json_output: bool,
    ) {
        let assertion_for_submission = AssertionForSubmission {
            assertion_contract: assertion_key.assertion_name.clone(),
            assertion_id: assertion_id.to_string(),
            signature: signature.to_string(),
            constructor_args: assertion_key.constructor_args.clone(),
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
        let assertions = self.assertions_to_store();
        let client = self
            .create_da_client(config)
            .map_err(DaSubmitError::DaClientError)?;
        let total = assertions.len();

        for (index, assertion_key) in assertions.into_iter().enumerate() {
            let spinner = if json_output {
                ProgressBar::hidden()
            } else {
                Self::create_spinner()
            };

            if !json_output {
                let prefix = if total > 1 {
                    format!(
                        "Submitting {} to DA ({}/{})...",
                        assertion_key.assertion_name,
                        index + 1,
                        total
                    )
                } else {
                    format!("Submitting {} to DA...", assertion_key.assertion_name)
                };
                spinner.set_message(prefix);
            }

            let build_output = self.build_and_flatten_assertion(&assertion_key.assertion_name)?;
            let submission_response = self
                .submit_to_da(
                    &client,
                    &assertion_key.assertion_name,
                    &assertion_key.constructor_args,
                    &build_output,
                    &spinner,
                )
                .await?;
            self.update_config(
                config,
                &assertion_key,
                &submission_response.id,
                &submission_response.prover_signature,
                &spinner,
                json_output,
            );
        }

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

    /// Creates default store args pointing at the mock-project testdata
    fn create_test_store_args(da_url: String) -> DaStoreArgs {
        let constructor_arg = Address::random().to_string();
        DaStoreArgs {
            da_url,
            root: Some(PathBuf::from("../../../testdata/mock-protocol")),
            assertion_specs: vec![AssertionKey::new(
                "MockAssertion".to_string(),
                vec![constructor_arg],
            )],
            positional_assertions: vec![],
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
        let args = create_test_store_args(server.url());

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
        let args = create_test_store_args(server.url());

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
        let mut args = create_test_store_args(server.url());
        args.assertion_specs = vec![AssertionKey::new(
            "MockAssertion".to_string(),
            vec!["invalid_arg".to_string()],
        )];

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
            root: None,
            assertion_specs: vec![AssertionKey::new(
                "test_assertion".to_string(),
                vec!["arg1".to_string(), "arg2".to_string()],
            )],
            positional_assertions: vec![],
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
        let args = create_test_store_args(server.url());

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
        let args = create_test_store_args(server.url());

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
        let args = create_test_store_args(server.url());

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
        let args = create_test_store_args(server.url());

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
            root: None,
            assertion_specs: vec![AssertionKey::new(
                "ExampleAssertion".to_string(),
                vec!["arg1".to_string(), "arg2".to_string()],
            )],
            positional_assertions: vec![],
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
            root: None,
            assertion_specs: vec![AssertionKey::new(
                "ExampleAssertion".to_string(),
                vec!["arg1".to_string(), "arg2".to_string()],
            )],
            positional_assertions: vec![],
        };

        let config = CliConfig::default();
        let client = args.create_da_client(&config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_assertions_to_store_merges_specs() {
        let args = DaStoreArgs {
            da_url: "https://demo-21-assertion-da.phylax.systems".to_string(),
            root: None,
            assertion_specs: vec![AssertionKey::new(
                "SpecAssertion".to_string(),
                vec!["arg1".to_string()],
            )],
            positional_assertions: vec!["PositionalAssertion".to_string()],
        };

        let assertions = args.assertions_to_store();
        assert_eq!(assertions.len(), 2);
        assert_eq!(assertions[0].assertion_name, "SpecAssertion");
        assert_eq!(assertions[0].constructor_args, vec!["arg1"]);
        assert_eq!(assertions[1].assertion_name, "PositionalAssertion");
        assert!(assertions[1].constructor_args.is_empty());
    }

    #[test]
    fn test_positional_assertions_parse_inline_args_with_spaces() {
        let args = DaStoreArgs {
            da_url: "https://demo-21-assertion-da.phylax.systems".to_string(),
            root: None,
            assertion_specs: vec![],
            positional_assertions: vec!["InlineAssertion(arg1,".to_string(), "arg2)".to_string()],
        };

        let assertions = args.assertions_to_store();
        assert_eq!(assertions.len(), 1);
        assert_eq!(assertions[0].assertion_name, "InlineAssertion");
        assert_eq!(
            assertions[0].constructor_args,
            vec!["arg1".to_string(), "arg2".to_string()]
        );
    }

    #[test]
    fn test_positional_assertions_parse_csv_and_whitespace() {
        let args = DaStoreArgs {
            da_url: "https://demo-21-assertion-da.phylax.systems".to_string(),
            root: None,
            assertion_specs: vec![],
            positional_assertions: vec![
                "FirstAssertion()".to_string(),
                "SecondAssertion(arg1,arg2),ThirdAssertion".to_string(),
            ],
        };

        let assertions = args.assertions_to_store();
        assert_eq!(assertions.len(), 3);
        assert_eq!(assertions[0].assertion_name, "FirstAssertion");
        assert!(assertions[0].constructor_args.is_empty());
        assert_eq!(assertions[1].assertion_name, "SecondAssertion");
        assert_eq!(
            assertions[1].constructor_args,
            vec!["arg1".to_string(), "arg2".to_string()]
        );
        assert_eq!(assertions[2].assertion_name, "ThirdAssertion");
        assert!(assertions[2].constructor_args.is_empty());
    }

    #[tokio::test]
    async fn test_update_config() {
        let args = DaStoreArgs {
            da_url: "https://demo-21-assertion-da.phylax.systems".to_string(),
            root: None,
            assertion_specs: vec![AssertionKey::new(
                "test_assertion".to_string(),
                vec!["arg1".to_string(), "arg2".to_string()],
            )],
            positional_assertions: vec![],
        };

        let mut config = CliConfig::default();
        let spinner = DaStoreArgs::create_spinner();
        let assertion_key = AssertionKey::new(
            "test_assertion".to_string(),
            vec!["arg1".to_string(), "arg2".to_string()],
        );

        args.update_config(
            &mut config,
            &assertion_key,
            "test_id",
            "test_signature",
            &spinner,
            false,
        );

        assert_eq!(config.assertions_for_submission.len(), 1);

        let expected_key = assertion_key;

        let assertion = config.assertions_for_submission.get(&expected_key).unwrap();
        assert_eq!(assertion.assertion_contract, "test_assertion");
        assert_eq!(assertion.assertion_id, "test_id");
        assert_eq!(assertion.signature, "test_signature");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_run_with_expired_auth() {
        let mut server = Server::new_async().await;
        let mock = server.mock("POST", "/").with_status(401).create();

        let args = create_test_store_args(server.url());

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
            root: None,
            assertion_specs: vec![AssertionKey::new(
                "ExampleAssertion".to_string(),
                vec!["arg1".to_string(), "arg2".to_string()],
            )],
            positional_assertions: vec![],
        };

        let mut config = CliConfig::default();
        let cli_args = CliArgs::default();

        let result = args.run(&cli_args, &mut config).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_positional_assertions_parse_array_arg() {
        // Single array argument
        let args = DaStoreArgs {
            da_url: "https://demo-21-assertion-da.phylax.systems".to_string(),
            root: None,
            assertion_specs: vec![],
            positional_assertions: vec!["ArrayAssertion([addr1,addr2])".to_string()],
        };

        let assertions = args.assertions_to_store();
        assert_eq!(assertions.len(), 1);
        assert_eq!(assertions[0].assertion_name, "ArrayAssertion");
        assert_eq!(assertions[0].constructor_args, vec!["[addr1,addr2]"]);
    }

    #[test]
    fn test_positional_assertions_parse_array_with_other_args() {
        // Array with other arguments
        let args = DaStoreArgs {
            da_url: "https://demo-21-assertion-da.phylax.systems".to_string(),
            root: None,
            assertion_specs: vec![],
            positional_assertions: vec!["MixedAssertion(arg1,[addr1,addr2],arg3)".to_string()],
        };

        let assertions = args.assertions_to_store();
        assert_eq!(assertions.len(), 1);
        assert_eq!(assertions[0].assertion_name, "MixedAssertion");
        assert_eq!(
            assertions[0].constructor_args,
            vec!["arg1", "[addr1,addr2]", "arg3"]
        );
    }

    #[test]
    fn test_positional_assertions_parse_multiple_arrays() {
        // Multiple array arguments
        let args = DaStoreArgs {
            da_url: "https://demo-21-assertion-da.phylax.systems".to_string(),
            root: None,
            assertion_specs: vec![],
            positional_assertions: vec!["MultiArrayAssertion([a,b],[c,d])".to_string()],
        };

        let assertions = args.assertions_to_store();
        assert_eq!(assertions.len(), 1);
        assert_eq!(assertions[0].assertion_name, "MultiArrayAssertion");
        assert_eq!(assertions[0].constructor_args, vec!["[a,b]", "[c,d]"]);
    }

    #[test]
    fn test_positional_assertions_csv_with_array() {
        // CSV separated assertions where one has an array
        let args = DaStoreArgs {
            da_url: "https://demo-21-assertion-da.phylax.systems".to_string(),
            root: None,
            assertion_specs: vec![],
            positional_assertions: vec![
                "FirstAssertion([addr1,addr2]),SecondAssertion(arg1)".to_string(),
            ],
        };

        let assertions = args.assertions_to_store();
        assert_eq!(assertions.len(), 2);
        assert_eq!(assertions[0].assertion_name, "FirstAssertion");
        assert_eq!(assertions[0].constructor_args, vec!["[addr1,addr2]"]);
        assert_eq!(assertions[1].assertion_name, "SecondAssertion");
        assert_eq!(assertions[1].constructor_args, vec!["arg1"]);
    }

    #[test]
    fn test_positional_assertions_split_across_tokens_with_array() {
        // Array argument split across shell tokens
        let args = DaStoreArgs {
            da_url: "https://demo-21-assertion-da.phylax.systems".to_string(),
            root: None,
            assertion_specs: vec![],
            positional_assertions: vec![
                "ArrayAssertion([addr1,".to_string(),
                "addr2])".to_string(),
            ],
        };

        let assertions = args.assertions_to_store();
        assert_eq!(assertions.len(), 1);
        assert_eq!(assertions[0].assertion_name, "ArrayAssertion");
        assert_eq!(assertions[0].constructor_args, vec!["[addr1, addr2]"]);
    }

    #[test]
    fn test_positional_assertions_ethereum_address_array() {
        // Realistic use case: array of Ethereum addresses
        let args = DaStoreArgs {
            da_url: "https://demo-21-assertion-da.phylax.systems".to_string(),
            root: None,
            assertion_specs: vec![],
            positional_assertions: vec![
                "TokenAssertion([0x1234567890123456789012345678901234567890,0xabcdefabcdefabcdefabcdefabcdefabcdefabcd])".to_string(),
            ],
        };

        let assertions = args.assertions_to_store();
        assert_eq!(assertions.len(), 1);
        assert_eq!(assertions[0].assertion_name, "TokenAssertion");
        assert_eq!(
            assertions[0].constructor_args,
            vec![
                "[0x1234567890123456789012345678901234567890,0xabcdefabcdefabcdefabcdefabcdefabcdefabcd]"
            ]
        );
    }
}
