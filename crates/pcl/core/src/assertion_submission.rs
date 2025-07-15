use crate::{
    config::{
        AssertionForSubmission,
        AssertionKey,
        CliConfig,
    },
    error::DappSubmitError,
};
use clap::ValueHint;
use inquire::{
    MultiSelect,
    Select,
};
use pcl_common::args::CliArgs;
use serde::Deserialize;
use serde_json::json;

// TODO(Odysseas) Add tests for the Dapp submission + Rust bindings from the Dapp API

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct Project {
    project_id: String,
    project_name: String,
    project_description: Option<String>,
    profile_image_url: Option<String>,
    project_networks: Vec<String>,
    project_manager: String,
    created_at: String,
    updated_at: String,
}

/// Arguments for submitting assertions to the Credible Layer dApp
///
/// This struct handles CLI arguments for the assertion submission process,
/// including the dApp URL, project name, and assertion names.
#[derive(clap::Parser)]
#[clap(
    name = "submit",
    about = "Submit assertions to the Credible Layer dApp",
    after_help = "EXAMPLES:\n    \
                  Submit a single assertion (positional args):\n        \
                  pcl submit AssertionName arg1 arg2 arg3\n\n    \
                  Submit multiple assertions (with -a flag):\n        \
                  pcl submit -a \"AssertionName1(arg1,arg2,arg3)\" -a \"AssertionName2(arg1,arg2,arg3)\"\n\n    \
                  Note: Positional arguments are for single assertions only.\n    \
                  The -a flag with parentheses format is for specifying assertions with arguments."
)]
pub struct DappSubmitArgs {
    /// Base URL for the Credible Layer dApp API
    #[clap(
        short = 'u',
        long = "api-url",
        env = "PCL_API_URL",
        value_hint = ValueHint::Url,
        value_name = "API Endpoint",
        default_value = "https://dapp.phylax.systems/api/v1"
    )]
    pub api_url: String,

    /// Optional project name to skip interactive selection
    #[clap(
        short = 'p',
        long,
        value_name = "PROJECT",
        value_hint = ValueHint::Other,

    )]
    pub project_name: Option<String>,

    /// Assertions to submit. Can be specified multiple times.
    /// Format: -a "AssertionName(arg1,arg2,arg3)"
    /// Use multiple -a flags for multiple assertions.
    #[clap(
        long = "assertion",
        short = 'a',
        value_name = "ASSERTION",
        value_hint = ValueHint::Other,
        help = "Assertion in format 'Name(arg1,arg2)'. Use multiple -a flags for multiple assertions."
    )]
    pub assertion_keys: Option<Vec<String>>,

    /// Positional argument for assertion name when submitting a single assertion
    #[clap(
        value_name = "ASSERTION_CONTRACT",
        help = "Name of the assertion to submit (when submitting a single assertion)",
        required = false,
        conflicts_with = "assertion_keys"
    )]
    pub assertion_name: Option<String>,

    /// Constructor arguments for the single assertion
    #[clap(
        value_name = "CONSTRUCTOR_ARGS",
        help = "Constructor arguments for the assertion",
        required = false,
        requires = "assertion_name"
    )]
    pub constructor_args: Vec<String>,
}

impl DappSubmitArgs {
    /// Parses assertion keys from command line arguments
    /// Supports positional arguments and string format with parentheses
    fn parse_assertion_keys(&self) -> Option<Vec<AssertionKey>> {
        // First check if positional arguments are provided
        if let Some(assertion_name) = &self.assertion_name {
            return Some(vec![AssertionKey::new(
                assertion_name.clone(),
                self.constructor_args.clone(),
            )]);
        }

        // Otherwise, parse from -a flags
        // Each -a flag should contain a single assertion in parentheses format
        self.assertion_keys.as_ref().map(|args| {
            args.iter()
                .map(|arg| AssertionKey::from(arg.clone()))
                .collect()
        })
    }

    /// Executes the assertion submission workflow
    ///
    /// # Arguments
    /// * `_cli_args` - General CLI arguments
    /// * `config` - Configuration containing assertions and auth details
    ///
    /// # Returns
    /// * `Result<(), DappSubmitError>` - Success or specific error
    pub async fn run(
        &self,
        _cli_args: &CliArgs,
        config: &mut CliConfig,
    ) -> Result<(), DappSubmitError> {
        let projects = self.get_projects(config).await?;
        let project = self.select_project(&projects)?;

        let keys: Vec<AssertionKey> = config.assertions_for_submission.keys().cloned().collect();
        let assertion_keys = self.select_assertions(keys.as_slice())?;

        let mut assertions = vec![];
        for key in assertion_keys {
            let assertion = config
                .assertions_for_submission
                .remove(&key.clone().into())
                .ok_or(DappSubmitError::CouldNotFindStoredAssertion(key.clone()))?;

            assertions.push(assertion);
        }

        self.submit_assertion(project, &assertions, config).await?;

        println!(
            "Successfully submitted {} assertion{} to project {}",
            assertions.len(),
            if assertions.len() > 1 { "s" } else { "" },
            project.project_name
        );

        Ok(())
    }

    /// Abstracted function for selecting a project
    fn select_project<'a>(&self, projects: &'a [Project]) -> Result<&'a Project, DappSubmitError> {
        if projects.is_empty() {
            return Err(DappSubmitError::NoProjectsFound);
        }

        let project_names: Vec<String> = projects.iter().map(|p| p.project_name.clone()).collect();
        let project_name = self.provide_or_select(
            self.project_name.clone(),
            project_names,
            "Select a project to submit the assertion to:".to_string(),
        )?;
        let project = projects
            .iter()
            .find(|p| p.project_name == project_name)
            .ok_or(DappSubmitError::NoProjectsFound)?;
        Ok(project)
    }

    /// Abstracted function for selecting assertions
    fn select_assertions(
        &self,
        assertion_keys_for_submission: &[AssertionKey],
    ) -> Result<Vec<String>, DappSubmitError> {
        if assertion_keys_for_submission.is_empty() {
            return Err(DappSubmitError::NoStoredAssertions);
        }

        let assertion_keys_for_selection = assertion_keys_for_submission
            .iter()
            .map(|k| k.to_string())
            .collect();

        let preselected_assertion_keys = self
            .parse_assertion_keys()
            .map(|keys| keys.iter().map(|k| k.to_string()).collect());

        self.provide_or_multi_select(
            preselected_assertion_keys,
            assertion_keys_for_selection,
            "Select an assertion to submit:".to_string(),
        )
    }
    ///
    /// # Arguments
    /// * `project` - Target project for submission
    /// * `assertions` - List of assertions to submit
    ///
    /// # Returns
    /// * `Result<(), DappSubmitError>` - Success or API error
    async fn submit_assertion(
        &self,
        project: &Project,
        assertions: &[AssertionForSubmission],
        config: &CliConfig,
    ) -> Result<(), DappSubmitError> {
        let client = reqwest::Client::new();
        let body = json!({
            "assertions": assertions.iter().map(|a| json!({
                "contract_name": &a.assertion_contract,
                "assertion_id": &a.assertion_id,
                "signature": &a.signature
            })).collect::<Vec<_>>()
        });

        let response = client
            .post(format!(
                "{}/projects/{}/submitted-assertions",
                self.api_url, project.project_id
            ))
            .header(
                "Authorization",
                format!("Bearer {}", config.auth.as_ref().unwrap().access_token),
            )
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            // If the response is unauthorized, return a specific error
            if response.status().as_u16() == 401 {
                return Err(DappSubmitError::NoAuthToken);
            }
            Err(DappSubmitError::SubmissionFailed(response.text().await?))
        }
    }

    /// Handles interactive or direct selection of a single value
    ///
    /// # Arguments
    /// * `maybe_key` - Optional pre-selected value
    /// * `values` - Available options
    /// * `message` - Prompt message for interactive selection
    ///
    /// # Returns
    /// * `Result<String, DappSubmitError>` - Selected value or error
    fn provide_or_select(
        &self,
        maybe_key: Option<String>,
        values: Vec<String>,
        message: String,
    ) -> Result<String, DappSubmitError> {
        match maybe_key {
            None => Ok(Select::new(message.as_str(), values).prompt()?),
            Some(key) => {
                let exists = values.contains(&key);
                if exists {
                    Ok(key.to_string())
                } else {
                    println!("{key} does not exist");
                    let choice = Select::new(message.as_str(), values).prompt()?;
                    Ok(choice)
                }
            }
        }
    }

    /// Handles interactive or direct selection of multiple values
    ///
    /// # Arguments
    /// * `maybe_keys` - Optional pre-selected values
    /// * `values` - Available options
    /// * `message` - Prompt message for interactive selection
    ///
    /// # Returns
    /// * `Result<Vec<String>, DappSubmitError>` - Selected values or error
    fn provide_or_multi_select(
        &self,
        maybe_keys: Option<Vec<String>>,
        values: Vec<String>,
        message: String,
    ) -> Result<Vec<String>, DappSubmitError> {
        match maybe_keys {
            None => Ok(MultiSelect::new(message.as_str(), values).prompt()?),
            Some(keys) => {
                let all_exist = keys.iter().all(|k| values.contains(k));
                if all_exist {
                    Ok(keys)
                } else {
                    let missing_keys = keys
                        .iter()
                        .filter(|k| !values.contains(k))
                        .cloned()
                        .collect::<Vec<_>>();
                    println!("{} does not exist", missing_keys.join(", "));
                    Ok(MultiSelect::new(message.as_str(), values).prompt()?)
                }
            }
        }
    }
    async fn get_projects(&self, config: &mut CliConfig) -> Result<Vec<Project>, DappSubmitError> {
        let client = reqwest::Client::new();
        let projects: Vec<Project> = client
            .get(format!(
                "{}/projects?user={}",
                self.api_url,
                config
                    .auth
                    .as_ref()
                    .ok_or(DappSubmitError::NoAuthToken)?
                    .user_address
            ))
            .send()
            .await?
            .json()
            .await?;
        Ok(projects)
    }
}

/// TODO(ODYSSEAS): Add tests for the DappSubmitArgs struct
#[cfg(test)]
mod tests {
    use super::*;
    use crate::assertion_submission::DappSubmitArgs;

    #[test]
    fn test_provide_or_select_with_valid_input() {
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: Some("Project1".to_string()),
            assertion_keys: None,
            assertion_name: None,
            constructor_args: vec![],
        };

        let values = vec!["Project1".to_string(), "Project2".to_string()];
        let result =
            args.provide_or_select(Some("Project1".to_string()), values, "Select:".to_string());

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Project1");
    }
    #[test]
    fn test_no_stored_assertions() {
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: None,
            assertion_name: None,
            constructor_args: vec![],
        };

        let empty_assertions = [];
        let result = args.select_assertions(&empty_assertions);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            DappSubmitError::NoStoredAssertions
        ));
    }
    #[test]
    fn test_no_projects() {
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: None,
            assertion_name: None,
            constructor_args: vec![],
        };

        let empty_projects: Vec<Project> = vec![];
        let result = args.select_project(&empty_projects);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            DappSubmitError::NoProjectsFound
        ));
    }
    #[test]
    fn test_select_assertions_with_preselected() {
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec!["assertion1".to_string()]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let stored_assertions = vec![
            AssertionKey::new("assertion1".to_string(), vec![]),
            AssertionKey::new(
                "assertion2".to_string(),
                vec!["a".to_string(), "b".to_string()],
            ),
            AssertionKey::new("assertion3".to_string(), vec![]),
        ];

        let result = args.select_assertions(&stored_assertions);

        assert!(result.is_ok());
        let selected = result.unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0], "assertion1");
    }
    #[test]
    fn test_provide_or_multi_select_with_preselected() {
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec!["assertion1".to_string()]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let values = vec!["assertion1".to_string(), "assertion2".to_string()];
        let result = args.provide_or_multi_select(
            Some(vec!["assertion1".to_string()]),
            values.clone(),
            "Select:".to_string(),
        );

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["assertion1".to_string()]);
    }

    #[test]
    fn test_provide_or_select_with_preselected() {
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: Some("Project1".to_string()),
            assertion_keys: None,
            assertion_name: None,
            constructor_args: vec![],
        };

        let values = vec!["Project1".to_string(), "Project2".to_string()];
        let result = args.provide_or_select(
            Some("Project1".to_string()),
            values.clone(),
            "Select:".to_string(),
        );

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Project1");
    }

    #[test]
    fn test_parse_assertion_keys_multiple_assertions() {
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec![
                "AssertionName1(arg1,arg2)".to_string(),
                "AssertionName2(arg3,arg4)".to_string(),
            ]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].assertion_name, "AssertionName1");
        assert_eq!(parsed[0].constructor_args, vec!["arg1", "arg2"]);
        assert_eq!(parsed[1].assertion_name, "AssertionName2");
        assert_eq!(parsed[1].constructor_args, vec!["arg3", "arg4"]);
    }

    #[test]
    fn test_parse_assertion_keys_string_format() {
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec!["AssertionName(arg1,arg2)".to_string()]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "AssertionName");
        assert_eq!(parsed[0].constructor_args, vec!["arg1", "arg2"]);
    }

    #[test]
    fn test_parse_assertion_keys_no_args() {
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec!["AssertionName()".to_string()]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "AssertionName");
        assert_eq!(parsed[0].constructor_args.len(), 0);
    }

    #[test]
    fn test_parse_assertion_keys_positional_args() {
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: None,
            assertion_name: Some("AssertionName".to_string()),
            constructor_args: vec!["arg1".to_string(), "arg2".to_string()],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "AssertionName");
        assert_eq!(parsed[0].constructor_args, vec!["arg1", "arg2"]);
    }

    #[test]
    fn test_parse_assertion_keys_positional_no_constructor_args() {
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: None,
            assertion_name: Some("AssertionName".to_string()),
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "AssertionName");
        assert_eq!(parsed[0].constructor_args.len(), 0);
    }

    #[test]
    fn test_parse_assertion_keys_positional_takes_precedence() {
        // When both positional and -a flags are provided, positional should take precedence
        // (though clap should prevent this with conflicts_with)
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec!["FlagAssertion".to_string()]),
            assertion_name: Some("PositionalAssertion".to_string()),
            constructor_args: vec!["pos_arg".to_string()],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "PositionalAssertion");
        assert_eq!(parsed[0].constructor_args, vec!["pos_arg"]);
    }

    #[test]
    fn test_parse_assertion_keys_without_parentheses() {
        // Test that assertions without parentheses are supported
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec!["AssertionName".to_string()]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "AssertionName");
        assert_eq!(parsed[0].constructor_args.len(), 0);
    }

    #[test]
    fn test_parse_assertion_keys_no_input() {
        // Test when no assertions are provided at all
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: None,
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys();
        assert!(parsed.is_none());
    }

    #[test]
    fn test_parse_assertion_keys_empty_assertion_keys_vec() {
        // Test when assertion_keys is Some but contains empty vec
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec![]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 0);
    }

    #[test]
    fn test_parse_assertion_keys_with_special_characters() {
        // Test handling of special characters in constructor args
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec!["AssertionName(arg-1,arg_2,arg.3)".to_string()]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "AssertionName");
        assert_eq!(parsed[0].constructor_args, vec!["arg-1", "arg_2", "arg.3"]);
    }

    #[test]
    fn test_parse_assertion_keys_with_spaces() {
        // Test handling of spaces around commas
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec!["AssertionName(arg1, arg2 , arg3)".to_string()]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "AssertionName");
        assert_eq!(parsed[0].constructor_args, vec!["arg1", " arg2 ", " arg3"]);
    }

    #[test]
    fn test_parse_assertion_keys_with_numeric_args() {
        // Test handling of numeric constructor args
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec!["AssertionName(123,456,789)".to_string()]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "AssertionName");
        assert_eq!(parsed[0].constructor_args, vec!["123", "456", "789"]);
    }

    #[test]
    fn test_parse_assertion_keys_mixed_format() {
        // Test multiple assertions with different formats
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec![
                "AssertionName1".to_string(),
                "AssertionName2()".to_string(),
                "AssertionName3(arg1)".to_string(),
                "AssertionName4(arg1,arg2,arg3)".to_string(),
            ]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 4);

        assert_eq!(parsed[0].assertion_name, "AssertionName1");
        assert_eq!(parsed[0].constructor_args.len(), 0);

        assert_eq!(parsed[1].assertion_name, "AssertionName2");
        assert_eq!(parsed[1].constructor_args.len(), 0);

        assert_eq!(parsed[2].assertion_name, "AssertionName3");
        assert_eq!(parsed[2].constructor_args, vec!["arg1"]);

        assert_eq!(parsed[3].assertion_name, "AssertionName4");
        assert_eq!(parsed[3].constructor_args, vec!["arg1", "arg2", "arg3"]);
    }

    #[test]
    fn test_parse_assertion_keys_address_format() {
        // Test handling of Ethereum addresses as constructor args
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec![
                "TokenAssertion(0x1234567890123456789012345678901234567890)".to_string(),
            ]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "TokenAssertion");
        assert_eq!(
            parsed[0].constructor_args,
            vec!["0x1234567890123456789012345678901234567890"]
        );
    }

    #[test]
    fn test_parse_assertion_keys_complex_args() {
        // Test handling of complex constructor args with mixed types
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec![
                "ComplexAssertion(0x742d35Cc6634C0532925a3b844Bc9e7595f8b2dc,1000000,true,ipfs://QmHash)".to_string()
            ]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "ComplexAssertion");
        assert_eq!(
            parsed[0].constructor_args,
            vec![
                "0x742d35Cc6634C0532925a3b844Bc9e7595f8b2dc",
                "1000000",
                "true",
                "ipfs://QmHash"
            ]
        );
    }

    #[test]
    fn test_parse_assertion_keys_positional_with_address() {
        // Test positional args with Ethereum address
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: None,
            assertion_name: Some("TokenAssertion".to_string()),
            constructor_args: vec![
                "0x1234567890123456789012345678901234567890".to_string(),
                "1000000".to_string(),
            ],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "TokenAssertion");
        assert_eq!(
            parsed[0].constructor_args,
            vec!["0x1234567890123456789012345678901234567890", "1000000"]
        );
    }

    #[test]
    fn test_parse_assertion_keys_with_nested_parentheses() {
        // Test handling of nested parentheses in constructor args
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec![
                "FunctionAssertion(someFunc(uint256,address),100)".to_string(),
            ]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "FunctionAssertion");
        // The current parser splits on all commas, including those within parentheses
        assert_eq!(
            parsed[0].constructor_args,
            vec!["someFunc(uint256", "address)", "100"]
        );
    }

    #[test]
    fn test_parse_assertion_keys_with_quoted_args() {
        // Test handling of quoted arguments (common for strings)
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec![
                r#"StringAssertion("hello, world","test string")"#.to_string(),
            ]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "StringAssertion");
        assert_eq!(
            parsed[0].constructor_args,
            vec![r#""hello"#, r#" world""#, r#""test string""#]
        );
    }

    #[test]
    fn test_parse_assertion_keys_with_empty_string_args() {
        // Test handling of empty string arguments
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec!["EmptyStringAssertion(,arg2,)".to_string()]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "EmptyStringAssertion");
        assert_eq!(parsed[0].constructor_args, vec!["", "arg2", ""]);
    }

    #[test]
    fn test_parse_assertion_keys_with_url_args() {
        // Test handling of URLs as constructor args
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec![
                "UrlAssertion(https://example.com/api,http://localhost:8080)".to_string(),
            ]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "UrlAssertion");
        assert_eq!(
            parsed[0].constructor_args,
            vec!["https://example.com/api", "http://localhost:8080"]
        );
    }

    #[test]
    fn test_parse_assertion_keys_positional_with_special_chars() {
        // Test positional args with special characters
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: None,
            assertion_name: Some("SpecialAssertion".to_string()),
            constructor_args: vec![
                "arg-with-dashes".to_string(),
                "arg_with_underscores".to_string(),
                "arg.with.dots".to_string(),
                "arg/with/slashes".to_string(),
            ],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "SpecialAssertion");
        assert_eq!(
            parsed[0].constructor_args,
            vec![
                "arg-with-dashes",
                "arg_with_underscores",
                "arg.with.dots",
                "arg/with/slashes"
            ]
        );
    }

    #[test]
    fn test_parse_assertion_keys_with_json_like_args() {
        // Test handling of JSON-like arguments
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec![
                r#"JsonAssertion({"key":"value"},["item1","item2"])"#.to_string(),
            ]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "JsonAssertion");
        // The current parser splits on all commas, including those within JSON structures
        assert_eq!(
            parsed[0].constructor_args,
            vec![r#"{"key":"value"}"#, r#"["item1""#, r#""item2"]"#]
        );
    }

    #[test]
    fn test_parse_assertion_keys_very_long_args() {
        // Test handling of very long constructor arguments
        let long_arg = "a".repeat(1000);
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec![format!("LongAssertion({},short)", long_arg)]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "LongAssertion");
        assert_eq!(parsed[0].constructor_args[0].len(), 1000);
        assert_eq!(parsed[0].constructor_args[1], "short");
    }

    #[test]
    fn test_parse_assertion_keys_unicode_args() {
        // Test handling of Unicode characters in args
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec!["UnicodeAssertion(Hello🌍,测试,🚀🚀🚀)".to_string()]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "UnicodeAssertion");
        assert_eq!(
            parsed[0].constructor_args,
            vec!["Hello🌍", "测试", "🚀🚀🚀"]
        );
    }

    #[test]
    fn test_parse_assertion_keys_malformed_parentheses() {
        // Test handling of malformed parentheses
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec![
                "MalformedAssertion(arg1,arg2".to_string(), // Missing closing parenthesis
            ]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "MalformedAssertion");
        // The parser still splits on commas even without closing parenthesis
        assert_eq!(parsed[0].constructor_args, vec!["arg1", "arg2"]);
    }

    #[test]
    fn test_parse_assertion_keys_constructor_args_without_name() {
        // Test that constructor_args are ignored without assertion_name
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: None,
            assertion_name: None,
            constructor_args: vec!["arg1".to_string(), "arg2".to_string()],
        };

        let parsed = args.parse_assertion_keys();
        assert!(parsed.is_none());
    }

    #[test]
    fn test_parse_assertion_keys_multiple_positional_mixed_args() {
        // Test multiple positional args with different types
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: None,
            assertion_name: Some("MixedAssertion".to_string()),
            constructor_args: vec![
                "0x742d35Cc6634C0532925a3b844Bc9e7595f8b2dc".to_string(),
                "1000000".to_string(),
                "true".to_string(),
                "ipfs://QmHash".to_string(),
                "https://example.com".to_string(),
            ],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "MixedAssertion");
        assert_eq!(
            parsed[0].constructor_args,
            vec![
                "0x742d35Cc6634C0532925a3b844Bc9e7595f8b2dc",
                "1000000",
                "true",
                "ipfs://QmHash",
                "https://example.com"
            ]
        );
    }

    #[test]
    fn test_parse_assertion_keys_single_flag_assertion() {
        // Test single assertion using -a flag instead of positional
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec!["SingleAssertion(arg1,arg2,arg3)".to_string()]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "SingleAssertion");
        assert_eq!(parsed[0].constructor_args, vec!["arg1", "arg2", "arg3"]);
    }

    #[test]
    fn test_parse_assertion_keys_whitespace_handling() {
        // Test handling of various whitespace scenarios
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec![
                " SpacedAssertion ( arg1 , arg2 ) ".to_string(),
                "\tTabbedAssertion\t(\targ1\t,\targ2\t)\t".to_string(),
            ]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 2);

        // Note: The current parser doesn't trim whitespace from assertion names
        assert_eq!(parsed[0].assertion_name, " SpacedAssertion ");
        // The parser includes the closing paren with the last arg when there's space before it
        assert_eq!(parsed[0].constructor_args, vec![" arg1 ", " arg2 ) "]);

        assert_eq!(parsed[1].assertion_name, "\tTabbedAssertion\t");
        assert_eq!(parsed[1].constructor_args, vec!["\targ1\t", "\targ2\t)\t"]);
    }

    #[test]
    fn test_parse_assertion_keys_consecutive_commas() {
        // Test handling of consecutive commas (empty args)
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec!["ConsecutiveCommas(arg1,,arg3,,,arg6)".to_string()]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].assertion_name, "ConsecutiveCommas");
        assert_eq!(
            parsed[0].constructor_args,
            vec!["arg1", "", "arg3", "", "", "arg6"]
        );
    }

    #[test]
    fn test_provide_or_select_with_invalid_preselected() {
        // Test when preselected value is not in the list
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: Some("NonExistentProject".to_string()),
            assertion_keys: None,
            assertion_name: None,
            constructor_args: vec![],
        };

        let values = vec!["Project1".to_string(), "Project2".to_string()];
        // This would normally prompt the user, but we can't test interactive behavior
        // Just verify the method signature is correct
        let _ = args.provide_or_select(
            Some("NonExistentProject".to_string()),
            values,
            "Select:".to_string(),
        );
    }

    #[test]
    fn test_provide_or_multi_select_with_partial_invalid() {
        // Test when some preselected values are not in the list
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: None,
            assertion_name: None,
            constructor_args: vec![],
        };

        let values = vec!["assertion1".to_string(), "assertion2".to_string()];
        let preselected = vec!["assertion1".to_string(), "assertion3".to_string()];
        // This would normally prompt the user, but we can't test interactive behavior
        let _ = args.provide_or_multi_select(Some(preselected), values, "Select:".to_string());
    }

    #[test]
    fn test_parse_assertion_keys_case_sensitivity() {
        // Test that assertion names preserve case
        let args = DappSubmitArgs {
            api_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec![
                "camelCaseAssertion(ARG1,arg2,Arg3)".to_string(),
                "UPPERCASE_ASSERTION(PARAM1,PARAM2)".to_string(),
                "lowercase_assertion(value1,value2)".to_string(),
            ]),
            assertion_name: None,
            constructor_args: vec![],
        };

        let parsed = args.parse_assertion_keys().unwrap();
        assert_eq!(parsed.len(), 3);

        assert_eq!(parsed[0].assertion_name, "camelCaseAssertion");
        assert_eq!(parsed[0].constructor_args, vec!["ARG1", "arg2", "Arg3"]);

        assert_eq!(parsed[1].assertion_name, "UPPERCASE_ASSERTION");
        assert_eq!(parsed[1].constructor_args, vec!["PARAM1", "PARAM2"]);

        assert_eq!(parsed[2].assertion_name, "lowercase_assertion");
        assert_eq!(parsed[2].constructor_args, vec!["value1", "value2"]);
    }
}
