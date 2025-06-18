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
    about = "Submit assertions to the Credible Layer dApp"
)]
pub struct DappSubmitArgs {
    /// Base URL for the Credible Layer dApp API
    #[clap(
        short = 'u',
        long,
        value_hint = ValueHint::Url,
        value_name = "API Endpoint",
        default_value = "https://dapp.phylax.systems/api/v1"
    )]
    dapp_url: String,

    /// Optional project name to skip interactive selection
    #[clap(
        short = 'p',
        long,
        value_name = "PROJECT",
        value_hint = ValueHint::Other,

    )]
    project_name: Option<String>,

    /// Optional list of assertion name and constructor args to skip interactive selection
    /// Format: assertion_name OR 'assertion_name(constructor_arg0,constructor_arg1)'
    #[clap(
        long,
        short = 'a',
        value_name = "ASSERTION_KEYS",
        value_hint = ValueHint::Other,
    )]
    assertion_keys: Option<Vec<AssertionKey>>,
}

impl DappSubmitArgs {
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
            .assertion_keys
            .clone()
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
                self.dapp_url, project.project_id
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
                self.dapp_url,
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
            dapp_url: "".to_string(),
            project_name: Some("Project1".to_string()),
            assertion_keys: None,
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
            dapp_url: "".to_string(),
            project_name: None,
            assertion_keys: None,
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
            dapp_url: "".to_string(),
            project_name: None,
            assertion_keys: None,
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
            dapp_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec![AssertionKey::new("assertion1".to_string(), vec![])]),
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
            dapp_url: "".to_string(),
            project_name: None,
            assertion_keys: Some(vec![AssertionKey::new("assertion1".to_string(), vec![])]),
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
            dapp_url: "".to_string(),
            project_name: Some("Project1".to_string()),
            assertion_keys: None,
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
}
