use crate::{
    DEFAULT_DAPP_URL,
    config::CliConfig,
    error::ApplyError,
};
use clap::ValueHint;
use inquire::Select;
use pcl_common::args::CliArgs;
use pcl_phoundry::build_and_flatten::BuildAndFlattenArgs;
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value;
use std::{
    collections::{
        BTreeMap,
        HashMap,
    },
    io::{
        Write,
        stderr,
        stdin,
    },
    path::{
        Path,
        PathBuf,
    },
};

#[derive(clap::Parser, Debug)]
#[command(
    name = "apply",
    about = "Preview and apply declarative deployment changes from credible.toml"
)]
pub struct ApplyArgs {
    #[arg(
        long,
        value_hint = ValueHint::DirPath,
        default_value = ".",
        help = "Project root directory"
    )]
    pub root: PathBuf,

    #[arg(long, help = "Emit machine-readable output for this command")]
    pub json: bool,

    #[arg(
        long = "yes",
        visible_alias = "auto-approve",
        help = "Apply without interactive confirmation"
    )]
    pub yes: bool,

    #[arg(
        short = 'u',
        long = "api-url",
        env = "PCL_API_URL",
        value_hint = ValueHint::Url,
        default_value = DEFAULT_DAPP_URL,
        help = "Base URL for the platform API"
    )]
    pub api_url: String,
}

#[derive(Debug, Deserialize)]
struct CredibleToml {
    environment: String,
    #[serde(default)]
    project_id: Option<String>,
    contracts: BTreeMap<String, CredibleContract>,
}

#[derive(Debug, Deserialize)]
struct CredibleContract {
    address: String,
    name: String,
    assertions: Vec<CredibleAssertion>,
}

#[derive(Debug, Deserialize)]
struct CredibleAssertion {
    file: String,
    #[serde(default, deserialize_with = "deserialize_args")]
    args: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct Project {
    project_id: String,
    project_name: String,
}

#[derive(Debug, Serialize)]
struct ApplyPayload {
    environment: String,
    contracts: BTreeMap<String, ApplyContractPayload>,
}

#[derive(Debug, Serialize)]
struct ApplyContractPayload {
    address: String,
    name: String,
    assertions: Vec<ApplyAssertionPayload>,
}

#[derive(Debug, Serialize)]
struct ApplyAssertionPayload {
    file: String,
    args: Vec<String>,
    flattened_source: String,
    bytecode: String,
    compiler_settings: CompilerSettingsPayload,
}

#[derive(Debug, Serialize)]
struct CompilerSettingsPayload {
    compiler_version: String,
    optimizer_enabled: bool,
    optimizer_runs: u64,
    evm_version: String,
    metadata_bytecode_hash: String,
    remappings: Vec<String>,
    libraries: HashMap<String, String>,
    compilation_target: String,
}

#[derive(Debug, Serialize)]
struct ApplyJsonOutput {
    status: &'static str,
    project_id: String,
    preview: Value,
    applied: bool,
    deployment: Option<Value>,
}

impl ApplyArgs {
    pub async fn run(&self, cli_args: &CliArgs, config: &CliConfig) -> Result<(), ApplyError> {
        let json_output = cli_args.json_output() || self.json;
        let root = canonicalize_root(&self.root)?;
        let credible = read_credible_toml(&root)?;
        let project_id = match credible.project_id.clone() {
            Some(project_id) => project_id,
            None if json_output => {
                return Err(ApplyError::InvalidConfig(
                    "`project_id` is required in credible.toml when using --json".to_string(),
                ));
            }
            None => self.select_project(config).await?,
        };
        let payload = Self::build_payload(&credible, &root)?;

        // TODO(ENG-2129): Uncomment the preview request and the preview/no-op handling block
        // below once the preview endpoint and diff rendering flow are finalized in follow-up PRs.
        // let preview = self
        //     .post_authenticated(
        //         config,
        //         &format!(
        //             "{}/api/v1/projects/{project_id}/deployments/preview",
        //             self.api_url.trim_end_matches('/')
        //         ),
        //         &payload,
        //     )
        //     .await?;
        //
        // let has_changes = preview_has_changes(&preview);
        // if json_output {
        //     if !has_changes {
        //         println!(
        //             "{}",
        //             serde_json::to_string_pretty(&ApplyJsonOutput {
        //                 status: "success",
        //                 project_id,
        //                 preview,
        //                 applied: false,
        //                 deployment: None,
        //             })?
        //         );
        //         return Ok(());
        //     }
        // } else {
        //     render_preview(&preview);
        //     if !has_changes {
        //         println!("No changes. Infrastructure is up-to-date.");
        //         return Ok(());
        //     }
        // }
        let preview = Value::Null;

        if json_output && !self.yes {
            return Err(ApplyError::JsonConfirmationRequiresYes);
        }

        if !self.yes && !confirm_apply()? {
            return Err(ApplyError::ApplyCancelled);
        }

        let deployment = self
            .post_authenticated(
                config,
                &format!(
                    "{}/api/v1/projects/{project_id}/deployments",
                    self.api_url.trim_end_matches('/')
                ),
                &payload,
            )
            .await?;

        if json_output {
            println!(
                "{}",
                serde_json::to_string_pretty(&ApplyJsonOutput {
                    status: "success",
                    project_id,
                    preview,
                    applied: true,
                    deployment: Some(deployment),
                })?
            );
            return Ok(());
        }

        Self::print_deployment_success(&self.api_url, &project_id, &deployment);
        Ok(())
    }

    fn build_payload(credible: &CredibleToml, root: &Path) -> Result<ApplyPayload, ApplyError> {
        let mut built_assertions = HashMap::new();
        let mut payload_contracts = BTreeMap::new();

        for (contract_key, contract) in &credible.contracts {
            let mut assertions = Vec::with_capacity(contract.assertions.len());

            for assertion in &contract.assertions {
                let build_key = assertion.file.clone();
                if !built_assertions.contains_key(&build_key) {
                    let output = BuildAndFlattenArgs {
                        root: Some(root.to_path_buf()),
                        assertion_contract: assertion_contract_name(&assertion.file)?,
                    }
                    .run()
                    .map_err(ApplyError::BuildFailed)?;
                    built_assertions.insert(build_key.clone(), output);
                }

                let built = built_assertions.get(&build_key).ok_or_else(|| {
                    ApplyError::InvalidConfig(format!(
                        "Missing build output for assertion file {}",
                        assertion.file
                    ))
                })?;

                assertions.push(ApplyAssertionPayload {
                    file: assertion.file.clone(),
                    args: assertion.args.clone(),
                    flattened_source: built.flattened_source.clone(),
                    bytecode: built.bytecode.clone(),
                    compiler_settings: CompilerSettingsPayload {
                        compiler_version: built.compiler_version.clone(),
                        optimizer_enabled: built.optimizer_enabled,
                        optimizer_runs: built.optimizer_runs,
                        evm_version: built.evm_version.clone(),
                        metadata_bytecode_hash: built.metadata_bytecode_hash.clone(),
                        remappings: built.remappings.clone(),
                        libraries: built.libraries.clone(),
                        compilation_target: built.compilation_target.clone(),
                    },
                });
            }

            payload_contracts.insert(
                contract_key.clone(),
                ApplyContractPayload {
                    address: contract.address.clone(),
                    name: contract.name.clone(),
                    assertions,
                },
            );
        }

        Ok(ApplyPayload {
            environment: credible.environment.clone(),
            contracts: payload_contracts,
        })
    }

    async fn select_project(&self, config: &CliConfig) -> Result<String, ApplyError> {
        let auth = config.auth.as_ref().ok_or(ApplyError::NoAuthToken)?;
        let user_id = auth.user_id.as_deref().ok_or_else(|| {
            ApplyError::InvalidConfig(
                "Missing user_id in auth config. Please run `pcl auth logout` then `pcl auth login` to refresh."
                    .to_string(),
            )
        })?;
        let url = format!(
            "{}/api/v1/projects?user={}",
            self.api_url.trim_end_matches('/'),
            user_id
        );
        let client = reqwest::Client::new();
        let response = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", auth.access_token))
            .send()
            .await
            .map_err(|source| {
                ApplyError::Network {
                    endpoint: url.clone(),
                    source,
                }
            })?;

        if response.status().as_u16() == 401 {
            return Err(ApplyError::NoAuthToken);
        }
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            return Err(ApplyError::Api {
                endpoint: url,
                status,
                body,
            });
        }

        let projects: Vec<Project> = response.json().await.map_err(|source| {
            ApplyError::Network {
                endpoint: "project selection response".to_string(),
                source,
            }
        })?;
        if projects.is_empty() {
            return Err(ApplyError::NoProjectsFound);
        }

        let options: Vec<String> = projects
            .iter()
            .map(|project| format!("{} ({})", project.project_name, project.project_id))
            .collect();
        let selected = Select::new("Select a project to apply to:", options)
            .prompt()
            .map_err(ApplyError::ProjectSelectionFailed)?;

        projects
            .into_iter()
            .find(|project| selected.ends_with(&format!("({})", project.project_id)))
            .map(|project| project.project_id)
            .ok_or_else(|| ApplyError::InvalidConfig("Selected project was not found".to_string()))
    }

    async fn post_authenticated<T: Serialize>(
        &self,
        config: &CliConfig,
        endpoint: &str,
        body: &T,
    ) -> Result<Value, ApplyError> {
        let auth = config.auth.as_ref().ok_or(ApplyError::NoAuthToken)?;
        let client = reqwest::Client::new();
        let response = client
            .post(endpoint)
            .header("Authorization", format!("Bearer {}", auth.access_token))
            .header("Content-Type", "application/json")
            .json(body)
            .send()
            .await
            .map_err(|source| {
                ApplyError::Network {
                    endpoint: endpoint.to_string(),
                    source,
                }
            })?;

        if response.status().as_u16() == 401 {
            return Err(ApplyError::NoAuthToken);
        }
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            return Err(ApplyError::Api {
                endpoint: endpoint.to_string(),
                status,
                body,
            });
        }

        response.json().await.map_err(|source| {
            ApplyError::Network {
                endpoint: endpoint.to_string(),
                source,
            }
        })
    }

    fn print_deployment_success(platform_url: &str, project_id: &str, deployment: &Value) {
        let deployment_number = deployment
            .get("deployment_number")
            .or_else(|| deployment.get("deploymentNumber"))
            .or_else(|| deployment.get("number"))
            .and_then(Value::as_u64)
            .map_or_else(|| "created".to_string(), |value| value.to_string());

        let deployment_id = deployment
            .get("deployment_id")
            .or_else(|| deployment.get("deploymentId"))
            .or_else(|| deployment.get("id"))
            .and_then(Value::as_str)
            .map_or_else(|| deployment_number.clone(), ToString::to_string);

        println!(
            "Deployment #{deployment_number} created. Review at: {}/projects/{project_id}/deployments/{deployment_id}",
            platform_url.trim_end_matches('/')
        );
    }
}

fn deserialize_args<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::Array(values) => Ok(values.into_iter().map(value_to_string).collect()),
        Value::Null => Ok(vec![]),
        other => Ok(vec![value_to_string(other)]),
    }
}

fn value_to_string(value: Value) -> String {
    match value {
        Value::String(value) => value,
        other => other.to_string(),
    }
}

fn canonicalize_root(root: &Path) -> Result<PathBuf, ApplyError> {
    std::fs::canonicalize(root).map_err(ApplyError::Io)
}

fn read_credible_toml(root: &Path) -> Result<CredibleToml, ApplyError> {
    let path = root.join("credible.toml");
    let contents = std::fs::read_to_string(path).map_err(ApplyError::Io)?;
    toml::from_str(&contents).map_err(ApplyError::Toml)
}

fn assertion_contract_name(file: &str) -> Result<String, ApplyError> {
    if let Some((_, contract_name)) = file.rsplit_once(':') {
        return Ok(contract_name.to_string());
    }

    let file_name = Path::new(file)
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| ApplyError::InvalidConfig(format!("Invalid assertion file path: {file}")))?;

    for suffix in [".a.sol", ".sol"] {
        if let Some(contract_name) = file_name.strip_suffix(suffix) {
            return Ok(contract_name.to_string());
        }
    }

    Err(ApplyError::InvalidConfig(format!(
        "Could not infer assertion contract from file {file}"
    )))
}

#[cfg_attr(not(test), allow(dead_code))]
fn preview_has_changes(preview: &Value) -> bool {
    if let Some(has_changes) = preview.get("has_changes").and_then(Value::as_bool) {
        return has_changes;
    }
    if let Some(no_changes) = preview.get("no_changes").and_then(Value::as_bool) {
        return !no_changes;
    }
    if let Some(summary) = preview.get("summary").and_then(Value::as_object) {
        let total = ["create", "update", "delete", "replace"]
            .into_iter()
            .filter_map(|key| summary.get(key).and_then(Value::as_u64))
            .sum::<u64>();
        if total > 0 {
            return true;
        }
    }
    for key in ["changes", "operations", "diff", "plan"] {
        if let Some(values) = preview.get(key).and_then(Value::as_array) {
            return !values.is_empty();
        }
    }

    true
}

#[allow(dead_code)]
fn render_preview(preview: &Value) {
    println!("Preview:");
    if let Some(summary) = preview.get("summary").and_then(Value::as_object) {
        let create = summary.get("create").and_then(Value::as_u64).unwrap_or(0);
        let update = summary.get("update").and_then(Value::as_u64).unwrap_or(0);
        let delete = summary.get("delete").and_then(Value::as_u64).unwrap_or(0);
        let replace = summary.get("replace").and_then(Value::as_u64).unwrap_or(0);
        println!(
            "  Plan: {create} to add, {update} to change, {delete} to destroy, {replace} to replace."
        );
    }

    if let Some(changes) = preview
        .get("changes")
        .or_else(|| preview.get("operations"))
        .and_then(Value::as_array)
    {
        for change in changes {
            let action = change
                .get("action")
                .or_else(|| change.get("kind"))
                .and_then(Value::as_str)
                .unwrap_or("change");
            let target = change
                .get("target")
                .or_else(|| change.get("resource"))
                .or_else(|| change.get("name"))
                .and_then(Value::as_str)
                .unwrap_or("resource");
            println!("  {action} {target}");
        }
        return;
    }

    println!(
        "{}",
        serde_json::to_string_pretty(preview).unwrap_or_else(|_| preview.to_string())
    );
}

fn confirm_apply() -> Result<bool, ApplyError> {
    eprint!("Do you want to apply these changes? Only 'yes' will be accepted: ");
    stderr().flush().map_err(ApplyError::Io)?;
    let mut input = String::new();
    stdin().read_line(&mut input).map_err(ApplyError::Io)?;
    Ok(input.trim() == "yes")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn infers_assertion_contract_name_from_solidity_path() {
        assert_eq!(
            assertion_contract_name("assertions/src/MockAssertion.a.sol").unwrap(),
            "MockAssertion"
        );
        assert_eq!(
            assertion_contract_name("assertions/src/Other.sol:NamedAssertion").unwrap(),
            "NamedAssertion"
        );
    }

    #[test]
    fn detects_preview_noop_from_summary() {
        let preview = json!({
            "summary": {
                "create": 0,
                "update": 0,
                "delete": 0,
                "replace": 0
            },
            "changes": []
        });

        assert!(!preview_has_changes(&preview));
    }
}
