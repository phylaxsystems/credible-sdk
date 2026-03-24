use crate::{
    DEFAULT_DAPP_URL,
    config::CliConfig,
    error::ApplyError,
};
use chrono::{
    DateTime,
    Utc,
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
use url::Url;
use uuid::Uuid;

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

    #[arg(
        short = 'c',
        long = "config",
        value_hint = ValueHint::FilePath,
        default_value = "assertions/credible.toml",
        help = "Path to credible.toml, relative to root or absolute"
    )]
    pub config: PathBuf,

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
    project_id: Option<Uuid>,
    contracts: BTreeMap<String, CredibleContract>,
}

impl CredibleToml {
    /// Reads and validates a `credible.toml` file at the given path.
    fn from_path(path: &Path) -> Result<Self, ApplyError> {
        let contents = std::fs::read_to_string(path).map_err(|e| {
            ApplyError::Io {
                message: format!("credible.toml not found at {}", path.display()),
                source: e,
            }
        })?;
        let credible: Self = toml::from_str(&contents).map_err(ApplyError::Toml)?;
        credible.validate()?;
        Ok(credible)
    }

    /// Runs all config validations.
    fn validate(&self) -> Result<(), ApplyError> {
        self.validate_unique_addresses()?;
        Ok(())
    }

    /// Ensures no two contracts share the same address.
    fn validate_unique_addresses(&self) -> Result<(), ApplyError> {
        let mut seen: HashMap<&str, &str> = HashMap::new();
        for (key, contract) in &self.contracts {
            if let Some(existing_key) = seen.get(contract.address.as_str()) {
                return Err(ApplyError::InvalidConfig(format!(
                    "duplicate contract address {}: used by both `{}` and `{}`",
                    contract.address, existing_key, key
                )));
            }
            seen.insert(&contract.address, key);
        }
        Ok(())
    }
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
    project_id: Uuid,
    project_name: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ApplyPayload {
    environment: String,
    assertions_dir: String,
    contracts: BTreeMap<String, ApplyContractPayload>,
}

#[derive(Debug, Serialize)]
struct ApplyContractPayload {
    address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    assertions: Vec<ApplyAssertionPayload>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ApplyAssertionPayload {
    file: String,
    args: Vec<String>,
    bytecode: String,
    flattened_source: String,
    compiler_version: String,
    contract_name: String,
    evm_version: String,
    optimizer_runs: u64,
    optimizer_enabled: bool,
    metadata_bytecode_hash: String,
    libraries: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
struct ApplyJsonOutput {
    status: &'static str,
    project_id: Uuid,
    preview: Value,
    applied: bool,
    release: Option<ReleaseResponse>,
}

/// Response from `POST /projects/{id}/releases`
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ReleaseResponse {
    id: Uuid,
    release_number: u64,
    status: String,
    previously_deployed: bool,
    diff: Option<Value>,
    diffed_against_release_id: Option<Uuid>,
    created_at: DateTime<Utc>,
    review_url: Option<Url>,
}

impl ApplyArgs {
    pub async fn run(&self, cli_args: &CliArgs, config: &CliConfig) -> Result<(), ApplyError> {
        let json_output = cli_args.json_output() || self.json;
        let root = canonicalize_root(&self.root)?;
        let config_path = root.join(&self.config);
        let credible = CredibleToml::from_path(&config_path)?;
        let project_id = match credible.project_id {
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

        // TODO: Re-enable confirmation prompt once preview is implemented
        // if json_output && !self.yes {
        //     return Err(ApplyError::JsonConfirmationRequiresYes);
        // }
        //
        // if !self.yes && !confirm_apply()? {
        //     return Err(ApplyError::ApplyCancelled);
        // }

        let release: ReleaseResponse = self
            .post_authenticated(
                config,
                &format!(
                    "{}/api/v1/projects/{project_id}/releases",
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
                    release: Some(release),
                })?
            );
            return Ok(());
        }

        Self::print_release_success(&self.api_url, &project_id, &release);
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

                let contract_name = assertion_contract_name(&assertion.file)?;
                assertions.push(ApplyAssertionPayload {
                    file: assertion.file.clone(),
                    args: assertion.args.clone(),
                    flattened_source: built.flattened_source.clone(),
                    bytecode: built.bytecode.clone(),
                    compiler_version: built.compiler_version.clone(),
                    contract_name,
                    evm_version: built.evm_version.clone(),
                    optimizer_runs: built.optimizer_runs,
                    optimizer_enabled: built.optimizer_enabled,
                    metadata_bytecode_hash: built.metadata_bytecode_hash.to_string(),
                    libraries: built.libraries.clone(),
                });
            }

            payload_contracts.insert(
                contract_key.clone(),
                ApplyContractPayload {
                    address: contract.address.clone(),
                    name: Some(contract.name.clone()),
                    assertions,
                },
            );
        }

        Ok(ApplyPayload {
            environment: credible.environment.clone(),
            assertions_dir: "assertions".to_string(),
            contracts: payload_contracts,
        })
    }

    async fn select_project(&self, config: &CliConfig) -> Result<Uuid, ApplyError> {
        let auth = config.auth.as_ref().ok_or(ApplyError::NoAuthToken)?;
        let user_id = auth.user_id.as_ref().ok_or_else(|| {
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

    async fn post_authenticated<T: Serialize, R: serde::de::DeserializeOwned>(
        &self,
        config: &CliConfig,
        endpoint: &str,
        body: &T,
    ) -> Result<R, ApplyError> {
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

    fn print_release_success(platform_url: &str, project_id: &Uuid, release: &ReleaseResponse) {
        let review_url = Url::parse(platform_url).map(|mut url| {
            url.set_path(&format!(
                "/dashboard/projects/{project_id}/releases/{}",
                release.id
            ));
            url
        });
        println!(
            "Release #{} created.\nReview at: {}",
            release.release_number,
            review_url.as_ref().map_or_else(
                |_| {
                    format!(
                        "{}/dashboard/projects/{project_id}/releases/{}",
                        platform_url.trim_end_matches('/'),
                        release.id
                    )
                },
                ToString::to_string
            )
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
    std::fs::canonicalize(root).map_err(|e| {
        ApplyError::Io {
            message: format!("Project root not found: {}", root.display()),
            source: e,
        }
    })
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

#[allow(dead_code)]
// TODO: to reuse when preview diff is activated
fn confirm_apply() -> Result<bool, ApplyError> {
    eprint!("Do you want to apply these changes? Only 'yes' will be accepted: ");
    stderr().flush().map_err(|e| {
        ApplyError::Io {
            message: "Failed to flush stderr".to_string(),
            source: e,
        }
    })?;
    let mut input = String::new();
    stdin().read_line(&mut input).map_err(|e| {
        ApplyError::Io {
            message: "Failed to read from stdin".to_string(),
            source: e,
        }
    })?;
    Ok(input.trim() == "yes")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;
    use tempfile::TempDir;

    const VALID_CREDIBLE_TOML: &str = r#"
        environment = "production"
        [contracts.my_contract]
        address = "0x1234567890abcdef1234567890abcdef12345678"
        name = "MockProtocol"
        [[contracts.my_contract.assertions]]
        file = "src/NoArgsAssertion.a.sol"
    "#;

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

    #[test]
    fn toml_rejects_duplicate_contract_keys() {
        let toml_str = r#"
            environment = "production"
            [contracts.ownable]
            address = "0xD1f444eA1D2d9fA567F8fD73b15199F90e630074"
            name = "Ownable"
            [[contracts.ownable.assertions]]
            file = "src/OwnableAssertion.a.sol"

            [contracts.ownable]
            address = "0xC9734723aAD51626dC9244fed32668ccb280856A"
            name = "Ownable2"
            [[contracts.ownable.assertions]]
            file = "src/OwnableAssertion.a.sol"
        "#;
        let result: Result<CredibleToml, toml::de::Error> =
            toml::from_str::<CredibleToml>(toml_str);
        assert!(result.is_err(), "TOML should reject duplicate keys");
    }

    #[test]
    fn rejects_duplicate_contract_addresses() {
        let toml_str = r#"
            environment = "production"
            [contracts.ownable]
            address = "0xD1f444eA1D2d9fA567F8fD73b15199F90e630074"
            name = "Ownable"
            [[contracts.ownable.assertions]]
            file = "src/OwnableAssertion.a.sol"

            [contracts.ownable2]
            address = "0xD1f444eA1D2d9fA567F8fD73b15199F90e630074"
            name = "Ownable2"
            [[contracts.ownable2.assertions]]
            file = "src/OwnableAssertion.a.sol"
        "#;
        let credible: CredibleToml = toml::from_str(toml_str).unwrap();
        let err = credible.validate_unique_addresses().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("duplicate contract address"),
            "expected duplicate address error, got: {msg}"
        );
        assert!(msg.contains("0xD1f444eA1D2d9fA567F8fD73b15199F90e630074"));
    }

    #[test]
    fn accepts_distinct_contract_addresses() {
        let toml_str = r#"
            environment = "production"
            [contracts.ownable]
            address = "0xD1f444eA1D2d9fA567F8fD73b15199F90e630074"
            name = "Ownable"
            [[contracts.ownable.assertions]]
            file = "src/OwnableAssertion.a.sol"

            [contracts.ownable2]
            address = "0xC9734723aAD51626dC9244fed32668ccb280856A"
            name = "Ownable2"
            [[contracts.ownable2.assertions]]
            file = "src/OwnableAssertion.a.sol"
        "#;
        let credible: CredibleToml = toml::from_str(toml_str).unwrap();
        credible.validate_unique_addresses().unwrap();
    }

    #[test]
    fn read_credible_toml_resolves_config_path() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();

        // default — assertions/credible.toml
        let assertions_dir = root.join("assertions");
        fs::create_dir_all(&assertions_dir).unwrap();
        fs::write(assertions_dir.join("credible.toml"), VALID_CREDIBLE_TOML).unwrap();

        let credible = CredibleToml::from_path(&root.join("assertions/credible.toml")).unwrap();
        assert_eq!(credible.environment, "production");
        assert_eq!(
            credible.contracts.get("my_contract").unwrap().name,
            "MockProtocol"
        );

        // backward compat, credible.toml at project root
        fs::write(root.join("credible.toml"), VALID_CREDIBLE_TOML).unwrap();

        let credible = CredibleToml::from_path(&root.join("credible.toml")).unwrap();
        assert_eq!(credible.environment, "production");

        // arbitrary path — custom/path/credible.toml
        let custom_dir = root.join("custom").join("path");
        fs::create_dir_all(&custom_dir).unwrap();
        fs::write(custom_dir.join("credible.toml"), VALID_CREDIBLE_TOML).unwrap();

        let credible = CredibleToml::from_path(&root.join("custom/path/credible.toml")).unwrap();
        assert_eq!(credible.environment, "production");

        // missing config yields ConfigNotFound
        let err = CredibleToml::from_path(&root.join("nonexistent/credible.toml")).unwrap_err();
        assert!(matches!(err, ApplyError::Io { .. }));
    }
}
