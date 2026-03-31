use crate::{
    DEFAULT_PLATFORM_URL,
    config::CliConfig,
    credible_config::{
        CredibleToml,
        assertion_contract_name,
    },
    error::ApplyError,
    verify::{
        VerificationSummary,
        build_deployment_bytecode,
        format_display_name,
        print_verification_summary,
        run_verification,
    },
};
use alloy_primitives::Bytes;
use clap::ValueHint;
use dapp_api_client::generated::client::{
    Client as GeneratedClient,
    types::{
        GetProjectsResponseItem,
        PostProjectsProjectIdReleasesBody,
        PostProjectsProjectIdReleasesBodyContractsValue,
        PostProjectsProjectIdReleasesBodyContractsValueAssertionsItem,
        PostProjectsProjectIdReleasesResponse,
    },
};
use inquire::Select;
use pcl_common::args::CliArgs;
use pcl_phoundry::build_and_flatten::BuildAndFlattenArgs;
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::HashMap,
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
        default_value = DEFAULT_PLATFORM_URL,
        help = "Base URL for the platform API"
    )]
    pub api_url: url::Url,
}

#[derive(Debug, Serialize)]
struct ApplyJsonOutput {
    status: &'static str,
    project_id: Uuid,
    verification: VerificationSummary,
    preview: Value,
    applied: bool,
    release: Option<PostProjectsProjectIdReleasesResponse>,
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
        let (payload, verification_inputs) = Self::build_payload(&credible, &root)?;
        let verification = Self::verify_all_assertions(&verification_inputs, json_output)?;

        // TODO(ENG-2129): Uncomment the preview request and the preview/no-op handling block
        // below once the preview endpoint and diff rendering flow are finalized in follow-up PRs.
        let preview = Value::Null;

        let client = self.authenticated_client(config)?;

        let release = client
            .post_projects_project_id_releases(&project_id, None, &payload)
            .await
            .map(dapp_api_client::generated::client::ResponseValue::into_inner)
            .map_err(|e| {
                ApplyError::Api {
                    endpoint: format!("/projects/{project_id}/releases"),
                    status: e.status().map(|s| s.as_u16()),
                    body: e.to_string(),
                }
            })?;

        if json_output {
            println!(
                "{}",
                serde_json::to_string_pretty(&ApplyJsonOutput {
                    status: "success",
                    project_id,
                    verification,
                    preview,
                    applied: true,
                    release: Some(release),
                })?
            );
            return Ok(());
        }

        Self::print_release_success(self.api_url.as_str(), &project_id, &release);
        Ok(())
    }

    // Build an authenticated generated API client
    fn authenticated_client(&self, config: &CliConfig) -> Result<GeneratedClient, ApplyError> {
        let auth = config.auth.as_ref().ok_or(ApplyError::NoAuthToken)?;
        let mut base = self.api_url.clone();
        base.set_path("/api/v1");
        let base_url = base.to_string();

        let mut headers = reqwest::header::HeaderMap::new();
        let auth_value = format!("Bearer {}", auth.access_token);
        let header_val = reqwest::header::HeaderValue::from_str(&auth_value)
            .map_err(|e| ApplyError::InvalidConfig(format!("Invalid auth token: {e}")))?;
        headers.insert(reqwest::header::AUTHORIZATION, header_val);

        let http_client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .map_err(|e| ApplyError::InvalidConfig(format!("Failed to build HTTP client: {e}")))?;

        Ok(GeneratedClient::new_with_client(&base_url, http_client))
    }

    fn build_payload(
        credible: &CredibleToml,
        root: &Path,
    ) -> Result<(PostProjectsProjectIdReleasesBody, Vec<(String, Bytes)>), ApplyError> {
        let mut built_assertions = HashMap::new();
        let mut payload_contracts = HashMap::new();
        let mut verification_inputs = Vec::new();

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

                let deployment_bytecode =
                    build_deployment_bytecode(&built.bytecode, &built.abi, &assertion.args)
                        .map_err(|e| ApplyError::InvalidConfig(e.to_string()))?;
                let display_name = format_display_name(&contract_name, &assertion.args);
                verification_inputs.push((display_name, deployment_bytecode));

                assertions.push(build_assertion_item(assertion, built, &contract_name)?);
            }

            let contract_value = build_contract_value(contract, assertions)?;
            payload_contracts.insert(contract_key.clone(), contract_value);
        }

        let environment = parse_field(&credible.environment, "environment")?;
        let assertions_dir = parse_field("assertions", "assertions dir")?;

        Ok((
            PostProjectsProjectIdReleasesBody {
                environment,
                assertions_dir,
                contracts: payload_contracts,
                compiler_args: vec![],
            },
            verification_inputs,
        ))
    }

    fn verify_all_assertions(
        inputs: &[(String, Bytes)],
        json_output: bool,
    ) -> Result<VerificationSummary, ApplyError> {
        let refs: Vec<(&str, Bytes)> = inputs
            .iter()
            .map(|(name, bytecode)| (name.as_str(), bytecode.clone()))
            .collect();

        let summary = run_verification(&refs);

        if !json_output {
            println!("pcl apply \u{2014} Verifying assertions...\n");
            print_verification_summary(&summary);
        }

        if summary.failed > 0 {
            if json_output {
                println!("{}", serde_json::to_string_pretty(&summary)?);
            }
            return Err(ApplyError::VerificationFailed(format!(
                "{} of {} assertion{} failed verification. Fix errors before applying.",
                summary.failed,
                summary.total,
                if summary.total == 1 { "" } else { "s" }
            )));
        }

        Ok(summary)
    }

    async fn select_project(&self, config: &CliConfig) -> Result<Uuid, ApplyError> {
        let auth = config.auth.as_ref().ok_or(ApplyError::NoAuthToken)?;
        let user_id = auth.user_id.as_ref().ok_or_else(|| {
            ApplyError::InvalidConfig(
                "Missing user_id in auth config. Please run `pcl auth logout` then `pcl auth login` to refresh."
                    .to_string(),
            )
        })?;

        let client = self.authenticated_client(config)?;
        let projects: Vec<GetProjectsResponseItem> = client
            .get_projects(None, Some(user_id), None)
            .await
            .map(dapp_api_client::generated::client::ResponseValue::into_inner)
            .map_err(|e| {
                ApplyError::Api {
                    endpoint: "/projects".to_string(),
                    status: e.status().map(|s| s.as_u16()),
                    body: e.to_string(),
                }
            })?;

        if projects.is_empty() {
            return Err(ApplyError::NoProjectsFound);
        }

        let options: Vec<String> = projects
            .iter()
            .map(|project| format!("{} ({})", *project.project_name, project.project_id))
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

    fn print_release_success(
        platform_url: &str,
        project_id: &Uuid,
        release: &PostProjectsProjectIdReleasesResponse,
    ) {
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

/// Parse a string into a generated newtype, mapping the error to `ApplyError`.
fn parse_field<T>(value: &str, field: &str) -> Result<T, ApplyError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|e| ApplyError::InvalidConfig(format!("Invalid {field}: {e}")))
}

fn build_assertion_item(
    assertion: &crate::credible_config::CredibleAssertion,
    built: &pcl_phoundry::build_and_flatten::BuildAndFlatOutput,
    contract_name: &str,
) -> Result<PostProjectsProjectIdReleasesBodyContractsValueAssertionsItem, ApplyError> {
    Ok(
        PostProjectsProjectIdReleasesBodyContractsValueAssertionsItem {
            file: parse_field(&assertion.file, "assertion file")?,
            args: assertion.args.clone(),
            bytecode: parse_field(&built.bytecode, "bytecode")?,
            flattened_source: parse_field(&built.flattened_source, "flattened source")?,
            compiler_version: parse_field(&built.compiler_version, "compiler version")?,
            contract_name: parse_field(contract_name, "contract name")?,
            evm_version: parse_field(&built.evm_version, "evm version")?,
            optimizer_runs: built.optimizer_runs,
            optimizer_enabled: built.optimizer_enabled,
            metadata_bytecode_hash: parse_field(
                &built.metadata_bytecode_hash.to_string(),
                "metadata bytecode hash",
            )?,
            libraries: built.libraries.clone(),
        },
    )
}

fn build_contract_value(
    contract: &crate::credible_config::CredibleContract,
    assertions: Vec<PostProjectsProjectIdReleasesBodyContractsValueAssertionsItem>,
) -> Result<PostProjectsProjectIdReleasesBodyContractsValue, ApplyError> {
    Ok(PostProjectsProjectIdReleasesBodyContractsValue {
        address: parse_field(&contract.address, "contract address")?,
        name: Some(parse_field(&contract.name, "contract name")?),
        assertions,
    })
}

fn canonicalize_root(root: &Path) -> Result<PathBuf, ApplyError> {
    std::fs::canonicalize(root).map_err(|e| {
        ApplyError::Io {
            message: format!("Project root not found: {}", root.display()),
            source: e,
        }
    })
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
