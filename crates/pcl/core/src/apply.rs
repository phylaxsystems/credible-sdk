use crate::{
    DEFAULT_PLATFORM_URL,
    client::authenticated_client,
    config::CliConfig,
    credible_config::{
        CredibleToml,
        assertion_contract_name,
    },
    diff::PreviewResponse,
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
    preview: Option<PreviewResponse>,
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

        let (http_client, base_url) = Self::build_http_client(config, &self.api_url)?;
        let preview = Self::call_preview(&http_client, &base_url, &project_id, &payload).await?;

        if !preview.has_changes() {
            if json_output {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&ApplyJsonOutput {
                        status: "no_changes",
                        project_id,
                        verification,
                        preview: Some(preview),
                        applied: false,
                        release: None,
                    })?
                );
            } else {
                println!("{}", crate::diff::NO_CHANGES_MESSAGE);
            }
            return Ok(());
        }

        if !json_output {
            print!("{}", preview.render_plan());
        }

        if !self.yes {
            if json_output {
                return Err(ApplyError::JsonConfirmationRequiresYes);
            }
            if !confirm_apply()? {
                return Err(ApplyError::ApplyCancelled);
            }
        }

        let client = self.build_client(config)?;

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
                    preview: Some(preview),
                    applied: true,
                    release: Some(release),
                })?
            );
            return Ok(());
        }

        Self::print_release_success(self.api_url.as_str(), &project_id, &release);
        Ok(())
    }

    fn build_client(&self, config: &CliConfig) -> Result<GeneratedClient, ApplyError> {
        authenticated_client(config, &self.api_url).map_err(|e| match e {
            crate::client::ClientBuildError::NoAuthToken => ApplyError::NoAuthToken,
            crate::client::ClientBuildError::InvalidConfig(msg) => {
                ApplyError::InvalidConfig(msg)
            }
        })
    }

    fn build_http_client(
        config: &CliConfig,
        api_url: &Url,
    ) -> Result<(reqwest::Client, String), ApplyError> {
        let auth = config.auth.as_ref().ok_or(ApplyError::NoAuthToken)?;
        let mut base = api_url.clone();
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

        Ok((http_client, base_url))
    }

    async fn call_preview(
        http_client: &reqwest::Client,
        base_url: &str,
        project_id: &Uuid,
        payload: &PostProjectsProjectIdReleasesBody,
    ) -> Result<PreviewResponse, ApplyError> {
        let url = format!("{base_url}/projects/{project_id}/releases/preview");
        let response = http_client
            .post(&url)
            .json(payload)
            .send()
            .await
            .map_err(|e| {
                ApplyError::Api {
                    endpoint: format!("/projects/{project_id}/releases/preview"),
                    status: e.status().map(|s| s.as_u16()),
                    body: e.to_string(),
                }
            })?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            return Err(ApplyError::Api {
                endpoint: format!("/projects/{project_id}/releases/preview"),
                status: Some(status),
                body,
            });
        }

        response.json::<PreviewResponse>().await.map_err(|e| {
            ApplyError::Api {
                endpoint: format!("/projects/{project_id}/releases/preview"),
                status: None,
                body: format!("Failed to parse preview response: {e}"),
            }
        })
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

        let client = self.build_client(config)?;
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

fn confirm_apply() -> Result<bool, ApplyError> {
    eprint!("Do you want to apply these changes? [Y/n]: ");
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
    let trimmed = input.trim();
    Ok(trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("y")
        || trimmed.eq_ignore_ascii_case("yes"))
}
