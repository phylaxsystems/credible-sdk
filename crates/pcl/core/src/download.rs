use crate::{
    DEFAULT_PLATFORM_URL,
    client::authenticated_client,
    config::CliConfig,
};
use alloy_primitives::Address;
use dapp_api_client::generated::client::{
    Client as GeneratedClient,
    types::GetViewsProjectsProjectIdAssertionsAssertionIdAssertionId,
};
use pcl_common::args::CliArgs;
use serde::Serialize;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(clap::Parser, Debug)]
#[command(
    name = "download",
    about = "Download assertion source code for a protocol"
)]
pub struct DownloadArgs {
    #[arg(
        long,
        help = "Project UUID to download assertions from",
        conflicts_with = "manager"
    )]
    pub project_id: Option<Uuid>,

    #[arg(
        long,
        help = "Protocol manager address to look up the project",
        conflicts_with = "project_id"
    )]
    pub manager: Option<Address>,

    #[arg(
        short = 'o',
        long = "output-dir",
        value_hint = clap::ValueHint::DirPath,
        help = "Output directory for .sol files (default: <project_name>-assertions/)"
    )]
    pub output_dir: Option<PathBuf>,

    #[arg(long, help = "Emit machine-readable output for this command")]
    pub json: bool,

    #[arg(
        short = 'u',
        long = "api-url",
        env = "PCL_API_URL",
        value_hint = clap::ValueHint::Url,
        default_value = DEFAULT_PLATFORM_URL,
        help = "Base URL for the platform API"
    )]
    pub api_url: url::Url,
}

#[derive(Debug, thiserror::Error)]
pub enum DownloadError {
    #[error("Run `pcl auth login` first")]
    NoAuthToken,

    #[error("Specify exactly one of --project-id or --manager")]
    MissingIdentifier,

    #[error("No project found for manager address: {0}")]
    ManagerNotFound(String),

    #[error("Multiple projects found for manager address: {0}")]
    MultipleProjectsForManager(String),

    #[error("No assertions found for project")]
    NoAssertionsFound,

    #[error("API request to {endpoint} failed{status_part}: {body}", status_part = .status.map_or(String::new(), |s| format!(" with status {s}")))]
    Api {
        endpoint: String,
        status: Option<u16>,
        body: String,
    },

    #[error("{message}: {source}")]
    Io {
        message: String,
        #[source]
        source: std::io::Error,
    },

    #[error("Failed to encode JSON output: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Invalid config: {0}")]
    InvalidConfig(String),
}

#[derive(Debug, Serialize)]
struct DownloadJsonOutput {
    status: &'static str,
    project_id: Uuid,
    project_name: String,
    files_downloaded: usize,
    files_skipped: usize,
    files: Vec<DownloadedFile>,
}

#[derive(Debug, Serialize)]
struct DownloadedFile {
    assertion_id: String,
    contract_name: String,
    file_name: String,
    source: String,
}

impl DownloadArgs {
    pub async fn run(&self, cli_args: &CliArgs, config: &CliConfig) -> Result<(), DownloadError> {
        let json_output = cli_args.json_output() || self.json;

        let client = self.build_client(config)?;

        let (project_id, project_name) = self.resolve_project(&client).await?;

        let assertions = self.fetch_assertions_list(&client, &project_id).await?;

        if assertions.is_empty() {
            return self.handle_empty_assertions(json_output, project_id, project_name);
        }

        let output_dir = self.prepare_output_dir(&project_name)?;

        if !json_output {
            println!(
                "Downloading {} assertion{} for project \"{project_name}\"...\n",
                assertions.len(),
                if assertions.len() == 1 { "" } else { "s" },
            );
        }

        let (downloaded, skipped) = self
            .download_assertions(&client, &project_id, &assertions, &output_dir, json_output)
            .await?;

        self.print_result(json_output, project_id, project_name, downloaded, skipped, &output_dir)
    }

    fn handle_empty_assertions(
        &self,
        json_output: bool,
        project_id: Uuid,
        project_name: String,
    ) -> Result<(), DownloadError> {
        if json_output {
            println!(
                "{}",
                serde_json::to_string_pretty(&DownloadJsonOutput {
                    status: "no_assertions",
                    project_id,
                    project_name,
                    files_downloaded: 0,
                    files_skipped: 0,
                    files: vec![],
                })?
            );
            return Ok(());
        }
        eprintln!("No assertions found for project.");
        Err(DownloadError::NoAssertionsFound)
    }

    fn prepare_output_dir(&self, project_name: &str) -> Result<PathBuf, DownloadError> {
        let output_dir = self
            .output_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from(format!("{project_name}-assertions")));

        std::fs::create_dir_all(&output_dir).map_err(|e| DownloadError::Io {
            message: format!(
                "Failed to create output directory: {}",
                output_dir.display()
            ),
            source: e,
        })?;

        Ok(output_dir)
    }

    async fn download_assertions(
        &self,
        client: &GeneratedClient,
        project_id: &Uuid,
        assertions: &[dapp_api_client::generated::client::types::GetViewsProjectsProjectIdAssertionsResponseDataAssertionsItem],
        output_dir: &PathBuf,
        json_output: bool,
    ) -> Result<(Vec<DownloadedFile>, usize), DownloadError> {
        let mut downloaded = Vec::new();
        let mut skipped = 0usize;

        for assertion in assertions {
            let assertion_id = &assertion.assertion_id;
            let contract_name = assertion
                .contract_name
                .clone()
                .unwrap_or_else(|| "unknown".to_string());

            let detail = self
                .fetch_assertion_detail(client, project_id, assertion_id)
                .await?;

            let source_code = detail
                .source
                .as_ref()
                .and_then(|s| s.source_code.clone())
                .or_else(|| detail.artifact.as_ref().map(|a| a.solidity_source.clone()));

            if let Some(code) = source_code {
                let id_prefix = assertion_id.get(..8).unwrap_or(assertion_id);
                let file_name = format!("{contract_name}_{id_prefix}.sol");
                let file_path = output_dir.join(&file_name);

                std::fs::write(&file_path, &code).map_err(|e| DownloadError::Io {
                    message: format!("Failed to write file: {}", file_path.display()),
                    source: e,
                })?;

                if !json_output {
                    println!("  {file_name}");
                }

                let source_label = detail
                    .source
                    .as_ref()
                    .filter(|s| s.source_code.is_some())
                    .map_or_else(
                        || {
                            detail
                                .artifact
                                .as_ref()
                                .map(|_| "artifact".to_string())
                                .unwrap_or_default()
                        },
                        |s| s.verification_status.to_string(),
                    );

                downloaded.push(DownloadedFile {
                    assertion_id: assertion_id.clone(),
                    contract_name: contract_name.clone(),
                    file_name: file_name.clone(),
                    source: source_label,
                });
            } else {
                skipped += 1;
                if !json_output {
                    println!("  [skipped] {contract_name} — no source code available");
                }
            }
        }

        Ok((downloaded, skipped))
    }

    fn print_result(
        &self,
        json_output: bool,
        project_id: Uuid,
        project_name: String,
        downloaded: Vec<DownloadedFile>,
        skipped: usize,
        output_dir: &PathBuf,
    ) -> Result<(), DownloadError> {
        if json_output {
            println!(
                "{}",
                serde_json::to_string_pretty(&DownloadJsonOutput {
                    status: "success",
                    project_id,
                    project_name,
                    files_downloaded: downloaded.len(),
                    files_skipped: skipped,
                    files: downloaded,
                })?
            );
        } else {
            println!(
                "\nDone. {} file{} written to {}/ ({skipped} skipped)",
                downloaded.len(),
                if downloaded.len() == 1 { "" } else { "s" },
                output_dir.display(),
            );
        }

        Ok(())
    }

    fn build_client(&self, config: &CliConfig) -> Result<GeneratedClient, DownloadError> {
        authenticated_client(config, &self.api_url).map_err(|e| match e {
            crate::client::ClientBuildError::NoAuthToken => DownloadError::NoAuthToken,
            crate::client::ClientBuildError::InvalidConfig(msg) => {
                DownloadError::InvalidConfig(msg)
            }
        })
    }

    async fn resolve_project(
        &self,
        client: &GeneratedClient,
    ) -> Result<(Uuid, String), DownloadError> {
        if self.project_id.is_some() && self.manager.is_some() {
            return Err(DownloadError::MissingIdentifier);
        }

        if let Some(pid) = self.project_id {
            let project = client
                .get_projects_project_id(&pid, None)
                .await
                .map(dapp_api_client::generated::client::ResponseValue::into_inner)
                .map_err(|e| DownloadError::Api {
                    endpoint: format!("/projects/{pid}"),
                    status: e.status().map(|s| s.as_u16()),
                    body: e.to_string(),
                })?;

            return Ok((project.project_id, project.project_name.to_string()));
        }

        let manager = self
            .manager
            .ok_or(DownloadError::MissingIdentifier)?;

        let manager_str = manager.to_string().to_lowercase();

        let response = client
            .get_views_projects(None, None, None, None, None)
            .await
            .map(dapp_api_client::generated::client::ResponseValue::into_inner)
            .map_err(|e| DownloadError::Api {
                endpoint: "/views/projects".to_string(),
                status: e.status().map(|s| s.as_u16()),
                body: e.to_string(),
            })?;

        let matches: Vec<_> = response
            .data
            .items
            .into_iter()
            .filter(|item| item.project_manager.to_lowercase() == manager_str)
            .collect();

        match matches.len() {
            0 => Err(DownloadError::ManagerNotFound(manager_str)),
            1 => {
                let project = &matches[0];
                let project_id = project.project_id.parse::<Uuid>().map_err(|e| {
                    DownloadError::InvalidConfig(format!("Invalid project UUID: {e}"))
                })?;
                Ok((project_id, project.project_name.clone()))
            }
            _ => Err(DownloadError::MultipleProjectsForManager(manager_str)),
        }
    }

    async fn fetch_assertions_list(
        &self,
        client: &GeneratedClient,
        project_id: &Uuid,
    ) -> Result<
        Vec<
            dapp_api_client::generated::client::types::GetViewsProjectsProjectIdAssertionsResponseDataAssertionsItem,
        >,
        DownloadError,
    >{
        let response = client
            .get_views_projects_project_id_assertions(project_id, None)
            .await
            .map(dapp_api_client::generated::client::ResponseValue::into_inner)
            .map_err(|e| {
                DownloadError::Api {
                    endpoint: format!("/views/projects/{project_id}/assertions"),
                    status: e.status().map(|s| s.as_u16()),
                    body: e.to_string(),
                }
            })?;

        Ok(response.data.assertions)
    }

    async fn fetch_assertion_detail(
        &self,
        client: &GeneratedClient,
        project_id: &Uuid,
        assertion_id: &str,
    ) -> Result<
        dapp_api_client::generated::client::types::GetViewsProjectsProjectIdAssertionsAssertionIdResponseData,
        DownloadError,
    >{
        let aid = GetViewsProjectsProjectIdAssertionsAssertionIdAssertionId::try_from(assertion_id)
            .map_err(|e| DownloadError::InvalidConfig(format!("Invalid assertion ID: {e}")))?;

        let response = client
            .get_views_projects_project_id_assertions_assertion_id(project_id, &aid)
            .await
            .map(dapp_api_client::generated::client::ResponseValue::into_inner)
            .map_err(|e| {
                DownloadError::Api {
                    endpoint: format!("/views/projects/{project_id}/assertions/{assertion_id}"),
                    status: e.status().map(|s| s.as_u16()),
                    body: e.to_string(),
                }
            })?;

        Ok(response.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct TestCli {
        #[command(subcommand)]
        command: TestCommand,
    }

    #[derive(clap::Subcommand)]
    enum TestCommand {
        Download(DownloadArgs),
    }

    #[test]
    fn parses_download_with_project_id() {
        let cli = TestCli::try_parse_from([
            "pcl",
            "download",
            "--project-id",
            "550e8400-e29b-41d4-a716-446655440000",
        ])
        .unwrap();
        match cli.command {
            TestCommand::Download(args) => {
                assert_eq!(
                    args.project_id.unwrap().to_string(),
                    "550e8400-e29b-41d4-a716-446655440000"
                );
                assert!(args.manager.is_none());
            }
        }
    }

    #[test]
    fn parses_download_with_manager() {
        let cli = TestCli::try_parse_from([
            "pcl",
            "download",
            "--manager",
            "0x1234567890abcdef1234567890abcdef12345678",
        ])
        .unwrap();
        match cli.command {
            TestCommand::Download(args) => {
                let expected: Address =
                    "0x1234567890abcdef1234567890abcdef12345678".parse().unwrap();
                assert_eq!(args.manager.unwrap(), expected);
                assert!(args.project_id.is_none());
            }
        }
    }

    #[test]
    fn parses_download_with_output_dir() {
        let cli = TestCli::try_parse_from([
            "pcl",
            "download",
            "--project-id",
            "550e8400-e29b-41d4-a716-446655440000",
            "--output-dir",
            "/tmp/my-sol-files",
        ])
        .unwrap();
        match cli.command {
            TestCommand::Download(args) => {
                assert_eq!(args.output_dir.unwrap(), PathBuf::from("/tmp/my-sol-files"));
            }
        }
    }

    #[test]
    fn parses_download_with_json_flag() {
        let cli = TestCli::try_parse_from([
            "pcl",
            "download",
            "--project-id",
            "550e8400-e29b-41d4-a716-446655440000",
            "--json",
        ])
        .unwrap();
        match cli.command {
            TestCommand::Download(args) => {
                assert!(args.json);
            }
        }
    }

    #[test]
    fn rejects_both_project_id_and_manager() {
        let result = TestCli::try_parse_from([
            "pcl",
            "download",
            "--project-id",
            "550e8400-e29b-41d4-a716-446655440000",
            "--manager",
            "0x1234567890abcdef1234567890abcdef12345678",
        ]);
        assert!(result.is_err());
    }
}
