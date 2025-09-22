use crate::{
    config::CliConfig,
    default_dapp_url,
    error::DappSubmitError,
};
use color_eyre::Result;
use colored::*;
use reqwest::Client;
use serde::Serialize;

/// Project-related commands for the PCL CLI
#[derive(clap::Parser)]
#[command(about = "Manage projects on the Credible Layer dApp")]
pub struct ProjectCommand {
    #[command(subcommand)]
    pub command: ProjectSubcommands,

    #[arg(
        long = "base-url",
        env = "PROJECT_BASE_URL",
        default_value = concat!(default_dapp_url!(), "/api/v1"),
        help = "Base URL for project service"
    )]
    pub base_url: String,
}

#[derive(clap::Subcommand)]
#[command(about = "Project operations")]
pub enum ProjectSubcommands {
    /// Create a new project
    #[command(
        long_about = "Create a new project on the Credible Layer dApp.",
        after_help = "Example: pcl project create --project-name MyProject --chain-id 1"
    )]
    Create {
        #[arg(long)]
        project_name: String,
        #[arg(long)]
        project_description: Option<String>,
        #[arg(long)]
        profile_image_url: Option<String>,
        #[arg(long, required = true)]
        assertion_adopters: Vec<String>,
        #[arg(long)]
        chain_id: u64,
    },
}

#[derive(Serialize)]
struct CreateProjectRequest {
    project_name: String,
    project_description: Option<String>,
    profile_image_url: Option<String>,
    assertion_adopters: Vec<String>,
    chain_id: u64,
}

impl ProjectCommand {
    pub async fn run(&self, config: &mut CliConfig) -> Result<(), DappSubmitError> {
        match &self.command {
            ProjectSubcommands::Create {
                project_name,
                project_description,
                profile_image_url,
                assertion_adopters,
                chain_id,
            } => {
                let auth = config.auth.as_ref().ok_or_else(|| {
                    Self::display_auth_required();
                    DappSubmitError::NoAuthToken
                })?;
                let req_body = CreateProjectRequest {
                    project_name: project_name.clone(),
                    project_description: project_description.clone(),
                    profile_image_url: profile_image_url.clone(),
                    assertion_adopters: assertion_adopters.clone(),
                    chain_id: *chain_id,
                };
                let client = Client::new();
                let url = format!("{}/projects/create", self.base_url.trim_end_matches('/'));
                let resp = client
                    .post(url)
                    .header("Content-Type", "application/json")
                    .header("Authorization", format!("Bearer {}", auth.access_token))
                    .json(&req_body)
                    .send()
                    .await
                    .map_err(DappSubmitError::ApiConnectionError)?;
                if resp.status().is_success() {
                    println!("{} Project created successfully!", "✅".green());
                    println!("\n{}", "Next steps:".bold());
                    println!(
                        "  • View your project at {}",
                        "https://dapp.phylax.systems".cyan()
                    );
                    println!(
                        "  • Submit assertions using: {}",
                        format!("pcl submit -p \"{}\"", project_name).yellow()
                    );
                    Ok(())
                } else {
                    println!("{:#?}", resp);
                    let err_text = resp
                        .text()
                        .await
                        .unwrap_or_else(|_| "Unknown error".to_string());
                    Err(DappSubmitError::SubmissionFailed(err_text))
                }
            }
        }
    }

    /// Display instructions for authentication when not logged in
    fn display_auth_required() {
        println!("{} {}", "❌".red(), "Authentication required".red().bold());
        println!("\nTo create a project, you need to be authenticated first.");
        println!("\n{}", "Please run:".bold());
        println!("  {} {}", "→".cyan(), "pcl auth login".yellow().bold());
        println!("\nThis will open a browser window for wallet authentication.");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CliConfig,
        UserAuth,
    };
    use alloy_primitives::Address;
    use chrono::Utc;
    use mockito::Server;

    fn create_test_config() -> CliConfig {
        CliConfig {
            auth: Some(UserAuth {
                access_token: "test_token".to_string(),
                refresh_token: "test_refresh".to_string(),
                user_address: Address::from_slice(&[0; 20]),
                expires_at: Utc::now(),
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_create_project_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/projects/create")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"project_id":"123"}"#)
            .create();

        let cmd = ProjectCommand {
            command: ProjectSubcommands::Create {
                project_name: "Test Project".to_string(),
                project_description: Some("desc".to_string()),
                profile_image_url: None,
                assertion_adopters: vec!["0xabc".to_string()],
                chain_id: 1,
            },
            base_url: server.url(),
        };
        let mut config = create_test_config();
        let result = cmd.run(&mut config).await;
        assert!(result.is_ok());
        mock.assert();
    }

    #[tokio::test]
    async fn test_create_project_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/projects/create")
            .with_status(400)
            .with_header("content-type", "application/json")
            .with_body(r#"{"error":"bad request"}"#)
            .create();

        let cmd = ProjectCommand {
            command: ProjectSubcommands::Create {
                project_name: "Test Project".to_string(),
                project_description: Some("desc".to_string()),
                profile_image_url: None,
                assertion_adopters: vec!["0xabc".to_string()],
                chain_id: 1,
            },
            base_url: server.url(),
        };
        let mut config = create_test_config();
        let result = cmd.run(&mut config).await;
        assert!(result.is_err());
        mock.assert();
    }

    #[tokio::test]
    async fn test_create_project_no_auth() {
        let cmd = ProjectCommand {
            command: ProjectSubcommands::Create {
                project_name: "Test Project".to_string(),
                project_description: None,
                profile_image_url: None,
                assertion_adopters: vec!["0xabc".to_string()],
                chain_id: 1,
            },
            base_url: concat!(default_dapp_url!(), "/api/v1").to_string(),
        };

        let mut config = CliConfig::default(); // No auth
        let result = cmd.run(&mut config).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DappSubmitError::NoAuthToken));
    }
}
