use crate::config::CliConfig;
use dapp_api_client::generated::client::Client as GeneratedClient;

#[derive(Debug, thiserror::Error)]
pub enum ClientBuildError {
    #[error("Run `pcl auth login` first")]
    NoAuthToken,

    #[error("Invalid config: {0}")]
    InvalidConfig(String),
}

pub fn authenticated_client(
    config: &CliConfig,
    api_url: &url::Url,
) -> Result<GeneratedClient, ClientBuildError> {
    let auth = config.auth.as_ref().ok_or(ClientBuildError::NoAuthToken)?;
    let mut base = api_url.clone();
    base.set_path("/api/v1");
    let base_url = base.to_string();

    let mut headers = reqwest::header::HeaderMap::new();
    let auth_value = format!("Bearer {}", auth.access_token);
    let header_val = reqwest::header::HeaderValue::from_str(&auth_value)
        .map_err(|e| ClientBuildError::InvalidConfig(format!("Invalid auth token: {e}")))?;
    headers.insert(reqwest::header::AUTHORIZATION, header_val);

    let http_client = reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .map_err(|e| {
            ClientBuildError::InvalidConfig(format!("Failed to build HTTP client: {e}"))
        })?;

    Ok(GeneratedClient::new_with_client(&base_url, http_client))
}
