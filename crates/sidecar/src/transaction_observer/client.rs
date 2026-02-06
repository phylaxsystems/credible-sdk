use super::{
    TransactionObserverConfig,
    TransactionObserverError,
};
use dapp_api_client::{
    Client as DappClient,
    Config as DappConfig,
};
use std::sync::Arc;
use url::Url;

/// Builds the dapp client if all required configuration is present.
///
/// Returns `None` if either endpoint or `auth_token` is empty/missing,
/// which disables incident publishing.
pub(super) fn build_dapp_client(
    config: &TransactionObserverConfig,
) -> Result<Option<Arc<DappClient>>, TransactionObserverError> {
    let endpoint = config.endpoint.trim();
    if endpoint.is_empty() {
        return Ok(None);
    }

    // Auth token is required for publishing
    if config.auth_token.trim().is_empty() {
        return Ok(None);
    }

    let base_url = endpoint_base_url(endpoint);
    if base_url.is_empty() {
        return Ok(None);
    }

    let client = DappClient::new(DappConfig::new(base_url)).map_err(|e| {
        TransactionObserverError::PublishFailed {
            reason: format!("Failed to create dapp API client: {e}"),
        }
    })?;

    Ok(Some(Arc::new(client)))
}

fn endpoint_base_url(endpoint: &str) -> String {
    let trimmed = endpoint.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return String::new();
    }

    if let Ok(mut url) = Url::parse(trimmed) {
        let path = url.path().to_string();
        if path.ends_with("/enforcer/incidents") {
            let new_path = path
                .trim_end_matches("/enforcer/incidents")
                .trim_end_matches('/');
            url.set_path(if new_path.is_empty() { "/" } else { new_path });
        }
        return url.to_string().trim_end_matches('/').to_string();
    }

    trimmed
        .trim_end_matches("/enforcer/incidents")
        .trim_end_matches('/')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::endpoint_base_url;

    #[test]
    fn endpoint_base_url_strips_incidents_path() {
        let endpoint = "https://dapp.phylax.systems/api/v1/enforcer/incidents";
        assert_eq!(
            endpoint_base_url(endpoint),
            "https://dapp.phylax.systems/api/v1"
        );
    }

    #[test]
    fn endpoint_base_url_handles_root_path() {
        let endpoint = "https://dapp.phylax.systems/enforcer/incidents";
        assert_eq!(endpoint_base_url(endpoint), "https://dapp.phylax.systems");
    }

    #[test]
    fn endpoint_base_url_preserves_unrelated_paths() {
        let endpoint = "https://dapp.phylax.systems/api/v1/other";
        assert_eq!(
            endpoint_base_url(endpoint),
            "https://dapp.phylax.systems/api/v1/other"
        );
    }

    #[test]
    fn endpoint_base_url_falls_back_without_scheme() {
        let endpoint = "dapp.phylax.systems/api/v1/enforcer/incidents";
        assert_eq!(endpoint_base_url(endpoint), "dapp.phylax.systems/api/v1");
    }

    #[test]
    fn endpoint_base_url_returns_empty_for_whitespace() {
        assert!(endpoint_base_url("   ").is_empty());
    }
}
