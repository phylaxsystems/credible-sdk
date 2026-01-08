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

pub(super) fn build_dapp_client(
    config: &TransactionObserverConfig,
) -> Result<Option<Arc<DappClient>>, TransactionObserverError> {
    let endpoint = config.endpoint.trim();
    if endpoint.is_empty() {
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
        if url.path().ends_with("/enforcer/incidents") {
            let new_path = url.path().trim_end_matches("/enforcer/incidents");
            let new_path = new_path.trim_end_matches('/');
            url.set_path(if new_path.is_empty() { "/" } else { new_path });
        }
        return url.to_string().trim_end_matches('/').to_string();
    }

    trimmed
        .trim_end_matches("/enforcer/incidents")
        .trim_end_matches('/')
        .to_string()
}
