use super::{
    ReplayError,
    ReplayExecutionSummary,
};
use crate::config::Config;
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct ReplayResultNotifier {
    endpoint: String,
    api_key: String,
    client: reqwest::Client,
}

impl ReplayResultNotifier {
    /// Builds a replay result notifier from runtime configuration.
    ///
    /// # Errors
    ///
    /// Returns [`NotifierInitError`] if creating the internal HTTP client fails.
    pub fn from_config(config: &Config) -> Result<Self, NotifierInitError> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(NotifierInitError::ClientBuild)?;

        Ok(Self {
            endpoint: config.replay_result_callback_url.trim().to_string(),
            api_key: config.replay_result_callback_api_key.trim().to_string(),
            client,
        })
    }

    pub(crate) async fn notify(
        &self,
        request_id: &str,
        replay_result: &Result<ReplayExecutionSummary, ReplayError>,
    ) -> Result<(), NotifyError> {
        let payload = ReplayResultNotification::from_replay_result(request_id, replay_result);
        let response = self
            .client
            .post(self.endpoint.clone())
            .header("x-api-key", &self.api_key)
            .header("x-request-id", request_id)
            .json(&payload)
            .send()
            .await
            .map_err(NotifyError::Request)?;

        if !response.status().is_success() {
            return Err(NotifyError::UnexpectedStatus(response.status()));
        }

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum NotifierInitError {
    #[error("failed to build replay result notifier HTTP client")]
    ClientBuild(#[source] reqwest::Error),
}

#[derive(Debug, Error)]
pub enum NotifyError {
    #[error("failed to deliver replay result callback")]
    Request(#[source] reqwest::Error),
    #[error("replay result callback returned non-success status {0}")]
    UnexpectedStatus(reqwest::StatusCode),
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
enum ReplayResultStatus {
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
struct ReplayResultNotification {
    request_id: String,
    status: ReplayResultStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<ReplayExecutionSummaryPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl ReplayResultNotification {
    fn from_replay_result(
        request_id: &str,
        replay_result: &Result<ReplayExecutionSummary, ReplayError>,
    ) -> Self {
        match replay_result {
            Ok(result) => {
                Self {
                    request_id: request_id.to_string(),
                    status: ReplayResultStatus::Succeeded,
                    result: Some(ReplayExecutionSummaryPayload::from(result.clone())),
                    error: None,
                }
            }
            Err(error) => {
                Self {
                    request_id: request_id.to_string(),
                    status: ReplayResultStatus::Failed,
                    result: None,
                    error: Some(error.to_string()),
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct ReplayExecutionSummaryPayload {
    start_block: u64,
    head_block: u64,
    replay_window_before: u64,
    replay_window_after: u64,
    elapsed_millis: u128,
    watched_assertion_ids: Vec<alloy::primitives::B256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    matched_assertion_id: Option<alloy::primitives::B256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    matched_block_number: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    matched_tx_hash: Option<alloy::primitives::B256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    matched_incident_payload: Option<sidecar::transaction_observer::DappIncidentPayload>,
}

impl From<ReplayExecutionSummary> for ReplayExecutionSummaryPayload {
    fn from(value: ReplayExecutionSummary) -> Self {
        Self {
            start_block: value.start_block,
            head_block: value.head_block,
            replay_window_before: value.replay_window_before,
            replay_window_after: value.replay_window_after,
            elapsed_millis: value.elapsed_millis,
            watched_assertion_ids: value.watched_assertion_ids,
            matched_assertion_id: value.matched_assertion_id,
            matched_block_number: value.matched_block_number,
            matched_tx_hash: value.matched_tx_hash,
            matched_incident_payload: value.matched_incident_payload,
        }
    }
}
