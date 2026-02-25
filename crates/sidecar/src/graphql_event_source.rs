//! # `graphql_event_source`
//! 
//! GraphQL-based implementation of [`EventSource`].
//!
//! Queries the sidecar-indexer's GraphQL API for assertion events.

use alloy_primitives::{
    Address,
    B256,
};
use assertion_executor::store::{
    AssertionAddedEvent,
    AssertionRemovedEvent,
    EventSource,
    EventSourceError,
};
use reqwest::Client;
use serde::{
    Deserialize,
    Deserializer,
    Serialize,
};
use std::time::Duration;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Configuration for the GraphQL event source.
#[derive(Debug, Clone)]
pub struct GraphqlEventSourceConfig {
    /// URL of the GraphQL API, e.g. `<http://localhost:4350/graphql>`
    pub graphql_url: String,
}

/// Event source that queries a GraphQL API
/// served by the sidecar-indexer.
pub struct GraphqlEventSource {
    client: Client,
    graphql_url: String,
}

impl GraphqlEventSource {
    /// Create a new GraphQL event source from the given config.
    pub fn new(config: GraphqlEventSourceConfig) -> Self {
        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .unwrap_or_default();
        Self {
            client,
            graphql_url: config.graphql_url,
        }
    }

    /// Execute a GraphQL query and deserialize the response.
    async fn execute_query<T: serde::de::DeserializeOwned, V: Serialize>(
        &self,
        query: &str,
        variables: V,
    ) -> Result<T, EventSourceError> {
        let body = GraphqlRequest { query, variables };

        let response = self
            .client
            .post(&self.graphql_url)
            .json(&body)
            .send()
            .await
            .map_err(|e| EventSourceError::RequestFailed(e.to_string()))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(EventSourceError::RequestFailed(format!(
                "HTTP {status}: {body}"
            )));
        }

        let gql_response: GraphqlResponse<T> = response
            .json()
            .await
            .map_err(|e| EventSourceError::ParseError(e.to_string()))?;

        if let Some(errors) = gql_response.errors {
            let messages: Vec<String> = errors.into_iter().map(|e| e.message).collect();
            return Err(EventSourceError::RequestFailed(messages.join("; ")));
        }

        gql_response
            .data
            .ok_or_else(|| EventSourceError::ParseError("No data in response".to_string()))
    }
}

impl EventSource for GraphqlEventSource {
    async fn fetch_added_events(
        &self,
        since_block: u64,
    ) -> Result<Vec<AssertionAddedEvent>, EventSourceError> {
        let query = r"
            query FetchAdded($sinceBlock: Int!) {
                assertionAddeds(
                    filter: { block: { greaterThan: $sinceBlock } }
                    orderBy: BLOCK_ASC
                ) {
                    nodes {
                        block
                        logIndex
                        assertionAdopter
                        assertionId
                        activationBlock
                    }
                }
            }
        ";

        let variables = BlockFilterVars {
            since_block: since_block.cast_signed(),
        };
        let data: AssertionAddedData = self.execute_query(query, variables).await?;

        Ok(data
            .assertion_added
            .nodes
            .into_iter()
            .map(Into::into)
            .collect())
    }

    async fn fetch_removed_events(
        &self,
        since_block: u64,
    ) -> Result<Vec<AssertionRemovedEvent>, EventSourceError> {
        let query = r"
            query FetchRemoved($sinceBlock: Int!) {
                assertionRemoveds(
                    filter: { block: { greaterThan: $sinceBlock } }
                    orderBy: BLOCK_ASC
                ) {
                    nodes {
                        block
                        logIndex
                        assertionAdopter
                        assertionId
                        deactivationBlock
                    }
                }
            }
        ";

        let variables = BlockFilterVars {
            since_block: since_block.cast_signed(),
        };
        let data: AssertionRemovedData = self.execute_query(query, variables).await?;

        Ok(data
            .assertion_removed
            .nodes
            .into_iter()
            .map(Into::into)
            .collect())
    }

    async fn get_indexer_head(&self) -> Result<Option<u64>, EventSourceError> {
        let query = r"
            query {
                _meta {
                    block { number }
                }
            }
        ";

        let data: MetaData = self.execute_query(query, ()).await?;

        Ok(data.meta.block.map(|b| b.number.cast_unsigned()))
    }
}

#[derive(Serialize)]
struct GraphqlRequest<'a, V: Serialize> {
    query: &'a str,
    variables: V,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BlockFilterVars {
    since_block: i64,
}

// GraphQL response types
#[derive(Debug, Deserialize)]
struct GraphqlResponse<T> {
    data: Option<T>,
    errors: Option<Vec<GraphqlError>>,
}

/// Configuration for the GraphQL event source.
#[derive(Debug, Clone)]
pub struct GraphqlEventSourceConfig {
    /// URL of the GraphQL API, e.g. `<http://localhost:4350/graphql>`
    pub graphql_url: String,
}

#[derive(Debug, Deserialize)]
struct GraphqlError {
    message: String,
}

#[derive(Debug, Deserialize)]
struct ConnectionWrapper<T> {
    nodes: Vec<T>,
}

#[derive(Debug, Deserialize)]
struct AssertionAddedData {
    #[serde(rename = "assertionAddeds")]
    assertion_added: ConnectionWrapper<AssertionAddedNode>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AssertionAddedNode {
    block: i64,
    log_index: i64,
    assertion_adopter: Address,
    assertion_id: B256,
    #[serde(deserialize_with = "deserialize_bigint_string")]
    activation_block: u64,
}

impl From<AssertionAddedNode> for AssertionAddedEvent {
    fn from(node: AssertionAddedNode) -> Self {
        Self {
            block: node.block.cast_unsigned(),
            log_index: node.log_index.cast_unsigned(),
            assertion_adopter: node.assertion_adopter,
            assertion_id: node.assertion_id,
            activation_block: node.activation_block,
        }
    }
}

#[derive(Debug, Deserialize)]
struct AssertionRemovedData {
    #[serde(rename = "assertionRemoveds")]
    assertion_removed: ConnectionWrapper<AssertionRemovedNode>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AssertionRemovedNode {
    block: i64,
    log_index: i64,
    assertion_adopter: Address,
    assertion_id: B256,
    #[serde(deserialize_with = "deserialize_bigint_string")]
    deactivation_block: u64,
}

impl From<AssertionRemovedNode> for AssertionRemovedEvent {
    fn from(node: AssertionRemovedNode) -> Self {
        Self {
            block: node.block.cast_unsigned(),
            log_index: node.log_index.cast_unsigned(),
            assertion_adopter: node.assertion_adopter,
            assertion_id: node.assertion_id,
            deactivation_block: node.deactivation_block,
        }
    }
}

#[derive(Debug, Deserialize)]
struct MetaData {
    #[serde(rename = "_meta")]
    meta: MetaBlock,
}

#[derive(Debug, Deserialize)]
struct MetaBlock {
    block: Option<MetaBlockInner>,
}

#[derive(Debug, Deserialize)]
struct MetaBlockInner {
    number: i64,
}

// Deserializes a `PostGraphile` BigInt into `u64`.
fn deserialize_bigint_string<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    let s = String::deserialize(deserializer)?;
    s.parse::<u64>().map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;

    #[tokio::test]
    async fn test_fetch_added_events() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(POST).path("/graphql");
            then.status(200).json_body(serde_json::json!({
                "data": {
                    "assertionAddeds": {
                        "nodes": [{
                            "block": 100,
                            "logIndex": 1,
                            "assertionAdopter": "0x1234567890123456789012345678901234567890",
                            "assertionId": "0x044852b2a670ade5407e78fb2863c51de9fcb96542a07186fe3aeda6bb8a116d",
                            "activationBlock": "150"
                        }]
                    }
                }
            }));
        });

        let source = GraphqlEventSource::new(GraphqlEventSourceConfig {
            graphql_url: server.url("/graphql"),
        });

        let events = source.fetch_added_events(50).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].block, 100);
        assert_eq!(events[0].activation_block, 150);

        mock.assert();
    }

    #[tokio::test]
    async fn test_fetch_removed_events() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(POST).path("/graphql");
            then.status(200).json_body(serde_json::json!({
                "data": {
                    "assertionRemoveds": {
                        "nodes": [{
                            "block": 200,
                            "logIndex": 2,
                            "assertionAdopter": "0x1234567890123456789012345678901234567890",
                            "assertionId": "0x044852b2a670ade5407e78fb2863c51de9fcb96542a07186fe3aeda6bb8a116d",
                            "deactivationBlock": "250"
                        }]
                    }
                }
            }));
        });

        let source = GraphqlEventSource::new(GraphqlEventSourceConfig {
            graphql_url: server.url("/graphql"),
        });

        let events = source.fetch_removed_events(100).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].block, 200);
        assert_eq!(events[0].deactivation_block, 250);

        mock.assert();
    }

    #[tokio::test]
    async fn test_fetch_empty_events() {
        let server = MockServer::start();
        let _mock = server.mock(|when, then| {
            when.method(POST).path("/graphql");
            then.status(200).json_body(serde_json::json!({
                "data": {
                    "assertionAddeds": {
                        "nodes": []
                    }
                }
            }));
        });

        let source = GraphqlEventSource::new(GraphqlEventSourceConfig {
            graphql_url: server.url("/graphql"),
        });

        let events = source.fetch_added_events(0).await.unwrap();
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn test_graphql_error_handling() {
        let server = MockServer::start();
        let _mock = server.mock(|when, then| {
            when.method(POST).path("/graphql");
            then.status(200).json_body(serde_json::json!({
                "errors": [{"message": "Something went wrong"}]
            }));
        });

        let source = GraphqlEventSource::new(GraphqlEventSourceConfig {
            graphql_url: server.url("/graphql"),
        });

        let result = source.fetch_added_events(0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_indexer_head() {
        let server = MockServer::start();
        let _mock = server.mock(|when, then| {
            when.method(POST).path("/graphql");
            then.status(200).json_body(serde_json::json!({
                "data": {
                    "_meta": {
                        "block": { "number": 12345 }
                    }
                }
            }));
        });

        let source = GraphqlEventSource::new(GraphqlEventSourceConfig {
            graphql_url: server.url("/graphql"),
        });

        let head = source.get_indexer_head().await.unwrap();
        assert_eq!(head, Some(12345));
    }

    #[tokio::test]
    async fn test_get_indexer_head_null_block() {
        let server = MockServer::start();
        let _mock = server.mock(|when, then| {
            when.method(POST).path("/graphql");
            then.status(200).json_body(serde_json::json!({
                "data": {
                    "_meta": {
                        "block": null
                    }
                }
            }));
        });

        let source = GraphqlEventSource::new(GraphqlEventSourceConfig {
            graphql_url: server.url("/graphql"),
        });

        let head = source.get_indexer_head().await.unwrap();
        assert_eq!(head, None);
    }
}
