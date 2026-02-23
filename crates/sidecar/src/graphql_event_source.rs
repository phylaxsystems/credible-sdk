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
use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;

/// Event source that queries a GraphQL API
/// served by the sidecar-indexer.
pub struct GraphqlEventSource {
    client: Client,
    graphql_url: String,
}

impl GraphqlEventSource {
    /// Create a new GraphQL event source from the given config.
    pub fn new(config: GraphqlEventSourceConfig) -> Self {
        Self {
            client: Client::new(),
            graphql_url: config.graphql_url,
        }
    }

    /// Execute a GraphQL query and deserialize the response.
    async fn execute_query<T: serde::de::DeserializeOwned>(
        &self,
        query: &str,
        variables: serde_json::Value,
    ) -> Result<T, EventSourceError> {
        let body = serde_json::json!({
            "query": query,
            "variables": variables,
        });

        let response = self
            .client
            .post(&self.graphql_url)
            .json(&body)
            .send()
            .await
            .map_err(|e| EventSourceError::RequestFailed(e.to_string()))?;

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

#[async_trait]
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
                        assertionAdopter
                        assertionId
                        activationBlock
                    }
                }
            }
        ";

        let variables = serde_json::json!({ "sinceBlock": since_block.cast_signed() });
        let data: AssertionAddedsData = self.execute_query(query, variables).await?;

        data.assertion_addeds
            .nodes
            .into_iter()
            .map(|node| {
                Ok(AssertionAddedEvent {
                    block: node.block.cast_unsigned(),
                    assertion_adopter: node
                        .assertion_adopter
                        .parse::<Address>()
                        .map_err(|e| EventSourceError::ParseError(e.to_string()))?,
                    assertion_id: node
                        .assertion_id
                        .parse::<B256>()
                        .map_err(|e| EventSourceError::ParseError(e.to_string()))?,
                    activation_block: node
                        .activation_block
                        .parse::<u64>()
                        .map_err(|e| EventSourceError::ParseError(e.to_string()))?,
                })
            })
            .collect()
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
                        assertionAdopter
                        assertionId
                        deactivationBlock
                    }
                }
            }
        ";

        let variables = serde_json::json!({ "sinceBlock": since_block.cast_signed() });
        let data: AssertionRemovedsData = self.execute_query(query, variables).await?;

        data.assertion_removeds
            .nodes
            .into_iter()
            .map(|node| {
                Ok(AssertionRemovedEvent {
                    block: node.block.cast_unsigned(),
                    assertion_adopter: node
                        .assertion_adopter
                        .parse::<Address>()
                        .map_err(|e| EventSourceError::ParseError(e.to_string()))?,
                    assertion_id: node
                        .assertion_id
                        .parse::<B256>()
                        .map_err(|e| EventSourceError::ParseError(e.to_string()))?,
                    deactivation_block: node
                        .deactivation_block
                        .parse::<u64>()
                        .map_err(|e| EventSourceError::ParseError(e.to_string()))?,
                })
            })
            .collect()
    }

    async fn get_indexer_head(&self) -> Result<Option<u64>, EventSourceError> {
        let query = r"
            query {
                _meta {
                    block { number }
                }
            }
        ";

        let data: MetaData = self.execute_query(query, serde_json::json!({})).await?;

        Ok(data._meta.block.map(|b| b.number.cast_unsigned()))
    }
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
#[serde(rename_all = "camelCase")]
struct AssertionAddedsData {
    assertion_addeds: ConnectionWrapper<AssertionAddedNode>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AssertionAddedNode {
    block: i64,
    assertion_adopter: String,
    assertion_id: String,
    activation_block: String, // BigInt comes as string from PostGraphile
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AssertionRemovedsData {
    assertion_removeds: ConnectionWrapper<AssertionRemovedNode>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AssertionRemovedNode {
    block: i64,
    assertion_adopter: String,
    assertion_id: String,
    deactivation_block: String,
}

#[derive(Debug, Deserialize)]
struct MetaData {
    _meta: MetaBlock,
}

#[derive(Debug, Deserialize)]
struct MetaBlock {
    block: Option<MetaBlockInner>,
}

#[derive(Debug, Deserialize)]
struct MetaBlockInner {
    number: i64,
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
