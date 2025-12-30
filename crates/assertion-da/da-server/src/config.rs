use sled::{
    Config as DbConfig,
    Db,
};
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
};

use bollard::Docker;
use clap::Parser;
use tokio::net::TcpListener;
use tracing::level_filters::LevelFilter;

use crate::{
    api::db::RedisDb,
    server::DaServer,
};

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    /// Path of the database, defaults to /usr/local/assertions
    #[arg(long, env = "DA_DB_PATH")]
    pub db_path: Option<PathBuf>,
    /// Cache size in bytes
    #[arg(long, env = "DA_CACHE_SIZE", default_value = "1000000")]
    pub cache_size: usize,
    /// Api server address
    #[arg(long, env = "DA_LISTEN_ADDR", default_value = "0.0.0.0:5001")]
    pub listen_addr: SocketAddr,
    /// Private key for the assertion DA
    #[arg(long, env = "DA_PRIVATE_KEY")]
    pub private_key: String,
    /// Log level
    #[arg(long, env = "DA_LOG_LEVEL", default_value = "info")]
    pub log_level: LevelFilter,
    /// Metrics server address
    #[arg(long, env = "DA_METRICS_ADDR", default_value = "0.0.0.0:9002")]
    pub metrics_addr: SocketAddr,
    /// Redis URL (if set, Redis will be used instead of Sled)
    #[arg(long, env = "DA_REDIS_URL")]
    pub redis_url: Option<String>,
}

#[allow(clippy::large_enum_variant)]
pub enum DatabaseBackend {
    Sled(DaServer<Db<{ crate::LEAF_FANOUT }>>),
    Redis(DaServer<RedisDb>),
}

impl Config {
    /// Build the assertion DA Server
    ///
    /// # Panics
    ///
    /// Will panic if the root directory is invalid
    pub async fn build(self) -> anyhow::Result<DatabaseBackend> {
        // Bind to an address
        let listener = TcpListener::bind(&self.listen_addr).await?;
        tracing::info!(listen_addr = ?self.listen_addr, "Listening on address");

        let docker = Arc::new(Docker::connect_with_local_defaults()?);
        tracing::info!("Connected to Docker daemon");

        // Check if Redis URL is set
        if let Some(redis_url) = &self.redis_url {
            let client = redis::Client::open(redis_url.as_str())?;
            // Test the connection
            let _ = client.get_connection()?;
            tracing::info!(redis_url = redis_url, "Connected to Redis");

            let db = RedisDb::new(client);
            let server = DaServer {
                listener,
                db,
                docker,
                private_key: self.private_key.clone(),
            };

            return Ok(DatabaseBackend::Redis(server));
        }

        // Fall back to Sled
        let root_dir =
            directories::ProjectDirs::from("com", "phylaxsystems", "assertion-da").unwrap();
        let db_path = if let Some(db_path) = &self.db_path {
            db_path
        } else {
            &root_dir.data_dir().join("db")
        };

        let db: Db<{ crate::LEAF_FANOUT }> = DbConfig::new()
            .path(db_path.clone())
            .cache_capacity_bytes(self.cache_size)
            .open()?;

        let db_size = db.size_on_disk()?;
        tracing::info!(
            database_size_mbs = db_size,
            database_path = db_path.to_str().unwrap(),
            "Opened Sled database"
        );
        metrics::gauge!("db_size_mb").set(u32::try_from(db_size)?);

        let server = DaServer {
            listener,
            db,
            docker,
            private_key: self.private_key.clone(),
        };

        Ok(DatabaseBackend::Sled(server))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::{
        primitives::FixedBytes,
        signers::local::PrivateKeySigner,
    };
    use assertion_da_client::DaClientError;
    use std::{
        net::SocketAddr,
        str::FromStr,
    };
    use testcontainers::ImageExt;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn test_server_random_port() -> anyhow::Result<()> {
        let addr = SocketAddr::from_str("127.0.0.1:0").unwrap();

        let config = Config {
            listen_addr: addr,
            db_path: None,
            cache_size: 1024 * 1024 * 1024, // 1GB
            private_key: hex::encode(PrivateKeySigner::random().to_bytes()),
            metrics_addr: "127.0.0.1:0".parse().unwrap(),
            log_level: tracing::level_filters::LevelFilter::current(),
            redis_url: None,
        };

        let backend = config.build().await?;

        let DatabaseBackend::Sled(server) = backend else {
            panic!("Expected Sled backend");
        };

        let listen_addr = server.listener.local_addr()?;
        // Check that we got a random port
        assert_ne!(listen_addr.port(), 0);

        let cancel_token = CancellationToken::new();

        let cancel_token_clone = cancel_token.clone();

        let task_handle = tokio::task::spawn(async move {
            #[allow(clippy::large_futures)]
            server.run(cancel_token_clone).await.unwrap();
        });

        std::thread::sleep(std::time::Duration::from_secs(1));
        let da_client =
            assertion_da_client::DaClient::new(&format!("http://{listen_addr}")).unwrap();

        if let Err(DaClientError::JsonRpcError { code, message }) =
            da_client.fetch_assertion(FixedBytes::default()).await
        {
            assert_eq!(code, 404);
            assert_eq!(message, "Assertion not found");
        }

        cancel_token.cancel();
        task_handle.await.unwrap();
        Ok(())
    }

    #[test]
    fn test_config_defaults() {
        // Test with required arguments and check defaults for the rest
        let config = Config::try_parse_from(vec![
            "program",
            "--private-key",
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        ])
        .unwrap();

        // Check default values
        assert_eq!(config.cache_size, 1000000);
        assert_eq!(config.listen_addr, "0.0.0.0:5001".parse().unwrap());
        assert_eq!(config.log_level, LevelFilter::INFO);
        assert_eq!(config.metrics_addr.to_string(), "0.0.0.0:9002");
        assert!(config.db_path.is_none());
    }

    #[test]
    fn test_config_args() {
        // Include the required private-key parameter
        let config = Config::try_parse_from(vec![
            "program",
            "--private-key",
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "--cache-size",
            "2000000",
            "--listen-addr",
            "127.0.0.1:8080",
            "--log-level",
            "debug",
            "--db-path",
            "/tmp/test-db",
        ])
        .unwrap();

        assert_eq!(config.cache_size, 2000000);
        assert_eq!(config.listen_addr, "127.0.0.1:8080".parse().unwrap());
        assert_eq!(config.log_level, LevelFilter::DEBUG);
        assert_eq!(config.db_path, Some(PathBuf::from("/tmp/test-db")));
    }

    #[tokio::test]
    async fn test_server_with_redis_backend() -> anyhow::Result<()> {
        use testcontainers::runners::AsyncRunner;
        use testcontainers_modules::redis::Redis;

        // Start Redis container
        let container = Redis::default().with_tag("7-alpine").start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(6379).await.unwrap();
        let redis_url = format!("redis://{host}:{port}");

        let addr = SocketAddr::from_str("127.0.0.1:0").unwrap();

        let config = Config {
            listen_addr: addr,
            db_path: None,
            cache_size: 1024 * 1024 * 1024,
            private_key: hex::encode(PrivateKeySigner::random().to_bytes()),
            metrics_addr: "127.0.0.1:0".parse().unwrap(),
            log_level: tracing::level_filters::LevelFilter::current(),
            redis_url: Some(redis_url),
        };

        let backend = config.build().await?;

        let DatabaseBackend::Redis(server) = backend else {
            panic!("Expected Redis backend");
        };

        let listen_addr = server.listener.local_addr()?;
        assert_ne!(listen_addr.port(), 0);

        let cancel_token = CancellationToken::new();
        let cancel_token_clone = cancel_token.clone();

        let task_handle = tokio::task::spawn(async move {
            #[allow(clippy::large_futures)]
            server.run(cancel_token_clone).await.unwrap();
        });

        // Wait for server to start
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let da_client =
            assertion_da_client::DaClient::new(&format!("http://{listen_addr}")).unwrap();

        // Test fetching non-existent assertion returns 404
        if let Err(DaClientError::JsonRpcError { code, message }) =
            da_client.fetch_assertion(FixedBytes::default()).await
        {
            assert_eq!(code, 404);
            assert_eq!(message, "Assertion not found");
        }

        // Submit an assertion
        let source_code = r"
            // SPDX-License-Identifier: MIT
            pragma solidity ^0.8.0;

            contract SimpleStorage {
                uint256 private value;

                function set(uint256 _value) public {
                    value = _value;
                }

                function get() public view returns (uint256) {
                    return value;
                }
            }
        ";

        let submission_response = da_client
            .submit_assertion(
                "SimpleStorage".to_string(),
                source_code.to_string(),
                "0.8.17".to_string(),
            )
            .await?;

        // Verify the submission response
        assert!(!submission_response.id.is_zero());
        assert!(!submission_response.prover_signature.is_empty());

        // Fetch the assertion back
        let fetch_response = da_client.fetch_assertion(submission_response.id).await?;

        // Verify the fetched assertion matches
        assert_eq!(
            fetch_response.prover_signature,
            submission_response.prover_signature
        );
        assert!(!fetch_response.bytecode.is_empty());

        // Clean up
        cancel_token.cancel();
        task_handle.await.unwrap();

        Ok(())
    }
}
