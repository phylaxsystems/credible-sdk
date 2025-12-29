//! Shared Redis fixture for state-store tests
//!
//! This module provides a shared Redis container that is reused across all tests
//! to avoid the overhead of starting multiple containers in parallel.

use std::sync::Arc;
use testcontainers::{
    ContainerAsync,
    runners::AsyncRunner,
};
use testcontainers_modules::redis::Redis;
use tokio::sync::OnceCell;

/// Shared Redis container instance
static REDIS_CONTAINER: OnceCell<Arc<SharedRedisContainer>> = OnceCell::const_new();

pub struct SharedRedisContainer {
    _container: ContainerAsync<Redis>,
    #[allow(dead_code)]
    pub host: String,
    #[allow(dead_code)]
    pub port: u16,
    pub _url: String,
}

impl SharedRedisContainer {
    async fn new() -> anyhow::Result<Self> {
        let container = Redis::default()
            .start()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to start Redis container: {e}"))?;

        let host = container.get_host().await?.to_string();

        let port = container.get_host_port_ipv4(6379).await?;

        let url = format!("redis://{host}:{port}");

        // Wait for Redis to be ready
        Self::wait_for_ready(&url).await?;

        Ok(Self {
            _container: container,
            host,
            port,
            _url: url,
        })
    }

    async fn wait_for_ready(url: &str) -> anyhow::Result<()> {
        use tokio::time::{
            Duration,
            sleep,
        };

        for _ in 0..30 {
            match redis::Client::open(url).and_then(|client| client.get_connection()) {
                Ok(_) => return Ok(()),
                Err(err) => {
                    if err.kind() != redis::ErrorKind::Io {
                        return Err(anyhow::anyhow!("Redis connection error: {err}"));
                    }
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
        Err(anyhow::anyhow!(
            "Redis at {url} was not ready after 30 attempts (3s)"
        ))
    }
}

/// Get or initialize the shared Redis container
pub async fn get_shared_redis() -> Arc<SharedRedisContainer> {
    REDIS_CONTAINER
        .get_or_init(|| {
            async {
                Arc::new(
                    SharedRedisContainer::new()
                        .await
                        .expect("Failed to initialize shared Redis container"),
                )
            }
        })
        .await
        .clone()
}
