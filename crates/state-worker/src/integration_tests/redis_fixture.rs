//! Shared Redis fixture for integration tests
//!
//! This module provides a shared Redis container that is reused across all tests
//! to avoid the overhead of starting multiple containers in parallel.
//!
//! Each test still gets complete isolation through unique namespaces.

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
    pub url: String,
}

impl SharedRedisContainer {
    async fn new() -> Result<Self, String> {
        let container = Redis::default()
            .start()
            .await
            .map_err(|e| format!("Failed to start Redis container: {e}"))?;

        let host = container
            .get_host()
            .await
            .map_err(|e| format!("Failed to get Redis host: {e}"))?
            .to_string();

        let port = container
            .get_host_port_ipv4(6379)
            .await
            .map_err(|e| format!("Failed to get Redis port: {e}"))?;

        let url = format!("redis://{host}:{port}");

        // Wait for Redis to be ready
        Self::wait_for_ready(&url).await?;

        Ok(Self {
            _container: container,
            host,
            port,
            url,
        })
    }

    async fn wait_for_ready(url: &str) -> Result<(), String> {
        use tokio::time::{
            Duration,
            sleep,
        };

        for attempt in 1..=30 {
            match redis::Client::open(url).and_then(|client| client.get_connection()) {
                Ok(_) => {
                    tracing::debug!("Redis ready after {} attempts", attempt);
                    return Ok(());
                }
                Err(err) => {
                    if err.kind() != redis::ErrorKind::Io {
                        return Err(format!("Redis connection error: {err}"));
                    }
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
        Err(format!(
            "Redis at {url} was not ready after 30 attempts (3s)"
        ))
    }
}

/// Get or initialize the shared Redis container
///
/// This function returns a reference to a shared Redis container that persists
/// for the entire test suite. The container is initialized on first call.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_shared_redis_initialization() {
        let redis = get_shared_redis().await;
        assert!(!redis.url.is_empty());

        // Verify we can connect
        let client = redis::Client::open(redis.url.as_str()).unwrap();
        let mut conn = client.get_connection().unwrap();

        redis::cmd("PING")
            .query::<String>(&mut conn)
            .expect("Redis PING failed");
    }

    #[tokio::test]
    async fn test_shared_redis_reused() {
        let redis1 = get_shared_redis().await;
        let redis2 = get_shared_redis().await;

        // Should be the same instance
        assert_eq!(redis1.url, redis2.url);
        assert_eq!(redis1.port, redis2.port);
    }
}
