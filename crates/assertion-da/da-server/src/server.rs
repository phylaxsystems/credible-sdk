use anyhow::Result;
use bollard::Docker;
use sled::Db;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

pub struct DaServer {
    pub listener: TcpListener,
    pub db: Db,
    pub docker: Arc<Docker>,
    pub private_key: String,
}

// Type alias for boxed future
pub type BoxedFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'static>>;

impl DaServer {
    /// Start the server
    /// Will run until either the database or the API server stops running.
    pub fn start(self, cancel_token: CancellationToken) -> (BoxedFuture, BoxedFuture) {
        // Set up the channel for communicating with the database
        let (db_tx, db_rx) = tokio::sync::mpsc::unbounded_channel::<crate::api::types::DbRequest>();

        // Start the database management task
        let db_handle = crate::api::db::listen_for_db(db_rx, self.db.clone(), cancel_token.clone());
        tracing::debug!("Started Database task");

        // Start the API server
        let api_handle = crate::api::serve(
            self.listener,
            db_tx,
            self.docker.clone(),
            self.private_key.clone(),
            cancel_token,
        );

        tracing::info!("Started API server");

        (Box::pin(api_handle), Box::pin(db_handle))
    }
    /// Run the server until the cancellation token is cancelled.
    pub async fn run(self, cancel_token: CancellationToken) -> Result<()> {
        let (mut api_handle, mut db_handle) = self.start(cancel_token.clone());
        loop {
            tokio::select! {
                res = &mut api_handle => {
                    match res {
                        Ok(()) => {
                            tracing::info!("Api stopped.");
                            db_handle.await?;
                            tracing::info!("Database stopped.");
                            break;
                        }
                        Err(e) => {
                            metrics::counter!("api_server_errors_counts").increment(1);
                            tracing::error!("API server encountered an error, restarting: {:?}", e);
                        }
                    }
                }
                res = &mut db_handle => {
                    match res {
                        Ok(()) => {
                            tracing::info!("Database stopped.");
                            api_handle.await?;
                            tracing::info!("Api stopped.");
                            break;
                        }
                        Err(e) => {
                            metrics::counter!("database_errors_count").increment(1);
                            tracing::error!("Database encountered an error, restarting: {:?}", e);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bollard::Docker;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_server_cancellation() {
        // Set up test components
        let temp_dir = TempDir::new().unwrap();
        let db = sled::Config::new().path(&temp_dir).open().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let docker = Arc::new(Docker::connect_with_local_defaults().unwrap());
        let private_key =
            "0000000000000000000000000000000000000000000000000000000000000001".to_string();

        // Create server instance
        let server = DaServer {
            db,
            listener,
            docker,
            private_key,
        };

        // Create cancellation token and clone for later cancellation
        let cancel_token = CancellationToken::new();
        let cancel_token_clone = cancel_token.clone();

        // Run server in background task
        let server_handle = tokio::spawn(async move {
            server.run(cancel_token).await.unwrap();
        });

        // Wait briefly to ensure server is running
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Cancel the server
        cancel_token_clone.cancel();

        // Server should shutdown gracefully
        server_handle.await.unwrap();
    }
}
