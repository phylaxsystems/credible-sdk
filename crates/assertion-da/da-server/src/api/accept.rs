use crate::api::{
    process_request::match_method,
    types::{
        DbOperation,
        DbRequest,
        DbRequestSender,
    },
};

use core::convert::Infallible;
use std::{
    sync::Arc,
    time::Duration,
};

use alloy::signers::local::PrivateKeySigner;
use bollard::Docker;
use http_body_util::Full;
use hyper::{
    Error,
    Method,
    Request,
    StatusCode,
    body::Bytes,
};
use tokio::sync::oneshot;

macro_rules! rpc_response {
    (
        $status:expr,
        $body:expr
    ) => {
        Ok(hyper::Response::builder()
            .status($status)
            .body($body)
            .unwrap())
    };
}

/// Accepts an incoming HTTP request, which it responds with
/// the appropriate api call.
#[tracing::instrument(level = "info", skip_all, target = "api::accept_request")]
pub async fn accept_request<B>(
    tx: Request<B>,
    db: DbRequestSender,
    signer: &PrivateKeySigner,
    docker: Arc<Docker>,
    client_addr: std::net::SocketAddr,
) -> Result<hyper::Response<Full<Bytes>>, Infallible>
where
    B: hyper::body::Body<Error = Error>,
{
    let path = tx.uri().path();
    let method = tx.method().clone();

    if path == "/health" && method == Method::GET {
        return rpc_response!(StatusCode::OK, Full::new(Bytes::from("ok")));
    }

    if path == "/ready" && method == Method::GET {
        let status = if check_database_readiness(&db).await {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        };

        let body = if status == StatusCode::OK {
            "ready"
        } else {
            "not ready"
        };

        return rpc_response!(status, Full::new(Bytes::from(body)));
    }

    tracing::debug!(target = "api::accept_request", "Incoming request");
    // Respond accordingly
    let resp = match match_method(tx, &db, signer, docker, client_addr).await {
        Ok(rax) => rax,
        Err(e) => {
            let e = e.to_string();
            return rpc_response!(400, Full::new(Bytes::from(e)));
        }
    };
    rpc_response!(200, Full::new(Bytes::from(resp)))
}

async fn check_database_readiness(db: &DbRequestSender) -> bool {
    let (response_tx, response_rx) = oneshot::channel();
    let request = DbRequest {
        request: DbOperation::Get(Vec::new()),
        response: response_tx,
    };

    if db.send(request).is_err() {
        return false;
    }

    match tokio::time::timeout(Duration::from_secs(1), response_rx).await {
        Ok(Ok(_)) => true,
        Ok(Err(_)) | Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn readiness_is_false_when_db_channel_is_closed() {
        let (tx, rx) = mpsc::unbounded_channel();
        drop(rx);

        assert!(!check_database_readiness(&tx).await);
    }

    #[tokio::test]
    async fn readiness_is_true_when_db_responds() {
        let (tx, mut rx) = mpsc::unbounded_channel::<DbRequest>();

        tokio::spawn(async move {
            if let Some(req) = rx.recv().await {
                let _ = req.response.send(None);
            }
        });

        assert!(check_database_readiness(&tx).await);
    }
}

/// Macros for accepting requests
#[macro_export]
macro_rules! accept {
    (
        $io:expr,
        $db:expr,
        $signer:expr,
        $docker:expr,
        $client_addr:expr
    ) => {
        let db_c = $db.clone();
        let signer_clone = $signer.clone();
        let docker_clone = $docker.clone();
        let client_addr = $client_addr;
        // Bind the incoming connection to our service
        if let Err(err) = hyper::server::conn::http1::Builder::new()
            // `service_fn` converts our function in a `Service`
            .serve_connection(
                $io,
                hyper::service::service_fn(move |req| {
                    let db_c = db_c.clone();
                    let signer_clone = signer_clone.clone();
                    let docker_clone = docker_clone.clone();
                    async move {
                        $crate::api::accept::accept_request(
                            req,
                            db_c,
                            &signer_clone,
                            docker_clone,
                            client_addr,
                        )
                        .await
                    }
                }),
            )
            .with_upgrades()
            .await
        {
            tracing::error!(?err, "Error serving connection");
        }
    };
}
