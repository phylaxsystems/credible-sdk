use crate::api::{
    process_request::match_method,
    types::DbRequestSender,
};

use core::convert::Infallible;
use std::sync::Arc;

use alloy::signers::local::PrivateKeySigner;
use bollard::Docker;
use http_body_util::Full;
use hyper::{
    body::Bytes,
    Error,
    Request,
};

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
