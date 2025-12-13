//! Example sidecar gRPC server that streams invalidation events to the proxy.
//!
//! This demonstrates how to implement the server side of the RpcProxyHeuristics
//! service to send invalidation notifications when transactions fail assertions.
//!
//! Run with:
//! ```bash
//! cargo run --example sidecar_server
//! ```
//!
//! Then in another terminal, start the proxy:
//! ```bash
//! cargo run --bin rpc-proxy -- \
//!   --listen 0.0.0.0:9547 \
//!   --upstream http://127.0.0.1:8545 \
//!   --sidecar-endpoint http://127.0.0.1:50051
//! ```

use alloy_primitives::{address, Address, B256};
use futures::StreamExt;
use prost_types::Timestamp;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tonic::{transport::Server, Request, Response, Status};

// Include the generated protobuf code
pub mod pb {
    tonic::include_proto!("rpcproxy.v1");
}

use pb::{
    rpc_proxy_heuristics_server::{RpcProxyHeuristics, RpcProxyHeuristicsServer},
    Assertion, Empty, Fingerprint, Invalidation, ShouldForwardRequest, ShouldForwardResponse,
};

/// Example sidecar server that broadcasts invalidation events
pub struct SidecarService {
    /// Broadcast channel for invalidation events
    /// When a transaction fails an assertion, send an invalidation here
    invalidation_tx: broadcast::Sender<Result<Invalidation, Status>>,
}

impl SidecarService {
    pub fn new() -> Self {
        // Create broadcast channel with capacity for 100 pending invalidations
        let (tx, _) = broadcast::channel(100);
        Self {
            invalidation_tx: tx,
        }
    }

    /// Send an invalidation event for a failed transaction
    ///
    /// Call this when your assertion validation determines that a transaction
    /// should be denied. The proxy will cache this fingerprint and reject
    /// future matching transactions.
    pub fn send_invalidation(
        &self,
        fingerprint_hash: B256,
        target: Address,
        selector: [u8; 4],
        arg_hash: [u8; 16],
        value_bucket: u64,
        gas_bucket: u32,
        assertion_id: B256,
        assertion_version: u64,
        adopter: String,
        l2_block_number: u64,
    ) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);

        let invalidation = Invalidation {
            fingerprint: Some(Fingerprint {
                hash: fingerprint_hash.as_slice().to_vec(),
                target: target.as_slice().to_vec(),
                selector: selector.to_vec(),
                arg_hash16: arg_hash.to_vec(),
                value_bucket,
                gas_bucket,
            }),
            assertion: Some(Assertion {
                assertion_id: assertion_id.as_slice().to_vec(),
                assertion_version,
            }),
            adopter,
            l2_block_number,
            observed_at: Some(Timestamp {
                seconds: now.as_secs() as i64,
                nanos: now.subsec_nanos() as i32,
            }),
        };

        // Broadcast to all connected proxy clients
        // Ignore error if no receivers (proxy not connected yet)
        let _ = self.invalidation_tx.send(Ok(invalidation));
    }
}

#[tonic::async_trait]
impl RpcProxyHeuristics for SidecarService {
    type StreamInvalidationsStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<Invalidation, Status>> + Send>>;

    async fn stream_invalidations(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::StreamInvalidationsStream>, Status> {
        // Subscribe to the broadcast channel
        let rx = self.invalidation_tx.subscribe();

        // Convert BroadcastStream to the required type
        let stream = BroadcastStream::new(rx).filter_map(|result| async move {
            match result {
                Ok(invalidation) => Some(invalidation),
                Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(skipped)) => {
                    eprintln!("⚠️  Proxy lagged behind, skipped {} messages", skipped);
                    None
                }
            }
        });

        println!("Proxy connected to invalidation stream");

        Ok(Response::new(Box::pin(stream)))
    }

    async fn should_forward(
        &self,
        request: Request<ShouldForwardRequest>,
    ) -> Result<Response<ShouldForwardResponse>, Status> {
        let _fingerprint = request
            .into_inner()
            .fingerprint
            .ok_or_else(|| Status::invalid_argument("missing fingerprint"))?;

        // In a real implementation, you would:
        // 1. Check your local cache of denied fingerprints
        // 2. Optionally query assertion state
        // 3. Return DENY if known bad, UNKNOWN otherwise

        // For this example, always return UNKNOWN (let proxy decide)
        Ok(Response::new(ShouldForwardResponse {
            verdict: pb::should_forward_response::Verdict::Unknown as i32,
            assertion: None,
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse()?;
    let service = SidecarService::new();

    println!("Starting example sidecar server on {}", addr);
    println!("Proxy can connect with: --sidecar-endpoint http://127.0.0.1:50051");
    println!();

    // Clone service for simulation task
    let service_for_sim = service.invalidation_tx.clone();

    // Spawn a task to simulate sending invalidations
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        println!("Simulating invalidation events...");

        loop {
            // Simulate a failed transaction every 10 seconds
            tokio::time::sleep(Duration::from_secs(10)).await;

            // Example: ERC20 transfer to address(0x1111...) with value 1000
            let fingerprint_hash = B256::random();
            let target = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"); // USDC
            let selector = [0xa9, 0x05, 0x9c, 0xbb]; // transfer(address,uint256)
            let arg_hash = [0x11; 16]; // simplified arg hash
            let value_bucket = 0; // no ETH value
            let gas_bucket = 100_000;
            let assertion_id = B256::random();
            let assertion_version = 1;
            let adopter = "example-adopter".to_string();
            let l2_block_number = 12345;

            println!("📤 Sending invalidation for fingerprint {:#x}", fingerprint_hash);
            println!("   Assertion: {:#x} v{}", assertion_id, assertion_version);
            println!("   Target: {} selector: 0x{}", target, hex::encode(selector));
            println!();

            let invalidation = Invalidation {
                fingerprint: Some(Fingerprint {
                    hash: fingerprint_hash.as_slice().to_vec(),
                    target: target.as_slice().to_vec(),
                    selector: selector.to_vec(),
                    arg_hash16: arg_hash.to_vec(),
                    value_bucket,
                    gas_bucket,
                }),
                assertion: Some(Assertion {
                    assertion_id: assertion_id.as_slice().to_vec(),
                    assertion_version,
                }),
                adopter,
                l2_block_number,
                observed_at: Some(Timestamp {
                    seconds: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64,
                    nanos: 0,
                }),
            };

            // Broadcast invalidation to connected proxies
            let _ = service_for_sim.send(Ok(invalidation));
        }
    });

    // Start gRPC server
    Server::builder()
        .add_service(RpcProxyHeuristicsServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
