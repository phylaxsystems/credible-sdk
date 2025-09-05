#[cfg(test)]
mod tests {
    use super::{
        proto::{
            sidecar_service_client::SidecarServiceClient,
            *,
        },
        *,
    };
    use crate::{
        engine::queue::TransactionQueueSender,
        transactions_state::TransactionsState,
        transport::Transport,
    };
    use crossbeam::channel::unbounded;
    use std::sync::Arc;
    use tonic::Request;

    async fn setup_grpc_transport() -> (GrpcTransport, tokio::task::JoinHandle<()>) {
        let (tx_sender, _tx_receiver) = unbounded();
        let state_results = Arc::new(TransactionsState::new());
        
        let config = GrpcTransportConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(), // Use random port
            ..Default::default()
        };

        let transport = GrpcTransport::new(config, tx_sender, state_results).unwrap();
        let bind_addr = transport.bind_addr;

        // Start the transport in background
        let handle = tokio::spawn(async move {
            transport.run().await.unwrap();
        });

        // Wait a bit for server to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        (transport, handle)
    }

    #[tokio::test]
    async fn test_grpc_health_check() {
        let (_transport, _handle) = setup_grpc_transport().await;
        
        // Connect to the gRPC server
        let mut client = SidecarServiceClient::connect("http://127.0.0.1:9090")
            .await
            .expect("Failed to connect to gRPC server");

        // Test health endpoint
        let request = Request::new(HealthRequest {});
        let response = client.health(request).await.unwrap();
        
        let health_response = response.into_inner();
        assert_eq!(health_response.status, "healthy");
        assert!(health_response.message.contains("running"));
    }

    #[tokio::test]
    async fn test_grpc_send_block_env() {
        let (_transport, _handle) = setup_grpc_transport().await;
        
        let mut client = SidecarServiceClient::connect("http://127.0.0.1:9090")
            .await
            .expect("Failed to connect to gRPC server");

        // Test sending block environment
        let request = Request::new(SendBlockEnvRequest {
            number: 1,
            coinbase: "0x0000000000000000000000000000000000000000".to_string(),
            timestamp: Some(1700000000),
            difficulty: Some(1000),
            prevrandao: None,
            basefee: 1000000000,
            gas_limit: Some(30000000),
        });

        let response = client.send_block_env(request).await.unwrap();
        let block_env_response = response.into_inner();
        
        assert_eq!(block_env_response.status, "accepted");
        assert_eq!(block_env_response.block_number, 1);
    }

    #[tokio::test]
    async fn test_grpc_send_transactions() {
        let (_transport, _handle) = setup_grpc_transport().await;
        
        let mut client = SidecarServiceClient::connect("http://127.0.0.1:9090")
            .await
            .expect("Failed to connect to gRPC server");

        // First send block environment
        let block_env_request = Request::new(SendBlockEnvRequest {
            number: 1,
            coinbase: "0x0000000000000000000000000000000000000000".to_string(),
            timestamp: Some(1700000000),
            difficulty: Some(1000),
            prevrandao: None,
            basefee: 1000000000,
            gas_limit: Some(30000000),
        });
        
        client.send_block_env(block_env_request).await.unwrap();

        // Now send transactions
        let tx_request = Request::new(SendTransactionsRequest {
            transactions: vec![
                Transaction {
                    hash: "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string(),
                    tx_env: Some(TransactionEnv {
                        caller: "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb6".to_string(),
                        gas_limit: 21000,
                        gas_price: "20000000000".to_string(),
                        transact_to: Some("0x0000000000000000000000000000000000000001".to_string()),
                        value: "1000000000000000000".to_string(),
                        data: "0x".to_string(),
                        nonce: 1,
                        chain_id: 1,
                        access_list: vec![],
                    }),
                }
            ],
        });

        let response = client.send_transactions(tx_request).await.unwrap();
        let tx_response = response.into_inner();
        
        assert_eq!(tx_response.status, "accepted");
        assert_eq!(tx_response.transaction_count, 1);
        assert!(tx_response.failed.is_empty());
    }

    #[tokio::test]
    async fn test_grpc_revert_transaction() {
        let (_transport, _handle) = setup_grpc_transport().await;
        
        let mut client = SidecarServiceClient::connect("http://127.0.0.1:9090")
            .await
            .expect("Failed to connect to gRPC server");

        // Test transaction reversion
        let request = Request::new(RevertTransactionRequest {
            tx_hash: "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string(),
            reason: "Line limit exceeded".to_string(),
        });

        let response = client.revert_transaction(request).await.unwrap();
        let revert_response = response.into_inner();
        
        assert_eq!(revert_response.status, "reverted");
        assert_eq!(revert_response.tx_hash, "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        assert!(revert_response.message.contains("reverted"));
    }

    #[tokio::test]
    async fn test_grpc_get_transactions() {
        let (_transport, _handle) = setup_grpc_transport().await;
        
        let mut client = SidecarServiceClient::connect("http://127.0.0.1:9090")
            .await
            .expect("Failed to connect to gRPC server");

        // Test querying transactions
        let request = Request::new(GetTransactionsRequest {
            hashes: vec![
                "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string(),
                "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890".to_string(),
            ],
        });

        let response = client.get_transactions(request).await.unwrap();
        let get_response = response.into_inner();
        
        assert_eq!(get_response.results.len(), 2);
        
        for result in get_response.results {
            assert!(!result.hash.is_empty());
            assert!(!result.status.is_empty());
        }
    }

    #[tokio::test]
    async fn test_grpc_streaming_transactions() {
        let (_transport, _handle) = setup_grpc_transport().await;
        
        let mut client = SidecarServiceClient::connect("http://127.0.0.1:9090")
            .await
            .expect("Failed to connect to gRPC server");

        // First send block environment
        let block_env_request = Request::new(SendBlockEnvRequest {
            number: 1,
            coinbase: "0x0000000000000000000000000000000000000000".to_string(),
            timestamp: Some(1700000000),
            difficulty: Some(1000),
            prevrandao: None,
            basefee: 1000000000,
            gas_limit: Some(30000000),
        });
        
        client.send_block_env(block_env_request).await.unwrap();

        // Create stream of transaction requests
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        
        // Send multiple batches
        for i in 0..3 {
            let batch = SendTransactionsRequest {
                transactions: vec![
                    Transaction {
                        hash: format!("0x{:064x}", i),
                        tx_env: Some(TransactionEnv {
                            caller: "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb6".to_string(),
                            gas_limit: 21000,
                            gas_price: "20000000000".to_string(),
                            transact_to: Some("0x0000000000000000000000000000000000000001".to_string()),
                            value: "0".to_string(),
                            data: "0x".to_string(),
                            nonce: i + 1,
                            chain_id: 1,
                            access_list: vec![],
                        }),
                    }
                ],
            };
            
            tx.send(batch).await.unwrap();
        }
        
        drop(tx); // Close the stream
        
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let request = Request::new(stream);
        
        let mut response_stream = client.send_transactions_stream(request).await.unwrap().into_inner();
        
        let mut response_count = 0;
        while let Some(response) = response_stream.message().await.unwrap() {
            assert_eq!(response.status, "accepted");
            assert_eq!(response.transaction_count, 1);
            response_count += 1;
        }
        
        assert_eq!(response_count, 3);
    }
}
