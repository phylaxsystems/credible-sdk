#![allow(dead_code)]
use crate::{
    connect_provider,
    genesis::GenesisState,
    redis::RedisStateWriter,
    worker::StateWorker,
};
use bytes::Bytes;
use int_test_utils::node_protocol_mock_server::DualProtocolMockServer;
use mini_redis::{
    Connection,
    Frame,
};
use std::{
    collections::HashMap,
    sync::Arc,
};
use tokio::{
    net::{
        TcpListener,
        TcpStream,
    },
    sync::Mutex,
};
use tracing::{
    debug,
    error,
};

pub(in crate::integration_tests) struct LocalInstance {
    pub http_server_mock: DualProtocolMockServer,
    pub handle_redis: tokio::task::JoinHandle<()>,
    pub handle_worder: tokio::task::JoinHandle<()>,
    pub redis_url: String,
}

impl LocalInstance {
    const NAMESPACE: &'static str = "state_worker_test";

    pub(in crate::integration_tests) async fn new() -> Result<LocalInstance, String> {
        Self::new_with_setup_and_genesis(|_| {}, None).await
    }

    pub(in crate::integration_tests) async fn new_with_setup<F>(
        setup: F,
    ) -> Result<LocalInstance, String>
    where
        F: FnOnce(&DualProtocolMockServer),
    {
        Self::new_with_setup_and_genesis(setup, None).await
    }

    pub(in crate::integration_tests) async fn new_with_setup_and_genesis<F>(
        setup: F,
        genesis_state: Option<GenesisState>,
    ) -> Result<LocalInstance, String>
    where
        F: FnOnce(&DualProtocolMockServer),
    {
        // Create the mock transport
        let http_server_mock = DualProtocolMockServer::new()
            .await
            .expect("Failed to create the http server mock");

        setup(&http_server_mock);

        let provider = connect_provider(&http_server_mock.ws_url())
            .await
            .expect("Failed to connect to provider");

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let redis_url = format!("redis://127.0.0.1:{}", addr.port());

        let handle_redis = spawn_embedded_redis(listener);

        let redis = RedisStateWriter::new(&redis_url, Self::NAMESPACE.to_string())
            .expect("failed to initialize redis client");

        let mut worker = StateWorker::new(provider, redis, genesis_state);
        let handle_worder = tokio::spawn(async move {
            if let Err(e) = worker.run(Some(0)).await {
                error!("worker server error: {}", e);
            }
        });

        Ok(LocalInstance {
            http_server_mock,
            handle_redis,
            handle_worder,
            redis_url,
        })
    }
}

#[derive(Default)]
struct EmbeddedRedisState {
    string_entries: HashMap<String, String>,
    hash_entries: HashMap<String, HashMap<String, String>>,
}

fn spawn_embedded_redis(listener: TcpListener) -> tokio::task::JoinHandle<()> {
    let state = Arc::new(Mutex::new(EmbeddedRedisState::default()));
    tokio::spawn(async move {
        if let Err(err) = run_embedded_redis(listener, state).await {
            error!("embedded redis server error: {}", err);
        }
    })
}

async fn run_embedded_redis(
    listener: TcpListener,
    state: Arc<Mutex<EmbeddedRedisState>>,
) -> mini_redis::Result<()> {
    loop {
        let (socket, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(err) => return Err(Box::new(err)),
        };

        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_redis_client(socket, state).await {
                error!("embedded redis client error: {}", err);
            }
        });
    }
}

async fn handle_redis_client(
    socket: TcpStream,
    state: Arc<Mutex<EmbeddedRedisState>>,
) -> mini_redis::Result<()> {
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await? {
        let response = match frame {
            Frame::Array(parts) => process_command(parts, &state).await?,
            _ => Frame::Error("ERR invalid redis command".into()),
        };

        connection
            .write_frame(&response)
            .await
            .map_err(|err| Box::new(err))?;
    }

    Ok(())
}

async fn process_command(
    parts: Vec<Frame>,
    state: &Arc<Mutex<EmbeddedRedisState>>,
) -> mini_redis::Result<Frame> {
    if parts.is_empty() {
        return Err("empty redis command".into());
    }

    let command = parse_bulk_string(&parts[0])?.to_lowercase();
    match command.as_str() {
        "set" => {
            if parts.len() < 3 {
                return Err("ERR wrong number of arguments for 'set' command".into());
            }
            let key = parse_bulk_string(&parts[1])?;
            let value = parse_bulk_string(&parts[2])?;
            debug!(command = "set", key = %key, value = %value);
            let mut guard = state.lock().await;
            guard.string_entries.insert(key, value);
            Ok(Frame::Simple("OK".into()))
        }
        "get" => {
            if parts.len() < 2 {
                return Err("ERR wrong number of arguments for 'get' command".into());
            }
            let key = parse_bulk_string(&parts[1])?;
            let guard = state.lock().await;
            if let Some(value) = guard.string_entries.get(&key) {
                debug!(command = "get", key = %key, value = %value);
                Ok(Frame::Bulk(Bytes::from(value.clone())))
            } else {
                debug!(command = "get", key = %key, result = "nil");
                Ok(Frame::Null)
            }
        }
        "hset" => {
            let key = parse_bulk_string(&parts[1])?;
            if parts.len() < 4 || parts.len() % 2 != 0 {
                return Err("ERR wrong number of arguments for 'hset' command".into());
            }
            debug!(
                command = "hset",
                key = %key,
                field_count = (parts.len().saturating_sub(2)) / 2
            );
            let mut guard = state.lock().await;
            let entry = guard.hash_entries.entry(key).or_default();
            let mut inserted_count = 0_u64;
            let mut idx = 2;
            while idx + 1 < parts.len() {
                let field = parse_bulk_string(&parts[idx])?;
                let value = parse_bulk_string(&parts[idx + 1])?;
                debug!(command = "hset", field = %field, value = %value);
                if entry.insert(field, value).is_none() {
                    inserted_count += 1;
                }
                idx += 2;
            }
            Ok(Frame::Integer(inserted_count))
        }
        "hget" => {
            if parts.len() < 3 {
                return Err("ERR wrong number of arguments for 'hget' command".into());
            }
            let key = parse_bulk_string(&parts[1])?;
            let field = parse_bulk_string(&parts[2])?;
            let guard = state.lock().await;
            let value = guard
                .hash_entries
                .get(&key)
                .and_then(|entry| entry.get(&field))
                .cloned();
            match value {
                Some(value) => {
                    debug!(command = "hget", key = %key, field = %field, value = %value);
                    Ok(Frame::Bulk(Bytes::from(value)))
                }
                None => {
                    debug!(command = "hget", key = %key, field = %field, result = "nil");
                    Ok(Frame::Null)
                }
            }
        }
        "keys" => {
            if parts.len() < 2 {
                return Err("ERR wrong number of arguments for 'keys' command".into());
            }
            let pattern = parse_bulk_string(&parts[1])?;
            let guard = state.lock().await;
            let mut matches = Vec::new();
            for key in guard.string_entries.keys() {
                if pattern_matches(key, &pattern) {
                    matches.push(Frame::Bulk(Bytes::from(key.clone())));
                }
            }
            for key in guard.hash_entries.keys() {
                if pattern_matches(key, &pattern) {
                    matches.push(Frame::Bulk(Bytes::from(key.clone())));
                }
            }
            debug!(
                command = "keys",
                pattern = %pattern,
                match_count = matches.len()
            );
            Ok(Frame::Array(matches))
        }
        "ping" => Ok(Frame::Simple("PONG".into())),
        "command" => Ok(Frame::Array(vec![])),
        _ => Ok(Frame::Error(format!("ERR unknown command '{}'", command))),
    }
}

fn parse_bulk_string(frame: &Frame) -> mini_redis::Result<String> {
    match frame {
        Frame::Bulk(bytes) => Ok(
            String::from_utf8(bytes.to_vec())
                .map_err(|err| -> mini_redis::Error { Box::new(err) })?,
        ),
        Frame::Simple(value) => Ok(value.clone()),
        _ => Err("expected bulk string".into()),
    }
}

fn pattern_matches(key: &str, pattern: &str) -> bool {
    if pattern == "*" {
        true
    } else if let Some(prefix) = pattern.strip_suffix('*') {
        key.starts_with(prefix)
    } else {
        key == pattern
    }
}
