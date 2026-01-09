use httpmock::MockServer;
use std::net::TcpListener;

pub fn try_start_mock_server() -> MockServer {
    TcpListener::bind("127.0.0.1:0")
        .map_err(|err| format!("Failed to bind localhost for httpmock: {err}"))
        .and_then(|listener| {
            drop(listener);
            std::panic::catch_unwind(MockServer::start).map_err(|err| {
                if let Some(msg) = err.downcast_ref::<&str>() {
                    (*msg).to_string()
                } else if let Some(msg) = err.downcast_ref::<String>() {
                    msg.clone()
                } else {
                    "MockServer::start() panicked".to_string()
                }
            })
        })
        .expect("Failed to start httpmock server")
}
