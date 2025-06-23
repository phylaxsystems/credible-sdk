use std::io;
use std::path::PathBuf;
use std::process::{
    Child,
    Command,
    ExitStatus,
    Stdio,
};
use std::thread;
use std::time::{
    Duration,
    Instant,
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DeployDappError {
    #[error("Command execution failed. Exit Status: {0}, Error: {1}")]
    CommandError(ExitStatus, String),
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),
    #[error("Dapp did not start listening on port in time")]
    PortTimeout,
}

/// Start the dapp by running the start script in the credible-layer-dapp submodule
///
/// # Arguments
///
/// * `project_root` - Root directory of the credible-layer-dapp repo (submodule)
/// * `rpc_url` - The RPC URL to set as NEXT_PUBLIC_SANDBOX_RPC_URL
/// * `da_url` - The DA URL to set as ASSERTION_DA_URL
///
/// # Returns
///
/// * `Result<Child, DeployDappError>` - The child process or error
pub fn start_dapp(
    project_root: &PathBuf,
    rpc_url: &str,
    da_url: &str,
    port: u16,
) -> Result<Child, DeployDappError> {
    let output = Command::new("pnpm")
        .current_dir(project_root)
        .arg("install")
        .output()?;
    if !output.status.success() {
        return Err(DeployDappError::CommandError(
            output.status,
            format!(
                "Installing dapp failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
        ));
    }
    let output = Command::new("pnpm")
        .current_dir(project_root)
        .arg("db:local")
        .output()?;
    if !output.status.success() {
        return Err(DeployDappError::CommandError(
            output.status,
            format!(
                "Starting database failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
        ));
    };

    let project_root = project_root.clone();
    let rpc_url = rpc_url.to_string();
    let da_url = da_url.to_string();
    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let child_result = Command::new("next")
            .current_dir(project_root)
            .arg("dev")
            .arg("--port")
            .arg(port.to_string())
            .env("NEXT_PUBLIC_SANDBOX_RPC_URL", &rpc_url)
            .env("ASSERTION_DA_URL", &da_url)
            .env("IRON_SESSION_SECRET", "Zu2mQkQLZii6LBzqxkK4bipHMuVVt8kj")
            .env("JWT_SECRET", "super-secret-jwt-token-with-at-least-32-characters-long")
            .stdout(Stdio::inherit())
            .stderr(Stdio::piped())
            .spawn();
        // Send the result of spawning the process
        let _ = tx.send(child_result);
    });

    // Wait for the result of spawning the process
    let child_result = rx.recv().unwrap();
    let mut child = match child_result {
        Ok(child) => child,
        Err(e) => return Err(DeployDappError::IoError(e)),
    };

    // Block until the port is available or timeout
    let start = Instant::now();
    let timeout = Duration::from_secs(60);
    let addr = format!("127.0.0.1:{port}");
    while start.elapsed() < timeout {
        if let Some(status) = child.try_wait().unwrap() {
            // Process exited before port was available
            let mut stderr = String::new();
            if let Some(mut s) = child.stderr.take() {
                use std::io::Read;
                let _ = s.read_to_string(&mut stderr);
            }
            return Err(DeployDappError::CommandError(status, stderr));
        }
        let stream_res = std::net::TcpStream::connect(&addr);
        if stream_res.is_ok() {
            let _ = stream_res.unwrap().shutdown(std::net::Shutdown::Both);
            println!("Dapp started on port {}", port);
            return Ok(child);
        }
        thread::sleep(Duration::from_millis(200));
    }
    // Timeout: kill the process if still running
    let _ = child.kill();
    if let Some(status) = child.try_wait().unwrap() {
        let mut stderr = String::new();
        if let Some(mut s) = child.stderr.take() {
            use std::io::Read;
            let _ = s.read_to_string(&mut stderr);
        }
        return Err(DeployDappError::CommandError(status, stderr));
    }
    Err(DeployDappError::PortTimeout)
}

/// Deploy the dapp by running the setup script and then the start script in the credible-layer-dapp submodule
///
/// # Returns
///
/// * `Result<(u16, Child), DeployDappError>` - Success, including the port the dapp is running on and the child process, or error
pub fn deploy_dapp(
    project_root: &PathBuf,
    rpc_url: &str,
    da_url: &str,
) -> Result<(u16, Child), DeployDappError> {
    let unused_port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let child = start_dapp(project_root, rpc_url, da_url, unused_port)?;
    Ok((unused_port, child))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_deploy_dapp() {
        let project_root = PathBuf::from("lib/credible-layer-dapp");

        let rpc_url = "http://localhost:8545";
        let da_url = "http://localhost:8080";

        let result = deploy_dapp(&project_root, rpc_url, da_url);
        // This will fail if pnpm or mise is not installed, or if the submodule is missing
        assert!(
            result.is_ok(),
            "Failed to deploy dapp: {:#?}",
            result.err().unwrap()
        );
        let (port, mut child) = result.unwrap();
        let url = format!("http://localhost:{port}/api/health",);
        let response = reqwest::get(url).await.unwrap();
        assert!(response.status().is_success());
        let _ = child.kill();
    }
}
