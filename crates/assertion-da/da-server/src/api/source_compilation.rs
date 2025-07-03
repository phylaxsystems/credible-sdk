use anyhow::Result;
use bollard::Docker;
use bollard::container::{
    Config, CreateContainerOptions, LogsOptions, RemoveContainerOptions, StartContainerOptions,
    WaitContainerOptions,
};
use bollard::image::ListImagesOptions;
use futures::TryStreamExt;
use futures_util::stream::StreamExt;
use metrics;
use regex::Regex;
use serde_json::Value;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::time::{Duration, sleep};
use tracing::warn;
use tracing::{Instrument, debug, instrument};
use uuid::Uuid;

/// Maximum number of attempts to ensure image availability
const MAX_ATTEMPTS: u32 = 3;
/// Initial delay of the exponential backoff for image availability check
const INITIAL_DELAY: Duration = Duration::from_secs(1);

/// Command line arguments for the Solidity compiler
#[derive(Debug, Clone)]
pub struct SolcArgs {
    pub output_type: String,
    pub metadata_hash: String,
    pub combined_json: bool,
    pub file_path: String,
    pub base_path: Option<String>,
}

impl Default for SolcArgs {
    fn default() -> Self {
        Self {
            output_type: "bin".to_string(),
            metadata_hash: "none".to_string(),
            combined_json: true,
            file_path: "".to_string(),
            base_path: None,
        }
    }
}

impl SolcArgs {
    pub fn with_file_path(&mut self, file_path: &str) -> &mut Self {
        self.file_path = file_path.to_string();
        self
    }

    pub fn with_base_path(&mut self, base_path: &str) -> &mut Self {
        self.base_path = Some(base_path.to_string());
        self
    }

    pub fn to_command_args(&self) -> Vec<String> {
        let mut args = Vec::new();

        if self.combined_json {
            args.push("--combined-json".to_string());
            args.push(self.output_type.clone());
        }

        args.push("--metadata-hash".to_string());
        args.push(self.metadata_hash.clone());

        if let Some(base_path) = &self.base_path {
            args.push("--base-path".to_string());
            args.push(base_path.clone());
        }

        args.push(self.file_path.to_string());

        args
    }
}

/// Handles Docker image operations
pub struct DockerImageManager {
    docker: Arc<Docker>,
}

impl DockerImageManager {
    pub fn new(docker: Arc<Docker>) -> Self {
        #[cfg(target_arch = "aarch64")]
        debug!(
            target: "solidity_compilation",
            "Running on arm64, solc containers will use amd64 emulation"
        );

        Self { docker }
    }

    /// Ensures an image is available locally, pulling it if necessary
    pub async fn ensure_image_available(&self, image_name: &str) -> Result<(), CompilationError> {
        let mut attempts = 0;
        let mut delay = INITIAL_DELAY;

        loop {
            attempts += 1;
            match self.try_ensure_image_available(image_name).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if attempts >= MAX_ATTEMPTS {
                        return Err(e);
                    }
                    warn!(
                        target: "solidity_compilation",
                        error = %e,
                        attempt = attempts,
                        "Failed to ensure image availability, retrying after delay"
                    );
                    sleep(delay).await;
                    delay *= 2; // Exponential backoff
                }
            }
        }
    }

    /// Internal method that performs the actual image check and pull
    async fn try_ensure_image_available(&self, image_name: &str) -> Result<(), CompilationError> {
        // Check if the image exists locally first
        let images = self
            .docker
            .list_images(None::<ListImagesOptions<String>>)
            .await?;
        let image_exists = images
            .iter()
            .any(|img| img.repo_tags.contains(&image_name.to_string()));

        // Only pull the image if it doesn't exist locally
        if !image_exists {
            debug!(target: "solidity_compilation", "Pulling image: {}", image_name);
            self.docker
                .create_image(
                    Some(bollard::image::CreateImageOptions {
                        from_image: image_name.to_string(),
                        platform: "linux/amd64".to_string(), // Force amd64 platform
                        ..Default::default()
                    }),
                    None,
                    None,
                )
                .try_collect::<Vec<_>>()
                .await?;
        } else {
            debug!(target: "solidity_compilation", image_name, "Solc image exists locally");
        }
        Ok(())
    }
}

/// Manages a Docker container's lifecycle
pub struct ContainerManager {
    docker: Arc<Docker>,
    container_id: Option<String>,
    container_name: String,
    is_cleaned_up: bool, // Track cleanup state
}

impl ContainerManager {
    /// Create a new container manager
    pub fn new(docker: Arc<Docker>, name_prefix: &str) -> Self {
        let container_name = format!("{}_{}", name_prefix, Uuid::new_v4());
        Self {
            docker,
            container_id: None,
            container_name,
            is_cleaned_up: false,
        }
    }

    /// Create and start a container with the given configuration
    pub async fn create_and_start(
        &mut self,
        image: &str,
        cmd: Vec<String>,
        binds: Vec<String>,
    ) -> Result<(), CompilationError> {
        let container_config = Config {
            image: Some(image.to_string()),
            cmd: Some(cmd),
            host_config: Some(bollard::service::HostConfig {
                binds: Some(binds),
                ..Default::default()
            }),
            ..Default::default()
        };

        let create_options = Some(CreateContainerOptions {
            name: self.container_name.clone(),
            platform: Some("linux/amd64".to_string()), // Force amd64 platform
        });

        debug!(
            target: "solidity_compilation",
            container_name = %self.container_name,
            "Creating container with config: {:?}",
            container_config
        );

        let container = self
            .docker
            .create_container(create_options, container_config)
            .await?;

        self.container_id = Some(container.id);

        debug!(
            target: "solidity_compilation",
            container_name = %self.container_name,
            "Starting container"
        );

        self.docker
            .start_container(
                self.container_id.as_ref().unwrap(),
                None::<StartContainerOptions<String>>,
            )
            .await?;

        Ok(())
    }

    /// Wait for the container to finish and return its exit code
    pub async fn wait_for_exit(&self) -> Result<i64, CompilationError> {
        let container_id = self
            .container_id
            .as_ref()
            .ok_or_else(|| CompilationError::NoContainerCreated)?;

        let options = Some(WaitContainerOptions {
            condition: "not-running",
        });

        // Wait for the container to stop running
        let wait_result = self
            .docker
            .wait_container(container_id, options)
            .next()
            .await
            .ok_or_else(|| CompilationError::ContainerWaitStreamEndedUnexpectedly)?;

        let logs = self.get_logs(true, true).await?;
        debug!(target: "solidity_compilation", logs = %logs);

        match wait_result {
            Ok(exit) => Ok(exit.status_code),
            Err(e) => match e {
                bollard::errors::Error::DockerContainerWaitError { error: _, code: _ } => {
                    tracing::error!(target: "solidity_compilation", "Compilation failed: {}", logs);
                    Err(CompilationError::CompilationFailed(logs))
                }
                _ => Err(CompilationError::DockerError(e)),
            },
        }
    }

    /// Get container logs
    pub async fn get_logs(&self, stdout: bool, stderr: bool) -> Result<String, CompilationError> {
        let container_id = self
            .container_id
            .as_ref()
            .ok_or_else(|| CompilationError::NoContainerCreated)?;

        let options = Some(LogsOptions::<String> {
            stdout,
            stderr,
            ..Default::default()
        });

        // Collect logs
        let logs: Vec<_> = self
            .docker
            .logs(container_id, options)
            .try_collect::<Vec<_>>()
            .await?;

        let log_messages = logs
            .iter()
            .filter_map(|log| match log {
                bollard::container::LogOutput::StdOut { message }
                | bollard::container::LogOutput::StdErr { message } => {
                    Some(String::from_utf8_lossy(message))
                }
                _ => None,
            })
            .collect::<String>();

        Ok(log_messages)
    }

    /// Get the container name
    pub fn name(&self) -> &str {
        &self.container_name
    }

    /// Explicitly clean up the container
    pub async fn cleanup(&mut self) -> Result<(), CompilationError> {
        if self.is_cleaned_up || self.container_id.is_none() {
            return Ok(());
        }

        let container_id = self.container_id.as_ref().unwrap();

        debug!(
            target: "solidity_compilation",
            container_name = %self.container_name,
            "Removing container"
        );

        self.docker
            .remove_container(
                container_id,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await?;

        self.is_cleaned_up = true;
        Ok(())
    }
}

/// Custom Drop impl for `ContainerManager` so we clean up containers properly
/// after they go out of scope
impl Drop for ContainerManager {
    fn drop(&mut self) {
        // Check if we already cleaned up the container
        if self.is_cleaned_up || self.container_id.is_none() {
            return;
        }

        let container_id = self.container_id.as_ref().unwrap();

        // Block on the async operations
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let _ = handle.block_on(async {
                // Remove the container
                let remove_options = Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                });

                if let Err(e) = self
                    .docker
                    .remove_container(container_id, remove_options)
                    .await
                {
                    warn!(
                        target: "solidity_compilation",
                        container_name = %self.container_name,
                        error = %e,
                        "Failed to remove container in Drop"
                    );
                }
            });

            // for good measure
            self.is_cleaned_up = true;
            return;
        }

        warn!(
            target: "solidity_compilation",
            container_name = %self.container_name,
            "Failed to clean up container in Drop, couldnt get tokio runtime"
        );
    }
}

/// Manages a Solidity source file
pub struct SoliditySourceFile {
    temp_dir: TempDir,
    file_name: String,
    file_path: std::path::PathBuf,
}

impl SoliditySourceFile {
    /// Create a new source file with the given content
    pub fn new(source_code: &str) -> Result<Self, CompilationError> {
        let temp_dir = TempDir::new()?;
        let file_name = format!("{}.sol", Uuid::new_v4());
        let file_path = temp_dir.path().join(&file_name);
        std::fs::write(&file_path, source_code)?;

        Ok(Self {
            temp_dir,
            file_name,
            file_path,
        })
    }

    /// Get the file name
    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    /// Get the file path
    pub fn file_path(&self) -> &std::path::Path {
        &self.file_path
    }

    /// Get the container path configuration
    pub fn container_paths(&self) -> Result<(String, String, Vec<String>), CompilationError> {
        let container_root_path = "/code".to_string();
        let container_file_path = format!("{}/{}", container_root_path, self.file_name);

        let mount_binds = vec![format!(
            "{}:{}",
            self.temp_dir.path().to_str().ok_or_else(|| {
                CompilationError::IoError(std::io::Error::other("Invalid path"))
            })?,
            container_root_path
        )];

        Ok((container_root_path, container_file_path, mount_binds))
    }
}

/// Configuration for a Solidity compilation
#[derive(Debug, Clone)]
pub struct CompilationConfig {
    pub assertion_contract_name: String,
    pub source_code: String,
    pub compiler_version: String,
    pub output_type: String,
    pub metadata_hash: String,
}

impl CompilationConfig {
    pub fn new(assertion_contract_name: &str, source_code: &str, compiler_version: &str) -> Self {
        // Validate compiler version format (should be like 0.8.17)
        // This regex handles both exact versions (0.8.17) and complex version requirements (=0.8.28 ^0.8.13)
        let version_regex =
            Regex::new(r"^\d+\.\d+\.\d+$|^=\d+\.\d+\.\d+\s+\^\d+\.\d+\.\d+$").unwrap();
        if !version_regex.is_match(compiler_version) {
            debug!(
                target: "solidity_compilation",
                compiler_version = compiler_version,
                "Invalid compiler version format"
            );
        }

        Self {
            assertion_contract_name: assertion_contract_name.to_string(),
            source_code: source_code.to_string(),
            compiler_version: compiler_version.to_string(),
            output_type: "bin".to_string(),
            metadata_hash: "none".to_string(),
        }
    }
}

/// Manages metrics for Solidity compilation
pub struct CompilationMetrics {
    labels: [(&'static str, String); 1],
}

impl CompilationMetrics {
    pub fn new(compiler_version: &str) -> Self {
        Self {
            labels: [("compiler_version", compiler_version.to_string())],
        }
    }

    pub fn start_compilation(&self) {
        metrics::counter!("assertion_compilations_total", &self.labels).increment(1);
        metrics::gauge!("assertion_compilations_running", &self.labels).increment(1);
    }

    pub fn end_compilation(&self) {
        metrics::gauge!("assertion_compilations_running", &self.labels).decrement(1);
    }
}

/// Manages the solidity compilation process
pub struct SolidityCompiler {
    docker: Arc<Docker>,
}

impl SolidityCompiler {
    pub fn new(docker: Arc<Docker>) -> Self {
        Self { docker }
    }

    /// Compiles a Solidity contract and returns the bytecode
    pub async fn compile(&self, config: CompilationConfig) -> Result<Vec<u8>, CompilationError> {
        let metrics = CompilationMetrics::new(&config.compiler_version);
        metrics.start_compilation();

        // Ensure the contract exists in the source code
        if !config.source_code.contains(&config.assertion_contract_name) {
            return Err(CompilationError::ContractNotFound(
                config.assertion_contract_name,
            ));
        }

        // Set up the image
        let image_name = format!("ethereum/solc:{}", config.compiler_version);
        let image_manager = DockerImageManager::new(self.docker.clone());
        image_manager.ensure_image_available(&image_name).await?;

        // Create the source file
        let source_file = SoliditySourceFile::new(&config.source_code)?;
        let (container_root_path, container_file_path, mount_binds) =
            source_file.container_paths()?;

        // Configure the compiler arguments
        let mut solc_args = SolcArgs::default();
        solc_args
            .with_file_path(&container_file_path)
            .with_base_path(&container_root_path);

        // Override with config values if specified
        solc_args.output_type = config.output_type;
        solc_args.metadata_hash = config.metadata_hash;

        // Execute the compilation
        let result = self
            .execute_compilation(
                &image_name,
                solc_args.to_command_args(),
                mount_binds,
                &config.assertion_contract_name,
                source_file.file_name(),
            )
            .instrument(tracing::info_span!("execute_compilation"))
            .await;

        // Always decrement the metrics counter
        metrics.end_compilation();

        result
    }

    /// Executes the compilation in a container and extracts the bytecode
    #[instrument(name = "execute_compilation", skip(self, mount_binds))]
    async fn execute_compilation(
        &self,
        image_name: &str,
        cmd_args: Vec<String>,
        mount_binds: Vec<String>,
        contract_name: &str,
        file_name: &str,
    ) -> Result<Vec<u8>, CompilationError> {
        // Create and run the container
        let mut container = ContainerManager::new(self.docker.clone(), contract_name);
        // Use a defer-like pattern with Result to ensure cleanup happens
        let result = async {
            container
                .create_and_start(image_name, cmd_args, mount_binds)
                .await?;
            debug!(target: "solidity_compilation", "Container started");
            // Wait for completion
            let exit_code = container.wait_for_exit().await?;

            if exit_code != 0 {
                let logs = container.get_logs(true, true).await?;
                tracing::error!(target: "solidity_compilation", "Compilation failed: {}", logs);
                return Err(CompilationError::CompilationFailed(logs));
            }

            // Get the output logs
            let log_messages = container.get_logs(true, false).await?;

            // Parse output and extract bytecode
            let bytecode = self
                .extract_bytecode(&log_messages, file_name, contract_name)
                .await?;

            debug!(
                target: "solidity_compilation",
                container_name = %container.name(),
                "Finished Solidity compilation"
            );

            Ok(bytecode)
        }
        .await;

        // Always try to clean up, regardless of the compilation result
        if let Err(e) = container.cleanup().await {
            debug!(
                target: "solidity_compilation",
                container_name = %container.name(),
                error = %e,
                "Failed to clean up container"
            );
        }

        result
    }

    /// Parse the JSON output and extract the bytecode
    async fn extract_bytecode(
        &self,
        json_output: &str,
        file_name: &str,
        contract_name: &str,
    ) -> Result<Vec<u8>, CompilationError> {
        // Parse the JSON output from solc
        let output: Value = serde_json::from_str(json_output)?;

        // Extract the bytecode from the assertion contract
        let contracts = output["contracts"]
            .as_object()
            .ok_or_else(|| CompilationError::InvalidJsonOutput)?;

        let contract_key = format!("{file_name}:{contract_name}");
        let bytecode = contracts[&contract_key]["bin"]
            .as_str()
            .ok_or_else(|| CompilationError::MissingBytecode)?;

        // Convert hex bytecode to bytes
        let bytecode_bytes = hex::decode(bytecode)?;

        Ok(bytecode_bytes)
    }
}

/// Compiles Solidity source code using a Docker container with the specified compiler version
pub async fn compile_solidity(
    assertion_contract_name: &str,
    source_code: &str,
    compiler_version: &str,
    docker: Arc<Docker>,
) -> Result<Vec<u8>> {
    let config = CompilationConfig::new(assertion_contract_name, source_code, compiler_version);
    let compiler = SolidityCompiler::new(docker);
    compiler.compile(config).await.map_err(Into::into)
}

#[derive(Debug, thiserror::Error)]
pub enum CompilationError {
    #[error("Contract '{0}' not found in source code")]
    ContractNotFound(String),

    #[error("Compilation failed: {0}")]
    CompilationFailed(String),

    #[error("Missing bytecode in compiler output")]
    MissingBytecode,

    #[error("Invalid compiler output: {0}")]
    InvalidOutput(String),

    #[error("Docker error {0}")]
    DockerError(#[from] bollard::errors::Error),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Hex decoding error: {0}")]
    HexError(#[from] hex::FromHexError),

    #[error("No container was created")]
    NoContainerCreated,

    #[error("Container wait stream ended unexpectedly")]
    ContainerWaitStreamEndedUnexpectedly,

    #[error("Invalid JSON output: missing 'contracts' object")]
    InvalidJsonOutput,
}

#[cfg(all(test, feature = "full-test"))]
mod tests {
    use super::*;
    
    #[cfg(feature = "full-test")]
    use once_cell::sync::Lazy;

    // Shared Docker client for all tests
    #[cfg(feature = "full-test")]
    static DOCKER: Lazy<Arc<Docker>> =
        Lazy::new(|| Arc::new(Docker::connect_with_local_defaults().unwrap()));

    #[cfg(feature = "full-test")]
    fn setup_docker() -> Arc<Docker> {
        DOCKER.clone()
    }

    #[cfg(feature = "full-test")]
    #[tokio::test]
    async fn test_successful_compilation() {
        let docker = setup_docker();
        let source_code = r#"
            // SPDX-License-Identifier: MIT
            pragma solidity ^0.8.0;
            
            contract SimpleStorage {
                uint256 private value;
                
                function set(uint256 _value) public {
                    value = _value;
                }
                
                function get() public view returns (uint256) {
                    return value;
                }
            }
        "#;

        let result = compile_solidity("SimpleStorage", source_code, "0.8.17", docker).await;
        assert!(result.is_ok(), "Compilation should succeed");
        let bytecode = result.unwrap();
        assert!(!bytecode.is_empty(), "Bytecode should not be empty");
    }

    #[cfg(feature = "full-test")]
    #[tokio::test]
    async fn test_invalid_compiler_version() {
        let docker = setup_docker();
        let source_code = "contract Test {}";

        let result = compile_solidity("SimpleStorage", source_code, "999.999.999", docker).await;
        assert!(result.is_err(), "Should fail with invalid compiler version");
    }

    #[cfg(feature = "full-test")]
    #[tokio::test]
    async fn test_multiple_contracts() {
        let docker = setup_docker();
        let source_code = r#"
            // SPDX-License-Identifier: MIT
            pragma solidity ^0.8.0;
            
            contract First {
                uint256 private value;
            }
            
            contract Second {
                string private name;
            }
        "#;

        let result = compile_solidity("First", source_code, "0.8.17", docker).await;
        assert!(result.is_ok(), "Should succeed with multiple contracts");
    }

    #[cfg(feature = "full-test")]
    #[tokio::test]
    async fn test_syntax_error() {
        let docker = setup_docker();
        let source_code = r#"
            contract BrokenContract {
                This is not valid Solidity;
            }
        "#;

        let result = compile_solidity("BrokenContract", source_code, "0.8.17", docker).await;
        assert!(result.is_err(), "Should fail with syntax error");
    }

    #[cfg(feature = "full-test")]
    #[tokio::test]
    async fn test_empty_source() {
        let docker = setup_docker();
        let result = compile_solidity("", "", "0.8.17", docker).await;
        assert!(result.is_err(), "Should fail with empty source");
    }

    #[cfg(feature = "full-test")]
    #[tokio::test]
    async fn test_different_compiler_versions() {
        let docker = setup_docker();
        let source_code = r#"
            // SPDX-License-Identifier: MIT
            pragma solidity ^0.8.0;
            
            contract VersionTest {
                uint256 private value;
            }
        "#;

        // Test multiple compiler versions
        let versions = vec!["0.8.17", "0.8.20", "0.8.24"];

        for version in versions {
            let result =
                compile_solidity("VersionTest", source_code, version, docker.clone()).await;
            assert!(
                result.is_ok(),
                "Compilation should succeed with version {version}"
            );
            assert!(!result.unwrap().is_empty(), "Bytecode should not be empty");
        }
    }

    #[cfg(feature = "full-test")]
    #[tokio::test]
    async fn test_complex_contract() {
        let docker = setup_docker();
        let source_code = std::fs::read_to_string("../../../testdata/MockAssertion.sol").unwrap();

        let result = compile_solidity("MockAssertion", &source_code, "0.8.28", docker).await;
        assert!(
            result.is_ok(),
            "Complex contract compilation should succeed"
        );
        assert!(
            !result.unwrap().is_empty(),
            "Complex contract bytecode should not be empty"
        );
    }

    #[cfg(feature = "full-test")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_compilations() {
        let docker = setup_docker();
        let source_code = r#"
            // SPDX-License-Identifier: MIT
            pragma solidity ^0.8.0;
            
            contract Simple {
                uint256 private value;
            }
        "#;

        let mut handles = vec![];

        // Launch 5 concurrent compilations
        for _ in 0..5 {
            let docker_clone = docker.clone();
            let source_clone = source_code.to_string();

            handles.push(tokio::spawn(async move {
                compile_solidity("Simple", &source_clone, "0.8.17", docker_clone).await
            }));
        }

        // Wait for all compilations to complete
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok(), "Concurrent compilation should succeed");
            assert!(!result.unwrap().is_empty(), "Bytecode should not be empty");
        }
    }

    #[cfg(feature = "full-test")]
    #[tokio::test]
    async fn test_docker_image_manager() {
        let docker = setup_docker();
        let image_manager = DockerImageManager::new(docker);

        // Test with a valid image
        let result = image_manager
            .ensure_image_available("ethereum/solc:0.8.17")
            .await;
        assert!(result.is_ok(), "Should successfully ensure a valid image");

        // Test with an invalid image (should still not error but might warn)
        let result = image_manager
            .ensure_image_available("does-not-exist:latest")
            .await;
        assert!(result.is_err(), "Should fail with an invalid image");
    }

    #[tokio::test]
    async fn test_solidity_source_file() {
        let source_code = "contract Test {}";
        let source_file = SoliditySourceFile::new(source_code).unwrap();

        assert!(
            source_file.file_name().ends_with(".sol"),
            "File should have .sol extension"
        );
        assert!(
            source_file.file_path().exists(),
            "File should exist on disk"
        );

        let (root_path, file_path, binds) = source_file.container_paths().unwrap();
        assert_eq!(root_path, "/code", "Root path should be /code");
        assert!(
            file_path.starts_with("/code/"),
            "File path should be in /code"
        );
        assert_eq!(binds.len(), 1, "Should have one bind mount");
    }

    #[cfg(feature = "full-test")]
    #[tokio::test]
    async fn test_container_cleanup() {
        let docker = setup_docker();

        // Create an image manager and ensure the image exists first
        let image_manager = DockerImageManager::new(docker.clone());
        image_manager
            .ensure_image_available("ethereum/solc:0.8.17")
            .await
            .expect("Failed to ensure image availability");

        // Create a test container
        let mut container = ContainerManager::new(docker.clone(), "cleanup_test");

        // Simple command that will exit quickly
        let cmd = vec!["solc".to_string(), "--version".to_string()];

        // Create and start the container
        container
            .create_and_start("ethereum/solc:0.8.17", cmd, vec![])
            .await
            .expect("Failed to create and start container");

        // Save the container ID for later verification
        let container_id = container.container_id.clone().expect("No container ID");

        // Verify the container was actually created and is in the list
        let containers_after_creation = docker
            .list_containers(Some(bollard::container::ListContainersOptions::<String> {
                all: true,
                ..Default::default()
            }))
            .await
            .expect("Failed to list containers");

        // Check that no container names contain "CleanupTest"
        let cleanup_containers = containers_after_creation
            .iter()
            .filter(|c| c.id.as_ref() == Some(&container_id))
            .collect::<Vec<_>>();

        assert!(
            cleanup_containers.len() == 1,
            "Container should have been created"
        );

        // Wait for the container to finish
        let exit_code = container
            .wait_for_exit()
            .await
            .expect("Failed to wait for container exit");

        assert_eq!(exit_code, 0, "Container should have exited with code 0");

        // Cleanup the container
        container
            .cleanup()
            .await
            .expect("Failed to clean up container");

        let containers_after_cleanup = docker
            .list_containers(Some(bollard::container::ListContainersOptions::<String> {
                all: true, // Include stopped containers
                ..Default::default()
            }))
            .await
            .expect("Failed to list containers");

        // Check that no container names contain "CleanupTest"
        let cleanup_containers = containers_after_cleanup
            .iter()
            .filter(|c| c.id.as_ref() == Some(&container_id))
            .collect::<Vec<_>>();

        assert!(
            cleanup_containers.is_empty(),
            "No containers with 'CleanupTest' in the name should exist after compilation"
        );
    }

    #[cfg(feature = "full-test")]
    #[tokio::test]
    async fn test_container_cleanup_after_compilation() {
        let docker = setup_docker();
        let source_code = r#"
            // SPDX-License-Identifier: MIT
            pragma solidity ^0.8.0;
            
            contract CleanupTest {
                uint256 private value;
            }
        "#;

        let result = compile_solidity("CleanupTest", source_code, "0.8.17", docker.clone()).await;
        assert!(result.is_ok(), "Compilation should succeed");

        // After compilation, check that no containers with the contract name exist
        let containers = docker
            .list_containers(Some(bollard::container::ListContainersOptions::<String> {
                all: true, // Include stopped containers
                ..Default::default()
            }))
            .await
            .expect("Failed to list containers");

        // Check that no container names contain "CleanupTest"
        let cleanup_containers = containers
            .iter()
            .filter(|c| {
                c.names
                    .as_ref()
                    .is_some_and(|names| names.iter().any(|name| name.contains("CleanupTest")))
            })
            .collect::<Vec<_>>();

        assert!(
            cleanup_containers.is_empty(),
            "No containers with 'CleanupTest' in the name should exist after compilation"
        );
    }

    #[cfg(feature = "full-test")]
    #[tokio::test]
    async fn test_image_pull_mechanism() {
        let docker = setup_docker();

        // Use a specific solc version that's not used in other tests
        // This minimizes interference with other tests
        let test_specific_version = "0.8.15"; // Choose a version not used elsewhere
        let image_name = format!("ethereum/solc:{test_specific_version}");

        // Try to remove the image if it exists
        let _ = docker
            .remove_image(
                &image_name,
                Some(bollard::image::RemoveImageOptions {
                    force: true,
                    ..Default::default()
                }),
                None,
            )
            .await;

        // Verify the image is gone
        let images = docker
            .list_images(None::<ListImagesOptions<String>>)
            .await
            .expect("Failed to list images");

        let image_exists = images
            .iter()
            .any(|img| img.repo_tags.contains(&image_name.to_string()));

        if image_exists {
            println!("Warning: Could not remove image for testing, test may be less reliable");
        }

        // Use a simple contract with a pragma that works with our specific version
        let source_code = r#"
            // SPDX-License-Identifier: MIT
            pragma solidity ^0.8.0;
            
            contract TestPull {
                uint256 private value;
            }
        "#;

        // Run compilation which should trigger a pull
        let result = compile_solidity(
            "TestPull",
            source_code,
            test_specific_version,
            docker.clone(),
        )
        .await;

        // Verify compilation succeeded
        assert!(
            result.is_ok(),
            "Compilation should succeed after pulling image"
        );

        // Verify the image now exists
        let images_after = docker
            .list_images(None::<ListImagesOptions<String>>)
            .await
            .expect("Failed to list images");

        let image_exists_after = images_after
            .iter()
            .any(|img| img.repo_tags.contains(&image_name.to_string()));

        assert!(image_exists_after, "Image should exist after compilation");
    }
}
