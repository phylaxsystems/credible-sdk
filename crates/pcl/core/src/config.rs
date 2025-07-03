use crate::error::ConfigError;
use alloy_primitives::Address;
use chrono::{
    DateTime,
    Utc,
};
use clap::Parser;
use colored::Colorize;
use dirs::home_dir;
use pcl_common::args::CliArgs;
use serde::{
    Deserialize,
    Deserializer,
    Serialize,
    Serializer,
    de::{
        self,
        Visitor,
    },
};

use std::{
    collections::HashMap,
    fmt,
    path::PathBuf,
};

/// Directory name for storing PCL configuration
pub const CONFIG_DIR: &str = ".pcl";
/// Configuration file name
pub const CONFIG_FILE: &str = "config.toml";

/// Main configuration structure for PCL
///
/// This struct holds all the configuration data for the PCL tool,
/// including authentication details and pending assertions.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CliConfig {
    /// Optional authentication details
    pub auth: Option<UserAuth>,
    /// Map of assertions pending submission, keyed by contract name
    pub assertions_for_submission: HashMap<AssertionKey, AssertionForSubmission>,
}

/// Key structure for assertions, used in the configuration map for storing assertions
#[derive(Clone, Debug, Default, Hash, Eq, PartialEq)]
pub struct AssertionKey {
    pub assertion_name: String,
    pub constructor_args: Vec<String>,
}
impl AssertionKey {
    /// Create a new assertion key
    ///
    /// # Arguments
    /// * `assertion_name` - The name of the assertion
    /// * `constructor_args` - The constructor arguments for the assertion
    pub fn new(assertion_name: String, constructor_args: Vec<String>) -> Self {
        Self {
            assertion_name,
            constructor_args,
        }
    }
}

impl fmt::Display for AssertionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.assertion_name)?;

        if !self.constructor_args.is_empty() {
            write!(f, "(")?;
            write!(f, "{}", self.constructor_args.join(","))?;
            write!(f, ")")?;
        }

        Ok(())
    }
}

impl From<String> for AssertionKey {
    fn from(assertion_key: String) -> Self {
        if !assertion_key.contains('(') {
            return Self {
                assertion_name: assertion_key,
                constructor_args: vec![],
            };
        }

        let (assertion_name, mut args) = assertion_key
            .split_once('(')
            .expect("Invalid assertion key");

        args = args.trim_end_matches(')');

        if args.is_empty() {
            return Self {
                assertion_name: assertion_name.to_string(),
                constructor_args: vec![],
            };
        }

        let constructor_args = args.split(',').map(|arg| arg.to_string()).collect();

        Self {
            assertion_name: assertion_name.to_string(),
            constructor_args,
        }
    }
}
// Custom Serialize implementation
impl Serialize for AssertionKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Convert AssertionKey to string using Display implementation
        let s = self.to_string();
        serializer.serialize_str(&s)
    }
}

// Custom Deserialize implementation
impl<'de> Deserialize<'de> for AssertionKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Use a visitor to deserialize the string
        struct AssertionKeyVisitor;

        impl<'de> Visitor<'de> for AssertionKeyVisitor {
            type Value = AssertionKey;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string in format 'name' or 'name(arg1,arg2,...)'")
            }

            fn visit_str<E>(self, value: &str) -> Result<AssertionKey, E>
            where
                E: de::Error,
            {
                Ok(AssertionKey::from(value.to_string()))
            }

            fn visit_string<E>(self, value: String) -> Result<AssertionKey, E>
            where
                E: de::Error,
            {
                Ok(AssertionKey::from(value))
            }
        }

        deserializer.deserialize_string(AssertionKeyVisitor)
    }
}

/// Command-line arguments for configuration management
#[derive(Parser)]
pub struct ConfigArgs {
    #[command(subcommand)]
    command: ConfigCommand,
}

/// Subcommands for configuration management
#[derive(clap::Subcommand)]
enum ConfigCommand {
    #[command(about = "Display the current configuration")]
    Show,
    #[command(about = "Delete the current configuration")]
    Delete,
}

impl ConfigArgs {
    /// Executes the configuration command
    ///
    /// # Arguments
    /// * `config` - The configuration to operate on
    ///
    /// # Returns
    /// * `Result<(), ConfigError>` - Success or error
    pub fn run(&self, config: &mut CliConfig) -> Result<(), ConfigError> {
        match self.command {
            ConfigCommand::Show => {
                println!("{config}");
                Ok(())
            }
            ConfigCommand::Delete => {
                *config = CliConfig::default();
                Ok(())
            }
        }
    }
}

impl CliConfig {
    /// Writes the configuration to the default config file, or a specific directory
    ///
    /// # Arguments
    /// * `cli_args` - Command line arguments
    ///
    /// # Returns
    /// * `Result<(), ConfigError>` - Success or error
    pub fn write_to_file(&self, cli_args: &CliArgs) -> Result<(), ConfigError> {
        self.write_to_file_at_dir(
            cli_args
                .config_dir
                .clone()
                .unwrap_or(Self::get_config_dir()),
        )
    }

    /// Writes the configuration to a specific directory
    ///
    /// # Arguments
    /// * `config_dir` - Directory to write the config file to
    ///
    /// # Returns
    /// * `Result<(), ConfigError>` - Success or error
    fn write_to_file_at_dir(&self, config_dir: PathBuf) -> Result<(), ConfigError> {
        // Ensure directory exists and is writable
        Self::ensure_writable_directory(&config_dir)?;

        // Get config file path and check permissions
        let config_file = config_dir.join(CONFIG_FILE);
        Self::ensure_writable_file(&config_file)?;

        // Serialize and write config
        let config_str = toml::to_string(self).map_err(ConfigError::SerializeError)?;
        std::fs::write(config_file, config_str).map_err(ConfigError::WriteError)?;
        Ok(())
    }

    /// Ensures a directory exists and is writable
    ///
    /// # Arguments
    /// * `dir` - Directory to check
    ///
    /// # Returns
    /// * `Result<(), ConfigError>` - Success or error
    fn ensure_writable_directory(dir: &PathBuf) -> Result<(), ConfigError> {
        if !dir.exists() {
            std::fs::create_dir_all(dir).map_err(|e| {
                ConfigError::WriteError(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("Failed to create config directory: {e}"),
                ))
            })?;
        }

        // Test write permissions by creating a temporary file
        let temp_file = dir.join(".pcl_test_write");
        std::fs::write(&temp_file, "").map_err(|e| {
            ConfigError::WriteError(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("No write permissions in config directory: {e}"),
            ))
        })?;
        std::fs::remove_file(&temp_file).ok(); // Clean up test file

        Ok(())
    }

    /// Ensures a file is writable
    ///
    /// # Arguments
    /// * `file` - File to check
    ///
    /// # Returns
    /// * `Result<(), ConfigError>` - Success or error
    fn ensure_writable_file(file: &PathBuf) -> Result<(), ConfigError> {
        if file.exists() {
            let metadata = std::fs::metadata(file).map_err(|e| {
                ConfigError::WriteError(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("Failed to check file permissions: {e}"),
                ))
            })?;

            if metadata.permissions().readonly() {
                return Err(ConfigError::WriteError(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "Config file is read-only",
                )));
            }
        }
        Ok(())
    }

    /// Gets the default configuration directory path
    ///
    /// # Returns
    /// * `PathBuf` - Path to the config directory
    pub fn get_config_dir() -> PathBuf {
        home_dir().unwrap().join(CONFIG_DIR)
    }

    /// Reads configuration from a specific directory
    ///
    /// # Arguments
    /// * `config_dir` - Directory to read the config file from
    ///
    /// # Returns
    /// * `Result<Self, ConfigError>` - Configuration or error
    fn read_from_file_at_dir(config_dir: PathBuf) -> Result<Self, ConfigError> {
        let config_file = config_dir.join(CONFIG_FILE);

        // If file doesn't exist, return default config
        if !config_file.exists() {
            return Ok(Self::default());
        }

        // Check if we have read permissions
        let metadata = std::fs::metadata(&config_file).map_err(|e| {
            ConfigError::ReadError(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("Failed to check file permissions: {e}"),
            ))
        })?;

        if !metadata.permissions().readonly() {
            // Test read permissions
            std::fs::read_to_string(&config_file).map_err(|e| {
                ConfigError::ReadError(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("No read permissions for config file: {e}"),
                ))
            })?;
        }

        let config_str = std::fs::read_to_string(config_file).map_err(ConfigError::ReadError)?;
        toml::from_str(&config_str).map_err(ConfigError::ParseError)
    }

    /// Reads configuration from the default config file, or a specific directory
    ///
    /// # Arguments
    /// * `cli_args` - Command line arguments
    ///
    /// # Returns
    /// * `Result<Self, ConfigError>` - Configuration or error
    pub fn read_from_file(cli_args: &CliArgs) -> Result<Self, ConfigError> {
        Self::read_from_file_at_dir(
            cli_args
                .config_dir
                .clone()
                .unwrap_or(Self::get_config_dir()),
        )
    }

    /// Adds an assertion to the pending submissions
    ///
    /// # Arguments
    /// * `assertion_for_submission` - The assertion to add
    pub fn add_assertion_for_submission(
        &mut self,
        assertion_for_submission: AssertionForSubmission,
    ) {
        let assertion_key = AssertionKey {
            assertion_name: assertion_for_submission.assertion_contract.clone(),
            constructor_args: assertion_for_submission.constructor_args.clone(),
        };
        self.assertions_for_submission
            .insert(assertion_key, assertion_for_submission);
    }
}

impl fmt::Display for CliConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let config_path = Self::get_config_dir().join(CONFIG_FILE);

        writeln!(f, "PCL Configuration")?;
        writeln!(f, "==================")?;
        writeln!(f, "Config path: {}", config_path.display())?;

        match &self.auth {
            Some(auth) => writeln!(f, "{auth}")?,
            None => writeln!(f, "Authentication: Not authenticated")?,
        }
        if !self.assertions_for_submission.is_empty() {
            writeln!(f, "\nPending Assertions for Submission")?;
            writeln!(f, "--------------------------------")?;
            for (i, assertion) in self.assertions_for_submission.values().enumerate() {
                writeln!(f, "Assertion #{}:\n{}", i + 1, assertion)?;
            }
        } else {
            writeln!(f, "\nNo pending assertions for submission")?;
        }

        Ok(())
    }
}

/// Authentication details for a user
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct UserAuth {
    /// Access token for API authentication
    pub access_token: String,
    /// Refresh token for obtaining new access tokens
    pub refresh_token: String,
    /// Ethereum address of the user
    pub user_address: Address,
    /// Token expiration timestamp
    #[serde(with = "chrono::serde::ts_seconds")]
    pub expires_at: DateTime<Utc>,
}

impl fmt::Display for UserAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Authentication:")?;
        writeln!(f, "  User Address: {}", self.user_address)?;
        let now = Utc::now();
        let expired = self.expires_at < now;
        let expiry_text = self.expires_at.format("%Y-%m-%d %H:%M:%S UTC").to_string();
        if expired {
            writeln!(f, "  Token Expired at {}", expiry_text.red())?;
        } else {
            writeln!(f, "  Token Expires at {}", expiry_text.green())?;
        }

        // Don't display actual tokens for security reasons
        writeln!(f, "  Access Token: [Set]")?;
        writeln!(f, "  Refresh Token: [Set]")?;
        Ok(())
    }
}

/// An assertion that is pending submission to the DA layer
#[derive(Debug, Default, Serialize, Deserialize, Hash, Eq, PartialEq, Clone)]
pub struct AssertionForSubmission {
    /// Name of the assertion contract
    pub assertion_contract: String,
    /// Unique identifier for the assertion
    pub assertion_id: String,
    /// Cryptographic signature of the assertion
    pub signature: String,
    /// Constructor arguments for the assertion
    pub constructor_args: Vec<String>,
}

impl fmt::Display for AssertionForSubmission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Contract: {}", self.assertion_contract)?;
        writeln!(f, "  ID: {}", self.assertion_id)?;
        writeln!(f, "  Constructor Args: {}", self.constructor_args.join(","))?;
        write!(
            f,
            "  DA Signature: {}...",
            &self.signature.chars().take(10).collect::<String>()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;

    /// Helper function to set up a temporary config directory
    fn setup_config_dir() -> (PathBuf, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        unsafe {
            env::set_var("HOME", temp_dir.path());
        }
        (temp_dir.path().join(CONFIG_DIR), temp_dir)
    }

    /// Helper function to create a read-only directory
    fn create_readonly_dir(path: &PathBuf) -> std::io::Result<()> {
        fs::create_dir_all(path)?;
        let mut perms = fs::metadata(path)?.permissions();
        perms.set_mode(0o555); // Read and execute only
        fs::set_permissions(path, perms)
    }

    /// Helper function to create a read-only file
    fn create_readonly_file(path: &PathBuf) -> std::io::Result<()> {
        fs::write(path, "")?;
        let mut perms = fs::metadata(path)?.permissions();
        perms.set_mode(0o444); // Read only
        fs::set_permissions(path, perms)
    }

    #[test]
    fn test_write_and_read_config() {
        let (config_dir, _temp_dir) = setup_config_dir();

        // Use a fixed timestamp for testing
        let fixed_timestamp = DateTime::from_timestamp(1672502400, 0).unwrap(); // 2022-12-31 16:00:00 UTC

        let config = CliConfig {
            auth: Some(UserAuth {
                access_token: "test_access".to_string(),
                refresh_token: "test_refresh".to_string(),
                user_address: Address::from_slice(&[0; 20]),
                expires_at: fixed_timestamp,
            }),
            assertions_for_submission: vec![(
                "contract1".to_string().into(),
                AssertionForSubmission {
                    assertion_contract: "contract1".to_string(),
                    assertion_id: "id1".to_string(),
                    signature: "sig1".to_string(),
                    constructor_args: vec!["arg1".to_string(), "arg2".to_string()],
                },
            )]
            .into_iter()
            .collect(),
        };

        // Test writing
        config.write_to_file_at_dir(config_dir.clone()).unwrap();

        // Test reading
        let read_config = CliConfig::read_from_file_at_dir(config_dir.clone()).unwrap();
        assert_eq!(
            read_config.auth.as_ref().unwrap().access_token,
            "test_access"
        );
        assert_eq!(
            read_config.auth.as_ref().unwrap().refresh_token,
            "test_refresh"
        );
        assert_eq!(
            read_config.auth.as_ref().unwrap().user_address,
            Address::from_slice(&[0; 20])
        );
        assert_eq!(read_config.assertions_for_submission.len(), 1);
        assert_eq!(
            read_config
                .assertions_for_submission
                .get(&("contract1".to_string().into()))
                .unwrap()
                .assertion_contract,
            "contract1"
        );

        // Test display format - check for key content rather than exact match due to color codes
        let formatted_cfg = format!("{read_config}");

        // Check that all the important information is present
        assert!(formatted_cfg.contains("PCL Configuration"));
        assert!(formatted_cfg.contains("Config path:"));
        assert!(formatted_cfg.contains(".pcl/config.toml"));
        assert!(formatted_cfg.contains("User Address: 0x0000000000000000000000000000000000000000"));
        assert!(formatted_cfg.contains("2022-12-31 16:00:00 UTC"));
        assert!(formatted_cfg.contains("Access Token: [Set]"));
        assert!(formatted_cfg.contains("Refresh Token: [Set]"));
        assert!(formatted_cfg.contains("Pending Assertions for Submission"));
        assert!(formatted_cfg.contains("Contract: contract1"));
        assert!(formatted_cfg.contains("ID: id1"));
        assert!(formatted_cfg.contains("Constructor Args: arg1,arg2"));
        assert!(formatted_cfg.contains("DA Signature: sig1..."));
    }

    #[test]
    fn test_read_nonexistent_config() {
        let (config_dir, _temp_dir) = setup_config_dir();

        // Try reading without creating a file
        let config = CliConfig::read_from_file_at_dir(config_dir).unwrap();
        assert!(config.auth.is_none());
        assert!(config.assertions_for_submission.is_empty());
    }

    #[test]
    fn test_add_assertion_for_submission() {
        let mut config = CliConfig::default();
        let assertion = AssertionForSubmission {
            assertion_contract: "test_contract".to_string(),
            assertion_id: "test_id".to_string(),
            signature: "test_signature".to_string(),
            constructor_args: vec!["arg1".to_string(), "arg2".to_string()],
        };

        config.add_assertion_for_submission(assertion.clone());
        assert_eq!(config.assertions_for_submission.len(), 1);
        assert_eq!(
            config
                .assertions_for_submission
                .get(&("test_contract(arg1,arg2)".to_string().into()))
                .unwrap(),
            &assertion
        );
    }

    #[test]
    fn test_user_auth_display() {
        let auth = UserAuth {
            access_token: "test_access".to_string(),
            refresh_token: "test_refresh".to_string(),
            user_address: Address::from_slice(&[0; 20]),
            expires_at: DateTime::from_timestamp(1672502400, 0).unwrap(), // 2022-12-31 16:00:00 UTC
        };

        let display = format!("{auth}");
        assert!(display.contains("User Address: 0x0000000000000000000000000000000000000000"));
        assert!(display.contains("Token Expired at"));
        assert!(display.contains("Access Token: [Set]"));
        assert!(display.contains("Refresh Token: [Set]"));
    }

    #[test]
    fn test_assertion_for_submission_display() {
        let assertion = AssertionForSubmission {
            assertion_contract: "test_contract".to_string(),
            assertion_id: "test_id".to_string(),
            signature: "test_signature".to_string(),
            constructor_args: vec!["arg1".to_string(), "arg2".to_string()],
        };

        let display = format!("{assertion}");
        assert!(display.contains("Contract: test_contract"));
        assert!(display.contains("ID: test_id"));
        assert!(display.contains("Signature: test_signa..."));
    }

    #[test]
    fn test_config_args_show() {
        let mut config = CliConfig::default();
        let args = ConfigArgs {
            command: ConfigCommand::Show,
        };
        assert!(args.run(&mut config).is_ok());
    }

    #[test]
    fn test_config_args_delete() {
        let mut config = CliConfig {
            auth: Some(UserAuth {
                access_token: "test".to_string(),
                refresh_token: "test".to_string(),
                user_address: Address::from_slice(&[0; 20]),
                expires_at: DateTime::from_timestamp(1672502400, 0).unwrap(),
            }),
            assertions_for_submission: HashMap::new(),
        };
        let args = ConfigArgs {
            command: ConfigCommand::Delete,
        };
        assert!(args.run(&mut config).is_ok());
        assert!(config.auth.is_none());
        assert!(config.assertions_for_submission.is_empty());
    }

    #[test]
    fn test_write_to_file_permission_error() {
        let temp_dir = tempfile::tempdir().unwrap();

        // Create a read-only directory
        let mut perms = std::fs::metadata(&temp_dir).unwrap().permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&temp_dir, perms).unwrap();

        let config = CliConfig::default();
        let result = config.write_to_file_at_dir(temp_dir.path().to_path_buf());

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Permission denied")
        );
    }

    #[test]
    fn test_read_from_file_invalid_toml() {
        let (config_dir, _temp_dir) = setup_config_dir();
        let config_file = config_dir.join(CONFIG_FILE);
        fs::create_dir_all(&config_dir).unwrap();
        fs::write(config_file, "invalid toml content").unwrap();

        let result = CliConfig::read_from_file_at_dir(config_dir);
        assert!(result.is_err());
    }

    #[test]
    fn test_display_empty_config() {
        let config = CliConfig::default();
        let display = format!("{config}");
        assert!(display.contains("Not authenticated"));
        assert!(display.contains("No pending assertions for submission"));
    }

    #[test]
    fn test_display_config_with_multiple_assertions() {
        let mut config = CliConfig::default();
        config.add_assertion_for_submission(AssertionForSubmission {
            assertion_contract: "contract1".to_string(),
            assertion_id: "id1".to_string(),
            signature: "sig1".to_string(),
            constructor_args: vec!["arg1".to_string(), "arg2".to_string()],
        });
        config.add_assertion_for_submission(AssertionForSubmission {
            assertion_contract: "contract2".to_string(),
            assertion_id: "id2".to_string(),
            signature: "sig2".to_string(),
            constructor_args: vec!["arg3".to_string(), "arg4".to_string()],
        });

        let display = format!("{config}");
        assert!(display.contains("Assertion #1"));
        assert!(display.contains("Assertion #2"));
        assert!(display.contains("contract1"));
        assert!(display.contains("contract2"));
    }

    #[test]
    fn test_user_auth_serialization() {
        let auth = UserAuth {
            access_token: "test_access".to_string(),
            refresh_token: "test_refresh".to_string(),
            user_address: Address::from_slice(&[0; 20]),
            expires_at: DateTime::from_timestamp(1672502400, 0).unwrap(),
        };

        let serialized = toml::to_string(&auth).unwrap();
        let deserialized: UserAuth = toml::from_str(&serialized).unwrap();

        assert_eq!(auth.access_token, deserialized.access_token);
        assert_eq!(auth.refresh_token, deserialized.refresh_token);
        assert_eq!(auth.user_address, deserialized.user_address);
        assert_eq!(auth.expires_at, deserialized.expires_at);
    }

    #[test]
    fn test_assertion_for_submission_serialization() {
        let assertion = AssertionForSubmission {
            assertion_contract: "test_contract".to_string(),
            assertion_id: "test_id".to_string(),
            signature: "test_signature".to_string(),
            constructor_args: vec!["arg1".to_string(), "arg2".to_string()],
        };

        let serialized = toml::to_string(&assertion).unwrap();
        let deserialized: AssertionForSubmission = toml::from_str(&serialized).unwrap();

        assert_eq!(
            assertion.assertion_contract,
            deserialized.assertion_contract
        );
        assert_eq!(assertion.assertion_id, deserialized.assertion_id);
        assert_eq!(assertion.signature, deserialized.signature);
    }

    #[test]
    fn test_ensure_writable_directory_success() {
        let (config_dir, _temp_dir) = setup_config_dir();
        assert!(CliConfig::ensure_writable_directory(&config_dir).is_ok());
    }

    #[test]
    fn test_ensure_writable_directory_readonly() {
        let (config_dir, _temp_dir) = setup_config_dir();
        create_readonly_dir(&config_dir).unwrap();

        let result = CliConfig::ensure_writable_directory(&config_dir);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Permission denied")
        );
    }

    #[test]
    fn test_ensure_writable_file_success() {
        let (config_dir, _temp_dir) = setup_config_dir();
        let config_file = config_dir.join(CONFIG_FILE);
        fs::create_dir_all(&config_dir).unwrap();
        fs::write(&config_file, "").unwrap();

        assert!(CliConfig::ensure_writable_file(&config_file).is_ok());
    }

    #[test]
    fn test_ensure_writable_file_readonly() {
        let (config_dir, _temp_dir) = setup_config_dir();
        let config_file = config_dir.join(CONFIG_FILE);
        fs::create_dir_all(&config_dir).unwrap();
        create_readonly_file(&config_file).unwrap();

        let result = CliConfig::ensure_writable_file(&config_file);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("read-only"));
    }

    #[test]
    fn test_ensure_writable_file_nonexistent() {
        let (config_dir, _temp_dir) = setup_config_dir();
        let config_file = config_dir.join(CONFIG_FILE);

        assert!(CliConfig::ensure_writable_file(&config_file).is_ok());
    }

    #[test]
    fn test_write_to_file_at_dir_permission_denied() {
        let (config_dir, _temp_dir) = setup_config_dir();
        create_readonly_dir(&config_dir).unwrap();

        let config = CliConfig::default();
        let result = config.write_to_file_at_dir(config_dir);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Permission denied")
        );
    }

    #[test]
    fn test_write_to_file_at_dir_readonly_file() {
        let (config_dir, _temp_dir) = setup_config_dir();
        let config_file = config_dir.join(CONFIG_FILE);
        fs::create_dir_all(&config_dir).unwrap();
        create_readonly_file(&config_file).unwrap();

        let config = CliConfig::default();
        let result = config.write_to_file_at_dir(config_dir);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("read-only"));
    }

    #[test]
    fn test_write_to_file_at_dir_success() {
        let (config_dir, _temp_dir) = setup_config_dir();
        let config = CliConfig::default();
        assert!(config.write_to_file_at_dir(config_dir).is_ok());
    }

    #[test]
    fn test_assertion_key_from_string() {
        let assertion_key_str = "assertion_name(arg1,arg2)".to_string();
        let assertion_key = AssertionKey::from(assertion_key_str.clone());

        assert_eq!(assertion_key.assertion_name, "assertion_name");
        println!("{:?}", assertion_key.constructor_args);
        assert_eq!(
            assertion_key.constructor_args,
            vec!["arg1".to_string(), "arg2".to_string()]
        );
        assert_eq!(assertion_key.to_string(), assertion_key_str);

        let assertion_key_str = "assertion_name()".to_string();
        let assertion_key = AssertionKey::from(assertion_key_str);
        assert_eq!(assertion_key.assertion_name, "assertion_name");
        assert_eq!(assertion_key.constructor_args, <Vec<String>>::new());

        let assertion_key_str = "assertion_name".to_string();

        let assertion_key = AssertionKey::from(assertion_key_str.clone());
        assert_eq!(assertion_key.assertion_name, "assertion_name");
        assert_eq!(assertion_key.constructor_args, <Vec<String>>::new());
        assert_eq!(assertion_key.to_string(), assertion_key_str);
    }
}
