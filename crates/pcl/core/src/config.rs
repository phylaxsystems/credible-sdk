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
    Serialize,
};

use std::{
    fmt,
    path::{
        Path,
        PathBuf,
    },
};
use uuid::Uuid;

/// Legacy directory name for storing PCL configuration (deprecated)
const LEGACY_CONFIG_DIR: &str = ".pcl";
/// Directory name for storing PCL configuration under XDG config home
const CONFIG_DIR_NAME: &str = "pcl";
/// Configuration file name
pub const CONFIG_FILE: &str = "config.toml";

/// Main configuration structure for PCL
///
/// This struct holds all the configuration data for the PCL tool,
/// including authentication details.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CliConfig {
    /// Optional authentication details
    pub auth: Option<UserAuth>,
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
            &cli_args
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
    fn write_to_file_at_dir(&self, config_dir: &PathBuf) -> Result<(), ConfigError> {
        // Ensure directory exists and is writable
        Self::ensure_writable_directory(config_dir)?;

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

    /// Gets the legacy configuration directory path (~/.pcl)
    ///
    /// # Returns
    /// * `PathBuf` - Path to the legacy config directory
    ///
    /// # Panics
    ///
    /// Will panic if it does not find the home directory
    fn get_legacy_config_dir() -> PathBuf {
        home_dir().unwrap().join(LEGACY_CONFIG_DIR)
    }

    /// Gets the default configuration directory path
    ///
    /// Uses XDG Base Directory Specification:
    /// - `$XDG_CONFIG_HOME/pcl` if `XDG_CONFIG_HOME` is set
    /// - `~/.config/pcl` otherwise
    ///
    /// # Returns
    /// * `PathBuf` - Path to the config directory
    ///
    /// # Panics
    ///
    /// Will panic if it does not find the home directory
    pub fn get_config_dir() -> PathBuf {
        std::env::var("XDG_CONFIG_HOME")
            .map_or_else(|_| home_dir().unwrap().join(".config"), PathBuf::from)
            .join(CONFIG_DIR_NAME)
    }

    /// Migrates configuration from the legacy location (`~/.pcl`) to the new
    /// XDG-compliant location (`~/.config/pcl` or `$XDG_CONFIG_HOME/pcl`)
    ///
    /// Migration only occurs if:
    /// - The legacy directory exists
    /// - The new directory does not exist
    ///
    /// # Returns
    /// * `Ok(true)` - Migration was performed
    /// * `Ok(false)` - No migration needed
    /// * `Err(ConfigError)` - Migration failed
    pub fn migrate_legacy_config() -> Result<bool, ConfigError> {
        let legacy_dir = Self::get_legacy_config_dir();
        let new_dir = Self::get_config_dir();

        // Only migrate if legacy exists and new doesn't
        if legacy_dir.exists() && !new_dir.exists() {
            // Create parent dirs if needed
            if let Some(parent) = new_dir.parent() {
                std::fs::create_dir_all(parent).map_err(ConfigError::WriteError)?;
            }
            // Move the directory
            std::fs::rename(&legacy_dir, &new_dir).map_err(ConfigError::WriteError)?;
            eprintln!(
                "{}: Migrated PCL config from {} to {}",
                "Warning".yellow().bold(),
                legacy_dir.display(),
                new_dir.display()
            );
            return Ok(true);
        }
        Ok(false)
    }

    /// Reads configuration from a specific directory
    ///
    /// # Arguments
    /// * `config_dir` - Directory to read the config file from
    ///
    /// # Returns
    /// * `Result<Self, ConfigError>` - Configuration or error
    fn read_from_file_at_dir(config_dir: &Path) -> Result<Self, ConfigError> {
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
    /// If using the default config directory, this will first attempt to migrate
    /// any existing configuration from the legacy location (~/.pcl) to the new
    /// XDG-compliant location.
    ///
    /// # Arguments
    /// * `cli_args` - Command line arguments
    ///
    /// # Returns
    /// * `Result<Self, ConfigError>` - Configuration or error
    pub fn read_from_file(cli_args: &CliArgs) -> Result<Self, ConfigError> {
        // Only attempt migration when using default config dir
        if cli_args.config_dir.is_none() {
            // Attempt migration from legacy location (errors are non-fatal)
            let _ = Self::migrate_legacy_config();
        }

        Self::read_from_file_at_dir(
            &cli_args
                .config_dir
                .clone()
                .unwrap_or(Self::get_config_dir()),
        )
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
    /// Token expiration timestamp
    #[serde(with = "chrono::serde::ts_seconds")]
    pub expires_at: DateTime<Utc>,
    /// Platform user ID (UUID), used for API calls that require it
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<Uuid>,
    /// Ethereum address of the user (only present for wallet-based auth)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wallet_address: Option<Address>,
    /// Email address of the user (for email-based auth)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
}

impl UserAuth {
    /// Returns the best available display name for this user.
    pub fn display_name(&self) -> String {
        if let Some(addr) = &self.wallet_address {
            if *addr != Address::ZERO {
                return addr.to_string();
            }
        }
        if let Some(email) = &self.email {
            return email.clone();
        }
        if let Some(id) = &self.user_id {
            return id.to_string();
        }
        "unknown".to_string()
    }
}

impl fmt::Display for UserAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Authentication:")?;
        writeln!(f, "  User: {}", self.display_name())?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        env,
        fs,
        os::unix::fs::PermissionsExt,
    };
    use tempfile::TempDir;

    /// Helper function to set up a temporary config directory
    fn setup_config_dir() -> (PathBuf, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        unsafe {
            env::set_var("HOME", temp_dir.path());
            // Clear XDG_CONFIG_HOME to ensure we use ~/.config
            env::remove_var("XDG_CONFIG_HOME");
        }
        (
            temp_dir.path().join(".config").join(CONFIG_DIR_NAME),
            temp_dir,
        )
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
                expires_at: fixed_timestamp,
                user_id: None,
                wallet_address: None,
                email: None,
            }),
        };

        // Test writing
        config.write_to_file_at_dir(&config_dir).unwrap();

        // Test reading
        let read_config = CliConfig::read_from_file_at_dir(&config_dir).unwrap();
        assert_eq!(
            read_config.auth.as_ref().unwrap().access_token,
            "test_access"
        );
        assert_eq!(
            read_config.auth.as_ref().unwrap().refresh_token,
            "test_refresh"
        );
        assert!(read_config.auth.as_ref().unwrap().wallet_address.is_none());

        // Test display format - check for key content rather than exact match due to color codes
        let formatted_cfg = format!("{read_config}");

        // Check that all the important information is present
        assert!(formatted_cfg.contains("PCL Configuration"));
        assert!(formatted_cfg.contains("Config path:"));
        assert!(formatted_cfg.contains("pcl/config.toml"));
        assert!(formatted_cfg.contains("User: unknown"));
        assert!(formatted_cfg.contains("2022-12-31 16:00:00 UTC"));
        assert!(formatted_cfg.contains("Access Token: [Set]"));
        assert!(formatted_cfg.contains("Refresh Token: [Set]"));
    }

    #[test]
    fn test_read_nonexistent_config() {
        let (config_dir, _temp_dir) = setup_config_dir();

        // Try reading without creating a file
        let config = CliConfig::read_from_file_at_dir(&config_dir).unwrap();
        assert!(config.auth.is_none());
    }

    #[test]
    fn test_user_auth_display() {
        let auth = UserAuth {
            access_token: "test_access".to_string(),
            refresh_token: "test_refresh".to_string(),
            expires_at: DateTime::from_timestamp(1672502400, 0).unwrap(), // 2022-12-31 16:00:00 UTC
            user_id: None,
            wallet_address: None,
            email: Some("test@example.com".to_string()),
        };

        let display = format!("{auth}");
        assert!(display.contains("User: test@example.com"));
        assert!(display.contains("Token Expired at"));
        assert!(display.contains("Access Token: [Set]"));
        assert!(display.contains("Refresh Token: [Set]"));
    }

    #[test]
    fn test_display_name_priority() {
        let expires = DateTime::from_timestamp(0, 0).unwrap();

        // Non-zero wallet address takes priority over everything
        let with_addr = UserAuth {
            access_token: String::new(),
            refresh_token: String::new(),
            expires_at: expires,
            wallet_address: Some(Address::from_slice(&[1; 20])),
            email: Some("test@example.com".to_string()),
            user_id: Some(Uuid::nil()),
        };
        assert_eq!(
            with_addr.display_name(),
            "0x0101010101010101010101010101010101010101"
        );

        // Email is next priority when no wallet address
        let with_email = UserAuth {
            access_token: String::new(),
            refresh_token: String::new(),
            expires_at: expires,
            wallet_address: None,
            email: Some("test@example.com".to_string()),
            user_id: Some(Uuid::nil()),
        };
        assert_eq!(with_email.display_name(), "test@example.com");

        // User ID is fallback when no address or email
        let with_id = UserAuth {
            access_token: String::new(),
            refresh_token: String::new(),
            expires_at: expires,
            wallet_address: None,
            email: None,
            user_id: Some(Uuid::nil()),
        };
        assert_eq!(
            with_id.display_name(),
            "00000000-0000-0000-0000-000000000000"
        );

        // "unknown" when nothing is set
        let bare = UserAuth {
            access_token: String::new(),
            refresh_token: String::new(),
            expires_at: expires,
            wallet_address: None,
            email: None,
            user_id: None,
        };
        assert_eq!(bare.display_name(), "unknown");
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
                expires_at: DateTime::from_timestamp(1672502400, 0).unwrap(),
                user_id: None,
                wallet_address: None,
                email: None,
            }),
        };
        let args = ConfigArgs {
            command: ConfigCommand::Delete,
        };
        assert!(args.run(&mut config).is_ok());
        assert!(config.auth.is_none());
    }

    #[test]
    fn test_write_to_file_permission_error() {
        let temp_dir = tempfile::tempdir().unwrap();

        // Create a read-only directory
        let mut perms = std::fs::metadata(&temp_dir).unwrap().permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&temp_dir, perms).unwrap();

        let config = CliConfig::default();
        let result = config.write_to_file_at_dir(&temp_dir.path().to_path_buf());

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

        let result = CliConfig::read_from_file_at_dir(&config_dir);
        assert!(result.is_err());
    }

    #[test]
    fn test_display_empty_config() {
        let config = CliConfig::default();
        let display = format!("{config}");
        assert!(display.contains("Not authenticated"));
    }

    #[test]
    fn test_user_auth_serialization() {
        let auth = UserAuth {
            access_token: "test_access".to_string(),
            refresh_token: "test_refresh".to_string(),
            expires_at: DateTime::from_timestamp(1672502400, 0).unwrap(),
            user_id: Some(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()),
            wallet_address: Some(Address::from_slice(&[0; 20])),
            email: Some("test@example.com".to_string()),
        };

        let serialized = toml::to_string(&auth).unwrap();
        let deserialized: UserAuth = toml::from_str(&serialized).unwrap();

        assert_eq!(auth.access_token, deserialized.access_token);
        assert_eq!(auth.refresh_token, deserialized.refresh_token);
        assert_eq!(auth.wallet_address, deserialized.wallet_address);
        assert_eq!(auth.expires_at, deserialized.expires_at);
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
        let result = config.write_to_file_at_dir(&config_dir);
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
        let result = config.write_to_file_at_dir(&config_dir);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("read-only"));
    }

    #[test]
    fn test_write_to_file_at_dir_success() {
        let (config_dir, _temp_dir) = setup_config_dir();
        let config = CliConfig::default();
        assert!(config.write_to_file_at_dir(&config_dir).is_ok());
    }
}
