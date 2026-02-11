//! MDBX fixture for integration tests

use std::path::PathBuf;

/// A temporary directory for MDBX database files.
pub struct MdbxTestDir {
    pub path: PathBuf,
}

impl MdbxTestDir {
    pub fn new() -> Result<Self, String> {
        let base_path = std::env::current_dir()
            .map_err(|e| format!("Failed to get current dir: {e}"))?
            .join("target")
            .join("mdbx-test-dbs");

        std::fs::create_dir_all(&base_path)
            .map_err(|e| format!("Failed to create base test directory: {e}"))?;

        let unique_id = uuid::Uuid::new_v4();
        let path = base_path.join(unique_id.to_string());

        std::fs::create_dir_all(&path)
            .map_err(|e| format!("Failed to create MDBX test directory: {e}"))?;

        Ok(Self { path })
    }

    pub fn path_str(&self) -> Result<&str, String> {
        self.path
            .to_str()
            .ok_or_else(|| "Invalid UTF-8 in path".to_string())
    }
}

impl Drop for MdbxTestDir {
    fn drop(&mut self) {
        if self.path.exists() {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}
