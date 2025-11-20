pub mod fork_db;

pub mod multi_fork_db;
pub use multi_fork_db::MultiForkDb;

pub mod overlay;

pub mod version_db;
pub use version_db::{
    VersionDb,
    VersionDbError,
};

mod error;
pub use error::NotFoundError;

pub use revm::database::{
    Database,
    DatabaseCommit,
    DatabaseRef,
};

pub trait PhDB: DatabaseRef + Sync + Send {}

impl<T> PhDB for T where T: DatabaseRef + Sync + Send {}

/// Marks a database as having features that allow us to roll back state
/// to specific points.
pub trait RollbackDb {
    type Err: std::error::Error;

    /// Roll back to a specific commit depth (0-based).
    /// Depth of 0 resets to the base snapshot.
    fn rollback_to(&mut self, depth: usize) -> Result<(), Self::Err>;
    /// Drops the changelog while keeping the latest state as the new base
    fn collapse_log(&mut self);
    fn depth(&self) -> usize;
}
