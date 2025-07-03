pub mod fork_db;

pub mod multi_fork_db;
pub use multi_fork_db::MultiForkDb;

pub mod overlay;

mod error;
pub use error::NotFoundError;

pub use revm::database::{
    Database,
    DatabaseCommit,
    DatabaseRef,
};

pub trait PhDB: DatabaseRef + Sync + Send {}

impl<T> PhDB for T where T: DatabaseRef + Sync + Send {}
