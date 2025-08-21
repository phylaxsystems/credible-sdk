//! # `indexer`
//!
//! Contains setup functions and types to initialize the assertion-executor indexer.

use assertion_executor::store::AssertionStore;

pub trait Indexer {
	type Error: std::error::Error;

	async fn run_indexer(assertion_store: AssertionStore) ->  Result<(), Self::Error>;
}

#[derive(thiserror::Error, Debug)]
pub enum IndexerError {
    #[error("Internal indexer error")]
	InternalError,
}

pub struct AeIndexer;

impl Indexer for AeIndexer {
	type Error = IndexerError;

	/// Runs the indexer 
	async fn run_indexer(assertion_store: AssertionStore) ->  Result<(), IndexerError> {
		unimplemented!()
	}
}
