use tokio::sync::{
    mpsc,
    oneshot,
};

/// `tokio` `mpsc` channel used to send requests to the DB.
pub type DbRequestSender = mpsc::UnboundedSender<DbRequest>;

/// Different operations that can be performed with the database.
#[derive(Debug, Clone)]
pub enum DbOperation {
    /// Return value by key
    Get(Vec<u8>),
    /// Inserts into Db
    Insert(Vec<u8>, Vec<u8>),
}

#[derive(Debug, Clone)]
pub enum DbResponse {
    /// Regular value response
    Value(Vec<u8>),
}

/// Contains a request for the database and a oneshot channel for a response.
#[derive(Debug)]
pub struct DbRequest {
    pub request: DbOperation,
    pub response: oneshot::Sender<Option<DbResponse>>,
}
