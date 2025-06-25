#[derive(thiserror::Error, Debug, PartialEq)]
#[error("Not found")]
pub struct NotFoundError;
