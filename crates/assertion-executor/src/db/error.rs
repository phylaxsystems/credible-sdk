use revm::context::DBErrorMarker;

#[derive(thiserror::Error, Debug, PartialEq)]
#[error("Not found")]
pub struct NotFoundError;

impl DBErrorMarker for NotFoundError {}
