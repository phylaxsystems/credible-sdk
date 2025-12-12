use async_trait::async_trait;
use futures::stream;

use crate::{
    error::Result,
    fingerprint::Fingerprint,
};

use super::{
    InvalidationStream,
    ShouldForwardVerdict,
    SidecarTransport,
};

#[derive(Debug, Default)]
pub struct NoopSidecarTransport;

#[async_trait]
impl SidecarTransport for NoopSidecarTransport {
    async fn subscribe_invalidations(&self) -> Result<InvalidationStream> {
        Ok(Box::pin(stream::empty()))
    }

    async fn should_forward(&self, _fingerprint: &Fingerprint) -> Result<ShouldForwardVerdict> {
        Ok(ShouldForwardVerdict::Unknown)
    }
}
