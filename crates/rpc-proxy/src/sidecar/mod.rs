use std::{
    pin::Pin,
    sync::Arc,
};

use async_trait::async_trait;
use futures::Stream;

use crate::{
    error::Result,
    fingerprint::{
        AssertionInfo,
        Fingerprint,
    },
};

pub mod grpc;
pub mod noop;

pub use grpc::GrpcSidecarTransport;
pub use noop::NoopSidecarTransport;

#[derive(Debug, Clone)]
pub struct InvalidationEvent {
    pub fingerprint: Fingerprint,
    pub assertion: AssertionInfo,
}

#[derive(Debug, Clone)]
pub enum ShouldForwardVerdict {
    Unknown,
    Allow,
    Deny(AssertionInfo),
}

pub type InvalidationStream = Pin<Box<dyn Stream<Item = Result<InvalidationEvent>> + Send>>;

#[async_trait]
pub trait SidecarTransport: Send + Sync {
    async fn subscribe_invalidations(&self) -> Result<InvalidationStream>;
    async fn should_forward(&self, fingerprint: &Fingerprint) -> Result<ShouldForwardVerdict>;
}

pub type SharedSidecarTransport = Arc<dyn SidecarTransport>;
