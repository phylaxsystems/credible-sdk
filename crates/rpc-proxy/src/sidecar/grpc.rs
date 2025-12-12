use async_trait::async_trait;
use futures::StreamExt;
use tonic::transport::{
    Channel,
    Endpoint,
};
use url::Url;

use crate::{
    error::{
        ProxyError,
        Result,
    },
    fingerprint::{
        AssertionInfo,
        Fingerprint,
    },
};

use super::{
    InvalidationEvent,
    InvalidationStream,
    ShouldForwardVerdict,
    SidecarTransport,
};

mod pb {
    tonic::include_proto!("rpcproxy.v1");
}

use alloy_primitives::{
    Address,
    B256,
};
use pb::{
    Assertion as PbAssertion,
    Empty,
    Fingerprint as PbFingerprint,
    rpc_proxy_heuristics_client::RpcProxyHeuristicsClient,
};

#[derive(Clone, Debug)]
pub struct GrpcSidecarTransport {
    endpoint: Endpoint,
}

impl GrpcSidecarTransport {
    pub fn new(url: Url) -> Result<Self> {
        let endpoint = Endpoint::from_shared(url.to_string()).map_err(|err| {
            ProxyError::SidecarTransport(format!("invalid sidecar endpoint: {err}"))
        })?;
        Ok(Self { endpoint })
    }

    async fn client(&self) -> Result<RpcProxyHeuristicsClient<Channel>> {
        let channel = self.endpoint.clone().connect().await.map_err(|err| {
            ProxyError::SidecarTransport(format!("failed to connect to sidecar: {err}"))
        })?;
        Ok(RpcProxyHeuristicsClient::new(channel))
    }

    fn convert_fingerprint(pb: PbFingerprint) -> Result<Fingerprint> {
        let target = Address::try_from(pb.target.as_slice())
            .map_err(|_| ProxyError::SidecarTransport("invalid fingerprint target".into()))?;
        if pb.selector.len() != 4 {
            return Err(ProxyError::SidecarTransport(
                "fingerprint selector must be 4 bytes".into(),
            ));
        }
        if pb.arg_hash16.len() != 16 {
            return Err(ProxyError::SidecarTransport(
                "fingerprint arg hash must be 16 bytes".into(),
            ));
        }
        let hash = B256::try_from(pb.hash.as_slice())
            .map_err(|_| ProxyError::SidecarTransport("invalid fingerprint hash".into()))?;
        let mut selector = [0u8; 4];
        selector.copy_from_slice(&pb.selector);
        let mut arg_hash = [0u8; 16];
        arg_hash.copy_from_slice(&pb.arg_hash16);
        Ok(Fingerprint::from_parts(
            hash,
            target,
            selector,
            arg_hash,
            pb.value_bucket,
            pb.gas_bucket,
        ))
    }

    fn convert_assertion(pb: Option<PbAssertion>) -> Result<AssertionInfo> {
        let assertion =
            pb.ok_or_else(|| ProxyError::SidecarTransport("missing assertion in response".into()))?;
        let id = B256::try_from(assertion.assertion_id.as_slice())
            .map_err(|_| ProxyError::SidecarTransport("invalid assertion id".into()))?;
        Ok(AssertionInfo {
            assertion_id: id,
            assertion_version: assertion.assertion_version,
        })
    }
}

#[async_trait]
impl SidecarTransport for GrpcSidecarTransport {
    async fn subscribe_invalidations(&self) -> Result<InvalidationStream> {
        let mut client = self.client().await?;
        let response = client
            .stream_invalidations(tonic::Request::new(Empty {}))
            .await
            .map_err(|err| {
                ProxyError::SidecarTransport(format!("stream invalidations failed: {err}"))
            })?;

        let stream = response
            .into_inner()
            .map(|msg| -> Result<InvalidationEvent> {
                let pb = msg.map_err(|status| {
                    ProxyError::SidecarTransport(format!("sidecar stream error: {status}"))
                })?;
                let fingerprint_pb = pb
                    .fingerprint
                    .ok_or_else(|| ProxyError::SidecarTransport("missing fingerprint".into()))?;
                let fingerprint = Self::convert_fingerprint(fingerprint_pb)?;
                let assertion = Self::convert_assertion(pb.assertion)?;
                Ok(InvalidationEvent {
                    fingerprint,
                    assertion,
                })
            });

        Ok(Box::pin(stream))
    }

    async fn should_forward(&self, fingerprint: &Fingerprint) -> Result<ShouldForwardVerdict> {
        let mut client = self.client().await?;
        let request = tonic::Request::new(pb::ShouldForwardRequest {
            fingerprint: Some(pb::Fingerprint {
                hash: fingerprint.hash.as_slice().to_vec(),
                target: fingerprint.target.as_slice().to_vec(),
                selector: fingerprint.selector.to_vec(),
                arg_hash16: fingerprint.arg_hash.to_vec(),
                value_bucket: fingerprint.value_bucket,
                gas_bucket: fingerprint.gas_bucket,
            }),
        });

        let response = client
            .should_forward(request)
            .await
            .map_err(|err| ProxyError::SidecarTransport(format!("should_forward failed: {err}")))?
            .into_inner();

        let verdict = match response.verdict() {
            pb::should_forward_response::Verdict::Unknown => ShouldForwardVerdict::Unknown,
            pb::should_forward_response::Verdict::Allow => ShouldForwardVerdict::Allow,
            pb::should_forward_response::Verdict::Deny => {
                let assertion = Self::convert_assertion(response.assertion)?;
                ShouldForwardVerdict::Deny(assertion)
            }
        };

        Ok(verdict)
    }
}
