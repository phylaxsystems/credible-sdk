//! `verifier-service`
//!
//! The verifier service is a microservice that verifies that assertions conform to the interface needed to be deployed and ran.
//! What this means in practice, is that the verifier service has an API that accepts assertion bytecode, and will return a success
//! if the assertion can be deployed and ran and error otherwise.
//!
//! The assertion can error for a few reasons on deployment. The assertion must be able to be put in the assertion store.
//! This means that the assertion:
//! - Must not error on deployment
//! - Must register triggers
//! - Must register assertion spec ***(TBA), spec not implemented yet***
//!
//! The core verification logic is provided by the `assertion-verification` crate,
//! which is also reused by `pcl test`.

pub mod rpc;

pub use assertion_verification::{
    VerificationResult,
    VerificationStatus,
    verify_assertion,
};
pub use rpc::{
    build_router,
    health,
    process_json_rpc,
};
