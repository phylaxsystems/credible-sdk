#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::type_complexity)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::unused_async)]

mod deploy_contracts;
pub use deploy_contracts::{
    Contracts,
    deploy_contracts,
    get_anvil_deployer,
};

mod deploy_da;
pub mod node_protocol_mock_server;

pub use deploy_da::{
    assertion_src,
    deploy_test_da,
};
