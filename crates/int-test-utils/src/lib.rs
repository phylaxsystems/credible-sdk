#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::missing_errors_doc)]

mod deploy_contracts;
pub use deploy_contracts::{
    Contracts,
    deploy_contracts,
    get_anvil_deployer,
};

mod deploy_da;
pub use deploy_da::{
    assertion_src,
    deploy_test_da,
};
