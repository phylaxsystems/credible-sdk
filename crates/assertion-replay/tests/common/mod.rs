pub mod setup;

macro_rules! require_test_instance {
    ($test_name:expr) => {{ crate::common::setup::require_test_instance($test_name).await }};
}

pub(crate) use require_test_instance;
