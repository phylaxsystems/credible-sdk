pub mod setup;

macro_rules! require_test_instance {
    ($test_name:expr) => {{
        match crate::common::setup::TestInstance::try_new().await {
            Ok(instance) => Some(instance),
            Err(error) => {
                if error.contains("Operation not permitted") {
                    eprintln!(
                        "{}: skipped due to sandbox socket restrictions ({})",
                        $test_name, error
                    );
                    None
                } else {
                    panic!(
                        "{}: failed to start integration test instance: {}",
                        $test_name, error
                    );
                }
            }
        }
    }};
}

pub(crate) use require_test_instance;
