use crate::state_sync::{
    mdbx_runtime,
    supervisor::TestSupervisor,
};
use std::sync::Arc;

#[test]
fn test_shared_mdbx_runtime_initializes_once() {
    let first = mdbx_runtime::init("/tmp/state.mdbx").unwrap();
    let second = mdbx_runtime::init("/tmp/state.mdbx").unwrap();

    assert!(Arc::ptr_eq(&first, &second));
}

#[test]
fn test_supervisor_restarts_worker_after_panic() {
    let supervisor = TestSupervisor::spawn_with_panicking_worker();
    supervisor.wait_for_restart_count(1);

    assert!(supervisor.status().healthy);
}
