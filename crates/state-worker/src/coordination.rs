use std::sync::{
    Arc,
    atomic::{
        AtomicU64,
        Ordering,
    },
};
use tokio::sync::Notify;

#[derive(Debug)]
pub struct FlushControl {
    permitted_flush_block: AtomicU64,
    committed_block: AtomicU64,
    notify: Notify,
}

impl FlushControl {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            permitted_flush_block: AtomicU64::new(0),
            committed_block: AtomicU64::new(0),
            notify: Notify::new(),
        })
    }

    pub fn allow_flush_to(&self, block_number: u64) {
        self.permitted_flush_block
            .fetch_max(Self::encode(block_number), Ordering::AcqRel);
        self.notify.notify_waiters();
    }

    #[must_use]
    pub fn permitted_flush_block(&self) -> Option<u64> {
        Self::decode(self.permitted_flush_block.load(Ordering::Acquire))
    }

    pub fn record_committed_block(&self, block_number: u64) {
        self.committed_block
            .fetch_max(Self::encode(block_number), Ordering::AcqRel);
        self.notify.notify_waiters();
    }

    #[must_use]
    pub fn committed_block(&self) -> Option<u64> {
        Self::decode(self.committed_block.load(Ordering::Acquire))
    }

    pub async fn wait_for_update(&self) {
        self.notify.notified().await;
    }

    pub fn notify_waiters(&self) {
        self.notify.notify_waiters();
    }

    #[inline]
    const fn encode(block_number: u64) -> u64 {
        block_number.saturating_add(1)
    }

    #[inline]
    const fn decode(value: u64) -> Option<u64> {
        if value == 0 { None } else { Some(value - 1) }
    }
}

#[cfg(test)]
mod tests {
    use super::FlushControl;

    #[test]
    fn flush_permission_is_monotonic() {
        let control = FlushControl::new();
        assert_eq!(control.permitted_flush_block(), None);

        control.allow_flush_to(4);
        control.allow_flush_to(2);
        control.allow_flush_to(9);

        assert_eq!(control.permitted_flush_block(), Some(9));
    }

    #[test]
    fn committed_head_is_monotonic() {
        let control = FlushControl::new();
        assert_eq!(control.committed_block(), None);

        control.record_committed_block(1);
        control.record_committed_block(0);
        control.record_committed_block(7);

        assert_eq!(control.committed_block(), Some(7));
    }
}
