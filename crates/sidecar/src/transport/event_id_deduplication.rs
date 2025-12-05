//! Circular buffer for event ID deduplication.
//!
//! Provides O(1) duplicate detection with FIFO eviction when capacity is reached.

use hashlink::LinkedHashMap;
use parking_lot::Mutex;
use std::sync::Arc;

/// A thread-safe circular buffer for tracking seen event IDs to detect duplicates.
/// Uses FIFO eviction when capacity is reached.
/// All operations are O(1).
#[derive(Debug)]
pub struct EventIdBuffer {
    inner: Arc<Mutex<EventIdBufferInner>>,
}

#[derive(Debug)]
struct EventIdBufferInner {
    /// Ordered map: maintains insertion order with O(1) operations.
    // Value is () since we only need the key ordering.
    seen: LinkedHashMap<u64, ()>,
    max_capacity: usize,
}

impl EventIdBuffer {
    /// Create a new event ID buffer with the specified capacity.
    ///
    /// # Panics
    /// Panics if `max_capacity` is 0.
    pub fn new(max_capacity: usize) -> Self {
        assert!(
            max_capacity > 0,
            "The maximum capacity must be greater than 0"
        );
        Self {
            inner: Arc::new(Mutex::new(EventIdBufferInner {
                seen: LinkedHashMap::with_capacity(max_capacity),
                max_capacity,
            })),
        }
    }

    /// Check if the event_id is a duplicate and insert it if not.
    ///
    /// Returns `true` if this event_id was already seen (duplicate).
    /// Returns `false` if this is a new event_id (inserted into buffer).
    ///
    /// If at capacity, the oldest event_id is evicted before inserting.
    #[inline]
    pub fn check_duplicate_and_insert(&self, event_id: u64) -> bool {
        let mut inner = self.inner.lock();

        // Fast path: check if already seen
        if inner.seen.contains_key(&event_id) {
            return true;
        }

        // Prune oldest if at capacity
        if inner.seen.len() >= inner.max_capacity {
            inner.seen.pop_front();
        }

        // Insert new event_id
        inner.seen.insert(event_id, ());
        false
    }

    /// Check if event_id was seen without inserting.
    #[inline]
    pub fn contains(&self, event_id: u64) -> bool {
        self.inner.lock().seen.contains_key(&event_id)
    }

    /// Returns the current number of tracked event IDs.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.inner.lock().seen.len()
    }

    /// Returns true if the buffer is empty.
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.inner.lock().seen.is_empty()
    }

    /// Returns the configured maximum capacity.
    #[cfg(test)]
    pub fn capacity(&self) -> usize {
        self.inner.lock().max_capacity
    }
}

impl Clone for EventIdBuffer {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_creates_empty_buffer() {
        let buffer = EventIdBuffer::new(10);
        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());
        assert_eq!(buffer.capacity(), 10);
    }

    #[test]
    #[should_panic(expected = "The maximum capacity must be greater than 0")]
    fn test_new_panics_on_zero_capacity() {
        EventIdBuffer::new(0);
    }

    #[test]
    fn test_insert_returns_false_for_new_event() {
        let buffer = EventIdBuffer::new(10);
        assert!(!buffer.check_duplicate_and_insert(1));
        assert_eq!(buffer.len(), 1);
    }

    #[test]
    fn test_insert_returns_true_for_duplicate() {
        let buffer = EventIdBuffer::new(10);
        assert!(!buffer.check_duplicate_and_insert(1));
        assert!(buffer.check_duplicate_and_insert(1));
        assert_eq!(buffer.len(), 1);
    }

    #[test]
    fn test_contains_without_insert() {
        let buffer = EventIdBuffer::new(10);
        assert!(!buffer.contains(1));
        buffer.check_duplicate_and_insert(1);
        assert!(buffer.contains(1));
    }

    #[test]
    fn test_fifo_eviction_at_capacity() {
        let buffer = EventIdBuffer::new(3);

        // Fill to capacity
        assert!(!buffer.check_duplicate_and_insert(1));
        assert!(!buffer.check_duplicate_and_insert(2));
        assert!(!buffer.check_duplicate_and_insert(3));
        assert_eq!(buffer.len(), 3);

        // Add one more - should evict 1
        assert!(!buffer.check_duplicate_and_insert(4));
        assert_eq!(buffer.len(), 3);

        // 1 should be evicted
        assert!(!buffer.contains(1));
        // 2, 3, 4 should still be present
        assert!(buffer.contains(2));
        assert!(buffer.contains(3));
        assert!(buffer.contains(4));
    }

    #[test]
    fn test_extensive_fifo_eviction() {
        let buffer = EventIdBuffer::new(2);

        for i in 1..=10 {
            assert!(!buffer.check_duplicate_and_insert(i));
        }

        assert_eq!(buffer.len(), 2);
        // Only last two should remain
        assert!(!buffer.contains(8));
        assert!(buffer.contains(9));
        assert!(buffer.contains(10));
    }

    #[test]
    fn test_capacity_one() {
        let buffer = EventIdBuffer::new(1);

        assert!(!buffer.check_duplicate_and_insert(1));
        assert_eq!(buffer.len(), 1);
        assert!(buffer.contains(1));

        assert!(!buffer.check_duplicate_and_insert(2));
        assert_eq!(buffer.len(), 1);
        assert!(!buffer.contains(1));
        assert!(buffer.contains(2));

        // Re-adding evicted ID should work
        assert!(!buffer.check_duplicate_and_insert(1));
        assert!(!buffer.contains(2));
        assert!(buffer.contains(1));
    }

    #[test]
    fn test_duplicate_does_not_reset_position() {
        let buffer = EventIdBuffer::new(3);

        assert!(!buffer.check_duplicate_and_insert(1));
        assert!(!buffer.check_duplicate_and_insert(2));
        assert!(!buffer.check_duplicate_and_insert(3));

        // Duplicate check on 1 - should NOT move it to back
        assert!(buffer.check_duplicate_and_insert(1));

        // Add 4 - should evict 1 (oldest)
        assert!(!buffer.check_duplicate_and_insert(4));

        assert!(!buffer.contains(1)); // 1 was evicted
        assert!(buffer.contains(2));
        assert!(buffer.contains(3));
        assert!(buffer.contains(4));
    }

    #[test]
    fn test_clone_shares_state() {
        let buffer1 = EventIdBuffer::new(10);
        let buffer2 = buffer1.clone();

        buffer1.check_duplicate_and_insert(1);
        assert!(buffer2.contains(1));
        assert_eq!(buffer2.len(), 1);

        buffer2.check_duplicate_and_insert(2);
        assert!(buffer1.contains(2));
        assert_eq!(buffer1.len(), 2);
    }

    #[test]
    fn test_thread_safety() {
        use std::thread;

        let buffer = EventIdBuffer::new(1000);
        let mut handles = vec![];

        // Spawn multiple threads inserting different ranges
        for t in 0..4 {
            let buf = buffer.clone();
            handles.push(thread::spawn(move || {
                for i in (t * 100)..((t + 1) * 100) {
                    buf.check_duplicate_and_insert(i);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // All 400 unique IDs should be present (capacity is 1000)
        assert_eq!(buffer.len(), 400);
    }

    #[test]
    fn test_o1_performance_at_scale() {
        let capacity = 10_000;
        let buffer = EventIdBuffer::new(capacity);

        // Fill to capacity
        for i in 0..capacity as u64 {
            buffer.check_duplicate_and_insert(i);
        }

        let start = std::time::Instant::now();

        // Add 1000 more (triggers 1000 FIFO evictions)
        for i in capacity as u64..(capacity as u64 + 1000) {
            buffer.check_duplicate_and_insert(i);
        }

        // Check 1000 duplicates
        for i in capacity as u64..(capacity as u64 + 1000) {
            assert!(buffer.check_duplicate_and_insert(i));
        }

        let elapsed = start.elapsed();

        assert!(
            elapsed.as_millis() < 50,
            "Operations took too long: {:?}ms - indicates non-O(1) behavior",
            elapsed.as_millis()
        );

        assert_eq!(buffer.len(), capacity);
    }
}
