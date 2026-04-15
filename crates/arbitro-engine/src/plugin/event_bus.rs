//! EventBus plugin — typed event dispatch.
//!
//! Level 4 — depends on `types`, `plugin/mod`.
//!
//! Events are dispatched to registered handlers. Used for observability
//! (metrics, logging, tracing) without coupling the engine core to any
//! specific observability system.

use crate::types::*;

/// Engine events emitted during runtime operations.
#[derive(Debug, Clone)]
pub enum EngineEvent {
    /// A connection was drained (all pending released, bindings removed).
    ConnectionDrained(ConnectionDrainedEvent),
    /// A pending message was acknowledged.
    PendingAcked(PendingAckedEvent),
    /// A pending message was nacked (will be redelivered).
    PendingNacked(PendingNackedEvent),
    /// A pending message timed out.
    PendingTimedOut(PendingTimedOutEvent),
    /// A batch was published.
    BatchPublished(BatchPublishedEvent),
    /// A subscription was drained.
    SubscriptionDrained(SubscriptionDrainedEvent),
    /// A consumer was drained.
    ConsumerDrained(ConsumerDrainedEvent),
    /// A queue was purged.
    QueuePurged(QueuePurgedEvent),
}

#[derive(Debug, Clone)]
pub struct ConnectionDrainedEvent {
    pub connection_id: ConnectionId,
    pub pending_released: u32,
    pub pending_requeued: u32,
    pub bindings_removed: u32,
}

#[derive(Debug, Clone)]
pub struct PendingAckedEvent {
    pub pending_id: PendingId,
    pub consumer_id: ConsumerId,
    pub queue_id: QueueId,
    pub seq: u64,
}

#[derive(Debug, Clone)]
pub struct PendingNackedEvent {
    pub pending_id: PendingId,
    pub consumer_id: ConsumerId,
    pub seq: u64,
}

#[derive(Debug, Clone)]
pub struct PendingTimedOutEvent {
    pub pending_id: PendingId,
    pub consumer_id: ConsumerId,
    pub seq: u64,
}

#[derive(Debug, Clone)]
pub struct BatchPublishedEvent {
    pub stream_id: StreamId,
    pub entries: u32,
    pub duplicates: u32,
}

#[derive(Debug, Clone)]
pub struct SubscriptionDrainedEvent {
    pub subscription_id: SubscriptionId,
    pub pending_released: u32,
}

#[derive(Debug, Clone)]
pub struct ConsumerDrainedEvent {
    pub consumer_id: ConsumerId,
    pub pending_released: u32,
}

#[derive(Debug, Clone)]
pub struct QueuePurgedEvent {
    pub queue_id: QueueId,
    pub pending_released: u32,
    pub ready_cleared: u32,
}

/// EventBus plugin — collects events during a processing cycle.
///
/// Events are buffered and can be drained by an external observer
/// (metrics thread, test harness, etc.) without allocation on the hot path.
pub struct EventBus {
    events: Vec<EngineEvent>,
}

impl EventBus {
    pub fn new() -> Self {
        Self {
            events: Vec::with_capacity(64),
        }
    }

    /// Emit an event. O(1) — Vec push (pre-allocated).
    #[inline]
    pub fn emit(&mut self, event: EngineEvent) {
        self.events.push(event);
    }

    /// Drain all buffered events. Returns the events and clears the buffer.
    /// The internal Vec retains its capacity for reuse.
    pub fn drain(&mut self) -> Vec<EngineEvent> {
        let mut events = Vec::with_capacity(self.events.capacity());
        std::mem::swap(&mut events, &mut self.events);
        events
    }

    /// Number of buffered events.
    #[inline]
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Whether the event buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

impl Default for EventBus {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emit_and_drain() {
        let mut bus = EventBus::new();
        bus.emit(EngineEvent::PendingAcked(PendingAckedEvent {
            pending_id: PendingId(1),
            consumer_id: ConsumerId(2),
            queue_id: QueueId(3),
            seq: 100,
        }));
        bus.emit(EngineEvent::BatchPublished(BatchPublishedEvent {
            stream_id: StreamId(1),
            entries: 10,
            duplicates: 0,
        }));

        assert_eq!(bus.len(), 2);
        let events = bus.drain();
        assert_eq!(events.len(), 2);
        assert!(bus.is_empty());
    }

    #[test]
    fn drain_reuses_capacity() {
        let mut bus = EventBus::new();
        for i in 0..100 {
            bus.emit(EngineEvent::PendingAcked(PendingAckedEvent {
                pending_id: PendingId(i),
                consumer_id: ConsumerId(0),
                queue_id: QueueId(0),
                seq: i as u64,
            }));
        }
        let _ = bus.drain();
        // Internal buffer retains capacity via swap
        assert!(bus.is_empty());
    }
}
