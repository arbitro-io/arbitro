//! Releasing acks, without saying how the command gets to the shard.
//!
//! Two wirings, both named, both kept:
//!
//! - [`QueuedCommands`] — through the shard's mpsc and its worker task.
//!   The original path, extracted rather than deleted. It is the only one
//!   that works when the caller is genuinely on another thread, and it is
//!   where an mpsc goes if one is wanted back in the middle.
//! - [`DirectCommands`] — straight into the shard's state, on the thread
//!   that owns it. No channel, no task wake.
//!
//! [`CommandPath`] picks. No call site knows which it got.
//!
//! ## What the measurements actually say
//!
//! The channel costs per COMMAND, not per ack — and `BatchAck` coalesces up
//! to 256 acks into one command, so on a batching client the 92.5 ns of
//! ceremony spreads to ~0.36 ns/ack and vanishes. Measured end to end
//! (`arbitro-e2e/benches/ack_affinity.rs`): steered against bootstrap came
//! out 1.00x, and 0.95x again with the settled-guard disabled. No gain
//! either way.
//!
//! That is worth stating plainly because the microbenchmark suggested
//! otherwise. `same_thread_handoff` modelled ONE command per ack, which the
//! broker does not do; it measured ceremony isolated from framing, network
//! and batching, and extrapolating from it overstated the prize.
//!
//! Where the channel does cost per unit is a client that acks one message
//! at a time without coalescing — there the command IS the ack and the
//! channel is paid in full. That is what `DirectCommands` is for, and why
//! both wirings stay.

use arbitro_engine_v2::types::{ConnectionId, ConsumerId};
use arbitro_engine_v2::AckEntry;

/// What a release actually did.
pub struct Released {
    pub accepted: u32,
    pub rejected: u32,
}

/// Commands a connection issues against one shard.
///
/// `None` means **not yet known**, never failure: the queued wiring cannot
/// answer without waiting, and waiting is what it exists to avoid. Encoding
/// "unknown" as `Released { accepted: 0, rejected: 0 }` would be a lie a
/// caller could act on.
#[allow(async_fn_in_trait)]
pub trait Commands {
    async fn release(
        &self,
        consumer: ConsumerId,
        conn: ConnectionId,
        entries: Vec<AckEntry>,
    ) -> Option<Released>;

    async fn requeue(
        &self,
        consumer: ConsumerId,
        conn: ConnectionId,
        entries: Vec<AckEntry>,
        delay_ms: u32,
    ) -> Option<Released>;
}

/// Through the shard's mpsc — the wiring that crosses threads.
pub struct QueuedCommands<'a> {
    handle: &'a crate::shard::handle::ShardHandle,
}

impl<'a> QueuedCommands<'a> {
    #[inline]
    pub fn new(handle: &'a crate::shard::handle::ShardHandle) -> Self {
        Self { handle }
    }
}

impl Commands for QueuedCommands<'_> {
    #[inline]
    async fn release(
        &self,
        consumer: ConsumerId,
        conn: ConnectionId,
        entries: Vec<AckEntry>,
    ) -> Option<Released> {
        let _ = self.handle.ack(consumer, conn.0, entries).await;
        None
    }

    #[inline]
    async fn requeue(
        &self,
        consumer: ConsumerId,
        conn: ConnectionId,
        entries: Vec<AckEntry>,
        delay_ms: u32,
    ) -> Option<Released> {
        let _ = self.handle.nack(consumer, conn.0, entries, delay_ms).await;
        None
    }
}

/// Straight into the shard's state, from the thread that owns it.
///
/// Carries the handle anyway, and that is not redundancy. Owning the shard
/// is settled at accept time; whether the worker is REACHABLE is settled
/// moment to moment, because the run loop publishes it only while parked.
/// A direct call arriving mid-command finds nothing — and without the
/// fallback that ack would be dropped in silence: the client counts it
/// sent, the broker never releases the pending entry, and the message comes
/// back later with nothing in any log to explain it.
pub struct DirectCommands<'a> {
    fallback: &'a crate::shard::handle::ShardHandle,
}

impl<'a> DirectCommands<'a> {
    #[inline]
    pub fn new(fallback: &'a crate::shard::handle::ShardHandle) -> Self {
        Self { fallback }
    }
}

impl Commands for DirectCommands<'_> {
    #[inline]
    async fn release(
        &self,
        consumer: ConsumerId,
        conn: ConnectionId,
        entries: Vec<AckEntry>,
    ) -> Option<Released> {
        // The closure runs only if the worker is here, so `owned` still
        // holds the entries when it is not — nothing is lost either way.
        let mut owned = Some(entries);
        if let Some(r) = crate::shard::local::with_worker(
            |w: &mut crate::shard::CommandWorker| {
                w.release_direct(consumer, conn, owned.take().unwrap())
            },
        ) {
            return r;
        }
        QueuedCommands::new(self.fallback)
            .release(consumer, conn, owned.take().unwrap())
            .await
    }

    #[inline]
    async fn requeue(
        &self,
        consumer: ConsumerId,
        conn: ConnectionId,
        entries: Vec<AckEntry>,
        delay_ms: u32,
    ) -> Option<Released> {
        let mut owned = Some(entries);
        if let Some(r) = crate::shard::local::with_worker(
            |w: &mut crate::shard::CommandWorker| {
                w.requeue_direct(consumer, conn, owned.take().unwrap(), delay_ms)
            },
        ) {
            return r;
        }
        QueuedCommands::new(self.fallback)
            .requeue(consumer, conn, owned.take().unwrap(), delay_ms)
            .await
    }
}

/// Which wiring a caller got. An enum, not `Box<dyn Commands>`: dynamic
/// dispatch measured 6.58 ns against 1.29 ns for a direct call
/// (`arbitro-kit/benches/same_thread_handoff.rs`), and this is per command.
pub enum CommandPath<'a> {
    Direct(DirectCommands<'a>),
    Queued(QueuedCommands<'a>),
}

impl CommandPath<'_> {
    #[inline]
    pub async fn release(
        &self,
        consumer: ConsumerId,
        conn: ConnectionId,
        entries: Vec<AckEntry>,
    ) -> Option<Released> {
        match self {
            CommandPath::Direct(c) => c.release(consumer, conn, entries).await,
            CommandPath::Queued(c) => c.release(consumer, conn, entries).await,
        }
    }

    #[inline]
    pub async fn requeue(
        &self,
        consumer: ConsumerId,
        conn: ConnectionId,
        entries: Vec<AckEntry>,
        delay_ms: u32,
    ) -> Option<Released> {
        match self {
            CommandPath::Direct(c) => c.requeue(consumer, conn, entries, delay_ms).await,
            CommandPath::Queued(c) => c.requeue(consumer, conn, entries, delay_ms).await,
        }
    }
}
