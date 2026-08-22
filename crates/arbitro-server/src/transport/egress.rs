//! Handing a frame to a client, without saying how it gets there.
//!
//! The drain used to do `write_tx.try_send(frame)` — push into a per-
//! connection mpsc that a separate writer task drained into the socket.
//! That channel costs a task wake per frame, and the wake is the expensive
//! part, not the send.
//!
//! `Egress` hides the difference. What it does NOT expose is the point: the
//! caller cannot tell whether the bytes went to the socket, into a channel,
//! or into a buffer. Putting an mpsc back in the middle is one more
//! implementation, not a change to any caller.
//!
//! ## The queue does not disappear, it moves
//!
//! A socket write can come back short. When the kernel's send buffer fills
//! because the client is not reading, `try_write` writes part of the frame
//! or none of it, and the remainder has to live somewhere. Under the mpsc
//! that somewhere was the channel; here it is `owed`.
//!
//! So this does not remove the need to bound a slow client — it moves where
//! that bound is read. `backlog()` is what the eviction path looks at, the
//! same job `drain_stall_evict_ms` already does against a full channel.
//! Redis makes the same trade and calls the bound
//! `client-output-buffer-limit`.
//!
//! ## Deliberately synchronous
//!
//! No `async` anywhere in the trait. An async signature would admit an
//! implementation that awaits the socket, and then every caller pays for a
//! case that only exists when the peer is slow. Same reasoning as
//! `StreamSink`: the fast path must not carry the shape of the slow one.

use std::collections::VecDeque;

use bytes::Bytes;
use tokio::sync::mpsc;

/// What happened to a frame.
pub enum Delivery {
    /// Fully on the wire.
    Sent,
    /// The socket would not take it all. `usize` is the total owed to this
    /// client, not the size of this frame — callers bound the client, not
    /// the message.
    Buffered(usize),
    /// The peer is gone. The caller retires its bindings.
    Dead,
}

/// One client's outbound path.
pub trait Egress {
    /// Hand over a frame. Never blocks, never awaits.
    fn send(&mut self, frame: Bytes) -> Delivery;

    /// Push whatever is owed toward the socket.
    ///
    /// Must be called by whoever owns the connection when it has a
    /// backlog — otherwise a client that stopped reading and then resumed
    /// would keep its owed bytes forever, with no new frame to carry them
    /// out. The drain calls this each cycle for connections with a
    /// backlog, which is why no writability task is needed.
    fn flush(&mut self) -> Delivery;

    /// Bytes owed to this client and not yet written.
    fn backlog(&self) -> usize;
}

/// Straight to the socket, from the thread that owns the connection.
///
/// Plain TCP only. TLS keeps its own record state and cannot be written
/// from a synchronous call, so those connections take [`QueuedEgress`] —
/// see `EgressPath`.
pub struct DirectEgress {
    w: tokio::net::tcp::OwnedWriteHalf,
    /// Frames the socket has not taken, oldest first. A partial write
    /// leaves its remainder at the front, which is why this is a deque of
    /// `Bytes` and not one buffer: `Bytes::advance` makes the remainder
    /// free, where a `Vec` would memmove on every partial write.
    owed: VecDeque<Bytes>,
    owed_bytes: usize,
}

impl DirectEgress {
    pub fn new(w: tokio::net::tcp::OwnedWriteHalf) -> Self {
        Self {
            w,
            owed: VecDeque::new(),
            owed_bytes: 0,
        }
    }

    /// Try to drain `owed`. Returns `Dead` on a real socket error.
    fn push_owed(&mut self) -> Delivery {
        while let Some(front) = self.owed.front_mut() {
            match self.w.try_write(front) {
                Ok(0) => return Delivery::Dead,
                Ok(n) if n == front.len() => {
                    self.owed_bytes -= n;
                    self.owed.pop_front();
                }
                Ok(n) => {
                    // Partial: keep the tail at the front of the queue.
                    let _ = front.split_to(n);
                    self.owed_bytes -= n;
                    return Delivery::Buffered(self.owed_bytes);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    return Delivery::Buffered(self.owed_bytes);
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(_) => return Delivery::Dead,
            }
        }
        Delivery::Sent
    }
}

impl Egress for DirectEgress {
    #[inline]
    fn send(&mut self, mut frame: Bytes) -> Delivery {
        // ORDER: anything already owed must go first. Writing this frame
        // ahead of it would interleave a later message before an earlier
        // one on the same connection — silent reordering, which no
        // consumer can detect and every consumer assumes cannot happen.
        if !self.owed.is_empty() {
            match self.push_owed() {
                Delivery::Dead => return Delivery::Dead,
                Delivery::Buffered(_) => {
                    self.owed_bytes += frame.len();
                    self.owed.push_back(frame);
                    return Delivery::Buffered(self.owed_bytes);
                }
                Delivery::Sent => {}
            }
        }

        loop {
            match self.w.try_write(&frame) {
                Ok(0) => return Delivery::Dead,
                Ok(n) if n == frame.len() => return Delivery::Sent,
                Ok(n) => {
                    let _ = frame.split_to(n);
                    self.owed_bytes += frame.len();
                    self.owed.push_back(frame);
                    return Delivery::Buffered(self.owed_bytes);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    self.owed_bytes += frame.len();
                    self.owed.push_back(frame);
                    return Delivery::Buffered(self.owed_bytes);
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(_) => return Delivery::Dead,
            }
        }
    }

    #[inline]
    fn flush(&mut self) -> Delivery {
        if self.owed.is_empty() {
            return Delivery::Sent;
        }
        self.push_owed()
    }

    #[inline]
    fn backlog(&self) -> usize {
        self.owed_bytes
    }
}

/// Through the per-connection mpsc and its writer task — the previous
/// model, kept because it is the only one TLS can use, and because it is
/// where an mpsc goes if one is wanted back in the middle.
pub struct QueuedEgress<'a> {
    tx: &'a mpsc::Sender<Bytes>,
}

impl<'a> QueuedEgress<'a> {
    #[inline]
    pub fn new(tx: &'a mpsc::Sender<Bytes>) -> Self {
        Self { tx }
    }
}

impl Egress for QueuedEgress<'_> {
    #[inline]
    fn send(&mut self, frame: Bytes) -> Delivery {
        match self.tx.try_send(frame) {
            Ok(()) => Delivery::Sent,
            // A full channel is this implementation's "the client is not
            // keeping up". Reported as `Buffered` with the capacity so the
            // eviction path reads one number regardless of implementation;
            // the frame itself is dropped, which is what `try_send` already
            // did before this seam existed.
            Err(mpsc::error::TrySendError::Full(_)) => Delivery::Buffered(self.tx.max_capacity()),
            Err(mpsc::error::TrySendError::Closed(_)) => Delivery::Dead,
        }
    }

    #[inline]
    fn flush(&mut self) -> Delivery {
        // The writer task owns the draining; there is nothing to push from
        // here. Not an error — `flush` is "make progress if you can".
        Delivery::Sent
    }

    #[inline]
    fn backlog(&self) -> usize {
        self.tx.max_capacity() - self.tx.capacity()
    }
}

/// Which path a connection uses. An enum rather than `Box<dyn Egress>`:
/// dynamic dispatch measured 6.58 ns against 1.29 ns for a direct call
/// (`arbitro-kit/benches/same_thread_handoff.rs`), and this is the per-frame
/// path.
pub enum EgressPath<'a> {
    Direct(&'a mut DirectEgress),
    Queued(QueuedEgress<'a>),
}

impl EgressPath<'_> {
    #[inline]
    pub fn send(&mut self, frame: Bytes) -> Delivery {
        match self {
            EgressPath::Direct(e) => e.send(frame),
            EgressPath::Queued(e) => e.send(frame),
        }
    }

    #[inline]
    pub fn flush(&mut self) -> Delivery {
        match self {
            EgressPath::Direct(e) => e.flush(),
            EgressPath::Queued(e) => e.flush(),
        }
    }

    #[inline]
    pub fn backlog(&self) -> usize {
        match self {
            EgressPath::Direct(e) => e.backlog(),
            EgressPath::Queued(e) => e.backlog(),
        }
    }
}
