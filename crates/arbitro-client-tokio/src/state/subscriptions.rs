//! Routing registry: `subscription_id → route`, plus the consumer each
//! subscription hangs off.
//!
//! A subscription is the leaf: it knows its consumer and its stream, and
//! nothing knows it back. Deliveries are keyed by subscription because
//! several filtered subscriptions may share one consumer, and a
//! consumer-keyed table can only hold one of them.
//!
//! Removal cascades downward and is the registry's job, never the
//! caller's: dropping a consumer drops its subscriptions and their route
//! entries; dropping the last subscription of a consumer drops the
//! consumer. No caller ever has to sequence a teardown.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::ackstore::SlotRef;
use crate::consume::message::Message;

type Map<K, V> = HashMap<K, V, foldhash::fast::FixedState>;

/// Delivery-channel capacity, per subscription.
const CHANNEL_CAP: usize = 4096;

/// Subject filter, classified once at subscribe time so the delivery path
/// never parses a pattern it could have parsed earlier.
enum Matcher {
    /// No filter, or `>`. Accepts every subject.
    All,
    /// Literal, no wildcards. Slice equality compares length first.
    Exact(Box<[u8]>),
    /// Contains `*` or `>`. The only case that walks tokens.
    Pattern(Box<[u8]>),
}

impl Matcher {
    fn new(filter: &[u8]) -> Self {
        if filter.is_empty() || filter == b">" {
            return Matcher::All;
        }
        if filter.contains(&b'*') || filter.contains(&b'>') {
            return Matcher::Pattern(filter.into());
        }
        Matcher::Exact(filter.into())
    }

    #[inline]
    fn accepts(&self, subject: &[u8]) -> bool {
        match self {
            Matcher::All => true,
            Matcher::Exact(lit) => subject == &lit[..],
            Matcher::Pattern(p) => subject_matches(p, subject),
        }
    }
}

/// NATS-style subject match: `*` is one token, `>` is one-or-more tokens
/// and must be last.
fn subject_matches(pattern: &[u8], subject: &[u8]) -> bool {
    let mut p = pattern.split(|&b| b == b'.');
    let mut s = subject.split(|&b| b == b'.');
    loop {
        match (p.next(), s.next()) {
            (Some(b">"), Some(_)) => return true,
            (Some(pt), Some(st)) if pt == b"*" || pt == st => continue,
            (None, None) => return true,
            _ => return false,
        }
    }
}

/// One subscription. Lives inside its consumer's array and nowhere else.
pub(crate) struct Subscription {
    pub id: u32,
    matcher: Matcher,
    /// Pre-encoded SubFrame, replayed verbatim on reconnect.
    frame: Bytes,
    tx: mpsc::Sender<Message>,
}

/// One consumer and every subscription on it.
pub(crate) struct Consumer {
    pub id: u32,
    pub stream_id: u32,
    pub fanout: bool,
    /// Durable redelivery dedup, keyed `(stream, consumer)` by the store.
    /// Shared by every subscription here, and consulted ONCE per message —
    /// once per subscription would let the first one mark the seq and the
    /// rest discard it as a duplicate.
    pub dedup: Option<Arc<dyn SlotRef>>,
    subs: Arc<[Subscription]>,
}

/// What one wire entry resolves to. `stream_id` and `dedup` are copied in
/// so the delivery path never has to reach the `Consumer`; `subs` is the
/// same allocation the consumer holds.
struct Route {
    consumer_id: u32,
    stream_id: u32,
    fanout: bool,
    idx: u32,
    dedup: Option<Arc<dyn SlotRef>>,
    subs: Arc<[Subscription]>,
}

/// One delivery destination.
pub(crate) struct Target {
    pub sub_id: u32,
    pub tx: mpsc::Sender<Message>,
}

/// Per-message context the caller needs after routing.
pub(crate) struct Delivery {
    pub stream_id: u32,
    pub dedup: Option<Arc<dyn SlotRef>>,
}

/// What a caller supplies to open a subscription.
pub(crate) struct Registration<'a> {
    pub consumer_id: u32,
    pub stream_id: u32,
    pub fanout: bool,
    pub dedup: Option<Arc<dyn SlotRef>>,
    pub sub_id: u32,
    pub filter: &'a [u8],
    pub frame: Bytes,
}

#[derive(Default)]
struct Index {
    by_sub: Map<u32, Route>,
    by_consumer: Map<u32, Arc<Consumer>>,
}

/// The client's routing table. Both maps live under one lock so they can
/// never disagree about which subscriptions a consumer has.
#[derive(Default)]
pub(crate) struct Subscriptions {
    inner: RwLock<Index>,
}

impl std::fmt::Debug for Subscriptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let g = self.inner.read().unwrap();
        f.debug_struct("Subscriptions")
            .field("subscriptions", &g.by_sub.len())
            .field("consumers", &g.by_consumer.len())
            .finish()
    }
}

impl Subscriptions {
    pub fn new() -> Self {
        Self::default()
    }

    // ── Delivery ────────────────────────────────────────────────────────

    /// Resolve one wire entry. Appends every destination to `targets`,
    /// which the caller owns and reuses across entries.
    ///
    /// Queue mode has exactly one destination: the broker already picked
    /// the subscription, and it only picks ones whose filter matched, so
    /// re-checking here would redo the server's work. Fanout collapses to
    /// one copy per `(connection, consumer)`, so the filter decides here.
    ///
    /// Returns `None` for an unknown subscription — a delivery that
    /// arrived after the handle was dropped, or before a reconnect replay
    /// confirmed it.
    pub fn route(
        &self,
        sub_id: u32,
        subject: &[u8],
        targets: &mut Vec<Target>,
    ) -> Option<Delivery> {
        let g = self.inner.read().unwrap();
        let r = g.by_sub.get(&sub_id)?;

        if r.fanout {
            for s in r.subs.iter() {
                if s.matcher.accepts(subject) {
                    targets.push(Target {
                        sub_id: s.id,
                        tx: s.tx.clone(),
                    });
                }
            }
        } else if let Some(s) = r.subs.get(r.idx as usize) {
            targets.push(Target {
                sub_id: s.id,
                tx: s.tx.clone(),
            });
        }

        Some(Delivery {
            stream_id: r.stream_id,
            dedup: r.dedup.clone(),
        })
    }

    // ── Lifecycle ───────────────────────────────────────────────────────

    /// Open a subscription, returning its receiver.
    ///
    /// Re-registering an existing `sub_id` replaces it: the old channel is
    /// dropped and its receiver sees the close. Consumer-level fields come
    /// from the newest registration, which is what a reconnect replay
    /// wants.
    pub fn register(&self, reg: Registration<'_>) -> mpsc::Receiver<Message> {
        let (tx, rx) = mpsc::channel(CHANNEL_CAP);
        let fresh = Subscription {
            id: reg.sub_id,
            matcher: Matcher::new(reg.filter),
            frame: reg.frame,
            tx,
        };

        let mut g = self.inner.write().unwrap();
        let mut subs = take_subs(&g, reg.consumer_id);
        subs.retain(|s| s.id != reg.sub_id);
        subs.push(fresh);

        rebuild(
            &mut g,
            reg.consumer_id,
            reg.stream_id,
            reg.fanout,
            reg.dedup,
            subs,
        );
        rx
    }

    /// Set a consumer's delivery mode. The broker is the authority, and it
    /// only reports it in the subscribe reply — which lands after the
    /// subscription is already registered, so the flag arrives second.
    pub fn set_fanout(&self, consumer_id: u32, fanout: bool) {
        let mut g = self.inner.write().unwrap();
        let Some(c) = g.by_consumer.get(&consumer_id) else {
            return;
        };
        if c.fanout == fanout {
            return;
        }
        let (stream_id, dedup) = (c.stream_id, c.dedup.clone());
        let subs = take_subs(&g, consumer_id);
        rebuild(&mut g, consumer_id, stream_id, fanout, dedup, subs);
    }

    /// Retire one subscription. Siblings keep running; the consumer goes
    /// away only when its last subscription does.
    pub fn remove(&self, sub_id: u32) {
        let mut g = self.inner.write().unwrap();
        let Some(r) = g.by_sub.get(&sub_id) else {
            return;
        };
        let consumer_id = r.consumer_id;
        let Some(c) = g.by_consumer.get(&consumer_id) else {
            // Route without a consumer cannot happen, but leaving the entry
            // behind would leak it forever.
            g.by_sub.remove(&sub_id);
            return;
        };

        let (stream_id, fanout, dedup) = (c.stream_id, c.fanout, c.dedup.clone());
        let mut subs = take_subs(&g, consumer_id);
        subs.retain(|s| s.id != sub_id);

        if subs.is_empty() {
            drop_consumer(&mut g, consumer_id);
            return;
        }
        rebuild(&mut g, consumer_id, stream_id, fanout, dedup, subs);
        g.by_sub.remove(&sub_id);
    }

    /// Retire a consumer and everything under it. Called when the broker has
    /// been told to delete the consumer, so the local routes cannot outlive
    /// it — a stale route would otherwise sit in the table until teardown and
    /// catch deliveries if the id were ever recycled.
    pub fn remove_consumer(&self, consumer_id: u32) {
        let mut g = self.inner.write().unwrap();
        drop_consumer(&mut g, consumer_id);
    }

    // The stream-level counterpart is deliberately absent: `delete_stream`
    // takes a name and the client keeps no name → id map, so it cannot say
    // which consumers to retire. Deleting a stream therefore still strands
    // its routes until teardown — the same shape as the consumer leak above,
    // one level up, and not fixable without that map.

    // ── Queries ─────────────────────────────────────────────────────────

    /// Durable dedup slot of a consumer. Used by the broker-cursor path
    /// (`AckBatchResp` / `AckStateRep`), which speaks in consumers.
    pub fn dedup_of(&self, consumer_id: u32) -> Option<Arc<dyn SlotRef>> {
        self.inner
            .read()
            .unwrap()
            .by_consumer
            .get(&consumer_id)
            .and_then(|c| c.dedup.clone())
    }

    /// Every stored SubFrame, for reconnect replay. Non-destructive.
    pub fn replay_frames(&self) -> Vec<Bytes> {
        let g = self.inner.read().unwrap();
        g.by_consumer
            .values()
            .flat_map(|c| c.subs.iter().map(|s| s.frame.clone()))
            .collect()
    }

    /// Consumers carrying a durable dedup slot. Feeds the reconnect
    /// `AckStateReq` sweep, which purges WAL entries a dead session left.
    pub fn dedup_consumers(&self) -> Vec<u32> {
        let g = self.inner.read().unwrap();
        g.by_consumer
            .values()
            .filter(|c| c.dedup.is_some())
            .map(|c| c.id)
            .collect()
    }

    #[cfg(test)]
    pub fn sizes(&self) -> (usize, usize) {
        let g = self.inner.read().unwrap();
        (g.by_sub.len(), g.by_consumer.len())
    }
}

// ── Index maintenance ───────────────────────────────────────────────────
//
// One writer, both maps, one lock. Every path that changes a consumer's
// subscriptions goes through `rebuild` or `drop_consumer`, so a route can
// never outlive the subscription it points at.

/// Clone a consumer's subscriptions into an owned vec for editing. Cheap:
/// the channel sender and the frame are refcounted, only the matcher's
/// filter bytes are copied.
fn take_subs(g: &Index, consumer_id: u32) -> Vec<Subscription> {
    match g.by_consumer.get(&consumer_id) {
        Some(c) => c.subs.iter().map(clone_sub).collect(),
        None => Vec::with_capacity(1),
    }
}

fn clone_sub(s: &Subscription) -> Subscription {
    Subscription {
        id: s.id,
        matcher: match &s.matcher {
            Matcher::All => Matcher::All,
            Matcher::Exact(l) => Matcher::Exact(l.clone()),
            Matcher::Pattern(p) => Matcher::Pattern(p.clone()),
        },
        frame: s.frame.clone(),
        tx: s.tx.clone(),
    }
}

/// Publish a new subscription array for `consumer_id` and re-point every
/// route at it. Routes for subscriptions that are gone are erased here —
/// that is what keeps `by_sub` from growing forever.
fn rebuild(
    g: &mut Index,
    consumer_id: u32,
    stream_id: u32,
    fanout: bool,
    dedup: Option<Arc<dyn SlotRef>>,
    subs: Vec<Subscription>,
) {
    let stale: Vec<u32> = g
        .by_consumer
        .get(&consumer_id)
        .map(|c| c.subs.iter().map(|s| s.id).collect())
        .unwrap_or_default();

    let subs: Arc<[Subscription]> = subs.into();
    let consumer = Arc::new(Consumer {
        id: consumer_id,
        stream_id,
        fanout,
        dedup: dedup.clone(),
        subs: Arc::clone(&subs),
    });

    for id in stale {
        g.by_sub.remove(&id);
    }
    for (idx, s) in subs.iter().enumerate() {
        g.by_sub.insert(
            s.id,
            Route {
                consumer_id,
                stream_id,
                fanout,
                idx: idx as u32,
                dedup: dedup.clone(),
                subs: Arc::clone(&subs),
            },
        );
    }
    g.by_consumer.insert(consumer_id, consumer);
}

fn drop_consumer(g: &mut Index, consumer_id: u32) {
    if let Some(c) = g.by_consumer.remove(&consumer_id) {
        for s in c.subs.iter() {
            g.by_sub.remove(&s.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reg(consumer_id: u32, sub_id: u32, filter: &[u8], fanout: bool) -> Registration<'_> {
        Registration {
            consumer_id,
            stream_id: 1,
            fanout,
            dedup: None,
            sub_id,
            filter,
            frame: Bytes::from_static(b"frame"),
        }
    }

    #[test]
    fn siblings_coexist_and_each_keeps_its_channel() {
        let s = Subscriptions::new();
        let mut a = s.register(reg(7, 1, b"orders.*", true));
        let mut b = s.register(reg(7, 2, b"payments.*", true));
        let mut c = s.register(reg(7, 3, b"", true));
        assert_eq!(s.sizes(), (3, 1), "three routes, one consumer");

        // A copy arrives stamped with sub 3; fanout gives it to whoever
        // matches, not to whoever the broker happened to stamp.
        let mut t = Vec::new();
        let d = s.route(3, b"orders.created", &mut t).unwrap();
        assert_eq!(d.stream_id, 1);
        let ids: Vec<u32> = t.iter().map(|x| x.sub_id).collect();
        assert_eq!(ids, vec![1, 3], "orders.* and the catch-all, not payments.*");

        // Every channel is live — the old registry overwrote the first two.
        assert!(a.try_recv().is_err() && b.try_recv().is_err() && c.try_recv().is_err());
    }

    #[test]
    fn queue_mode_delivers_only_to_the_named_subscription() {
        let s = Subscriptions::new();
        s.register(reg(7, 1, b"orders.*", false));
        s.register(reg(7, 2, b"", false));

        let mut t = Vec::new();
        s.route(2, b"orders.created", &mut t).unwrap();
        assert_eq!(
            t.iter().map(|x| x.sub_id).collect::<Vec<_>>(),
            vec![2],
            "the broker already chose; the client must not second-guess it"
        );
    }

    #[test]
    fn removing_one_subscription_leaves_the_others() {
        let s = Subscriptions::new();
        s.register(reg(7, 1, b"a.*", true));
        s.register(reg(7, 2, b"b.*", true));

        s.remove(1);
        assert_eq!(s.sizes(), (1, 1));
        assert!(s.route(1, b"a.x", &mut Vec::new()).is_none());
        assert!(s.route(2, b"b.x", &mut Vec::new()).is_some());
    }

    #[test]
    fn last_subscription_out_takes_the_consumer_with_it() {
        let s = Subscriptions::new();
        s.register(reg(7, 1, b"", true));
        s.remove(1);
        assert_eq!(s.sizes(), (0, 0), "no consumer left behind");
    }

    #[test]
    fn dropping_a_consumer_erases_every_route_under_it() {
        let s = Subscriptions::new();
        s.register(reg(7, 1, b"a.*", true));
        s.register(reg(7, 2, b"b.*", true));
        s.register(reg(8, 3, b"", true));

        s.remove_consumer(7);
        assert_eq!(s.sizes(), (1, 1), "only consumer 8 survives");
        assert!(s.route(1, b"a.x", &mut Vec::new()).is_none());
        assert!(s.route(3, b"anything", &mut Vec::new()).is_some());
    }

    #[test]
    fn re_registering_replaces_instead_of_accumulating() {
        let s = Subscriptions::new();
        let mut first = s.register(reg(7, 1, b"a.*", true));
        s.register(reg(7, 1, b"b.*", true));
        assert_eq!(s.sizes(), (1, 1));

        // The replaced channel is closed, not orphaned in the map.
        assert!(matches!(
            first.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));

        let mut t = Vec::new();
        s.route(1, b"b.x", &mut t).unwrap();
        assert_eq!(t.len(), 1, "the new filter is the live one");
        t.clear();
        s.route(1, b"a.x", &mut t).unwrap();
        assert!(t.is_empty(), "the old filter is gone");
    }

    #[test]
    fn matcher_classifies_and_matches() {
        assert!(Matcher::new(b"").accepts(b"anything"));
        assert!(Matcher::new(b">").accepts(b"a.b.c"));
        assert!(Matcher::new(b"orders.created").accepts(b"orders.created"));
        assert!(!Matcher::new(b"orders.created").accepts(b"orders.updated"));
        assert!(Matcher::new(b"orders.*").accepts(b"orders.created"));
        assert!(!Matcher::new(b"orders.*").accepts(b"orders.a.b"));
        assert!(Matcher::new(b"orders.>").accepts(b"orders.a.b"));
        assert!(!Matcher::new(b"orders.*").accepts(b"payments.created"));
    }

    #[test]
    fn unknown_subscription_routes_nowhere() {
        let s = Subscriptions::new();
        assert!(s.route(99, b"x", &mut Vec::new()).is_none());
    }
}
