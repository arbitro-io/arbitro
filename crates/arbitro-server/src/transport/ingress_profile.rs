//! Where a publish's time actually goes, from socket bytes to reply.
//!
//! Answers one question, because the microbenchmarks stopped adding up.
//! Every piece of the publish path measures in nanoseconds — the catalog
//! guard ~10 ns, the wire→seq hash 2.35 ns, the indexed reads ~3.3 ns, the
//! append sub-µs. They sum to roughly 30–50 ns. Yet a single connection
//! tops out near 1.5M msg/s, which is ~660 ns per message.
//!
//! So ~600 ns per message is somewhere those benchmarks never looked, and
//! arguing about whether a command crosses an mpsc is arguing about 15% of
//! the wrong number. This finds the other 85%.
//!
//! ## Phases
//!
//! - `read`     — `socket.read()` returning, and the accumulator work
//!                around it. Includes waiting on the kernel.
//! - `frame`    — splitting whole frames out of the accumulator and
//!                validating headers.
//! - `lookup`   — catalog snapshot, wire→seq, dedup window, quota reads.
//! - `dedup`    — the tracker, only when the stream declares a window.
//! - `append`   — the journal write plus the gate.
//! - `reply`    — building and handing off the RepOk.
//!
//! **Per FRAME, not per message.** A batch frame carries up to 256 entries,
//! so the six clock reads amortise. Timing each entry would cost more than
//! the entries — the mistake this instrument's drain-side sibling was built
//! to correct.
//!
//! OFF by default: without the feature every type here is a unit struct
//! with an empty `Drop` and every call site vanishes. Enable with
//! `--features ingress_profile` and call `report()` from a bench.

#[cfg(feature = "ingress_profile")]
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[cfg(feature = "ingress_profile")]
macro_rules! phases {
    ($($name:ident => $ctor:ident, $counter:ident);+ $(;)?) => {
        $(
            static $counter: AtomicU64 = AtomicU64::new(0);

            pub struct $name(std::time::Instant);

            impl Drop for $name {
                #[inline]
                fn drop(&mut self) {
                    $counter.fetch_add(self.0.elapsed().as_nanos() as u64, Relaxed);
                }
            }

            #[inline]
            pub fn $ctor() -> $name {
                $name(std::time::Instant::now())
            }
        )+
    };
}

#[cfg(feature = "ingress_profile")]
phases! {
    ReadPhase   => read,   READ_NS;
    FramePhase  => frame,  FRAME_NS;
    LookupPhase => lookup, LOOKUP_NS;
    DedupPhase  => dedup,  DEDUP_NS;
    AppendPhase => append, APPEND_NS;
    ReplyPhase  => reply,  REPLY_NS;
    // `total` wraps the whole `dispatch_frame_v2` call — every phase above
    // happens inside it, so `total - sum(inner)` is what nothing times:
    // action decode, frame validation, the match itself.
    TotalPhase  => total,  TOTAL_NS;
    // `append`, split by which door it took.
    LocalPhase  => append_local,  LOCAL_NS;
    RoutedPhase => append_routed, ROUTED_NS;
}

#[cfg(feature = "ingress_profile")]
static FRAMES: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "ingress_profile")]
static MESSAGES: AtomicU64 = AtomicU64::new(0);

/// One frame carrying `entries` messages was dispatched.
#[cfg(feature = "ingress_profile")]
#[inline]
pub fn frame_done(entries: usize) {
    FRAMES.fetch_add(1, Relaxed);
    MESSAGES.fetch_add(entries as u64, Relaxed);
}

#[cfg(not(feature = "ingress_profile"))]
#[inline]
pub fn frame_done(_entries: usize) {}

/// Print the breakdown and reset. Call from a bench after the load.
#[cfg(feature = "ingress_profile")]
pub fn report() {
    let frames = FRAMES.swap(0, Relaxed).max(1);
    let msgs = MESSAGES.swap(0, Relaxed).max(1);
    let read = READ_NS.swap(0, Relaxed);
    let frame = FRAME_NS.swap(0, Relaxed);
    let lookup = LOOKUP_NS.swap(0, Relaxed);
    let dedup = DEDUP_NS.swap(0, Relaxed);
    let append = APPEND_NS.swap(0, Relaxed);
    let reply = REPLY_NS.swap(0, Relaxed);
    let dispatch = TOTAL_NS.swap(0, Relaxed);
    let local = LOCAL_NS.swap(0, Relaxed);
    let routed = ROUTED_NS.swap(0, Relaxed);
    let total = read + frame + lookup + dedup + append + reply;

    println!("\n── ingress: {frames} frames, {msgs} messages ──\n");
    println!("  phase        ns/frame    ns/msg    share");
    let row = |name: &str, ns: u64| {
        println!(
            "  {name:<10} {:>9.1} {:>9.2}  {:>5.1}%",
            ns as f64 / frames as f64,
            ns as f64 / msgs as f64,
            if total > 0 {
                ns as f64 / total as f64 * 100.0
            } else {
                0.0
            }
        );
    };
    row("read", read);
    row("frame", frame);
    row("lookup", lookup);
    row("dedup", dedup);
    row("append", append);
    row("reply", reply);
    println!(
        "  {:<10} {:>9.1} {:>9.2}\n",
        "TOTAL",
        total as f64 / frames as f64,
        total as f64 / msgs as f64
    );
    row("  ..local", local);
    row("  ..routed", routed);
    println!();
    println!(
        "  dispatch envelope   {:>8.1} {:>9.2} ns   <- the real total",
        dispatch as f64 / frames as f64,
        dispatch as f64 / msgs as f64
    );
    println!(
        "  UNACCOUNTED         {:>8.1} {:>9.2} ns   <- inside dispatch, untimed",
        dispatch.saturating_sub(total) as f64 / frames as f64,
        dispatch.saturating_sub(total) as f64 / msgs as f64
    );
    println!(
        "\n  messages per frame: {:.1}\n",
        msgs as f64 / frames as f64
    );
}

#[cfg(not(feature = "ingress_profile"))]
pub fn report() {}

// ── Compiled-away stubs ──────────────────────────────────────────────────

#[cfg(not(feature = "ingress_profile"))]
macro_rules! stub_phases {
    ($($name:ident => $ctor:ident);+ $(;)?) => {
        $(
            pub struct $name;
            #[inline(always)]
            pub fn $ctor() -> $name { $name }
        )+
    };
}

#[cfg(not(feature = "ingress_profile"))]
stub_phases! {
    ReadPhase   => read;
    FramePhase  => frame;
    LookupPhase => lookup;
    DedupPhase  => dedup;
    AppendPhase => append;
    ReplyPhase  => reply;
    TotalPhase  => total;
    LocalPhase  => append_local;
    RoutedPhase => append_routed;
}
