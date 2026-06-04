//! # arbitro-client-tokio
//!
//! Clean-slate, pure-tokio Arbitro client. Coexists with `arbitro-client`
//! during the migration window; new callers should target this crate.
//!
//! ## Design pillars
//!
//! - **Built on `arbitro-kit` primitives.** `kit::Mpsc` for the writer
//!   queue, `kit::OneShotAsync` for request correlation, `kit::Pipe` /
//!   `kit::Hub` where the topology fits — kit is faster than
//!   `tokio::sync` for the hot paths and parks at ~0% CPU when idle.
//! - **Tokio runtime, no OS threads.** Every loop is `tokio::spawn`;
//!   kit's `*Async` flavors bridge non-tokio wakers cleanly.
//! - **Single-writer transport.** One task owns `OwnedWriteHalf`,
//!   drains the kit ring in batches, uses `write_vectored` for iovec
//!   frames.
//! - **Reconnect as a state machine.** Explicit `ConnState`, decorrelated
//!   jitter backoff, heartbeat watchdog, `CancellationToken` shutdown.
//! - **No duplication of wire types.** `ErrorCode`, `ProtoError`,
//!   `Action`, `Header`, `Envelope`, frame views all come from
//!   `arbitro-proto` — this crate only adds transport orchestration.
//!
//! See [the design plan](../../../../docs/arbitro-client-tokio.md) for
//! the full rationale.

#![doc(html_no_source)]
#![warn(missing_debug_implementations)]
// L17: stylistic clippy lints that the project has deliberately accepted —
// the broker / client wire layer has wide builder fn signatures
// (CreateStream / CreateConsumer mirror the wire body verbatim) and the
// doc lists use deliberate hanging indents for the design pillars.
#![allow(clippy::too_many_arguments)]
#![allow(clippy::doc_overindented_list_items)]

// Module skeleton — implementations land in subsequent steps of the plan.
pub mod client;
pub mod config;
pub mod consumer_builder;
pub mod cron;
pub mod error;
pub mod metrics;
pub mod workflow;

pub(crate) mod conn;
pub(crate) mod consume;
pub(crate) mod manage;
pub(crate) mod publish;
pub(crate) mod state;
pub(crate) mod transport;

#[doc(hidden)]
pub mod bench_helpers {
    pub use crate::state::seq::SeqAllocator;
}

/// Internal transport types exposed for benchmarking only.
/// Not part of the public API — may change without notice.
#[doc(hidden)]
pub mod transport_internal {
    pub use crate::transport::frame::{WriteFrame, INLINE_CAP, WRITE_QUEUE_CAP};
}

// Public re-exports — keep the surface symmetric with `arbitro-client`
// so a switch is a `Cargo.toml` line + import-path swap.
/// Per-subject inflight cap (pattern + limit). Pass a slice of these to
/// [`Client::create_consumer_with_limits`].
pub use arbitro_proto::v2::manager::SubjectLimit;
pub use client::{BatchEntry, Client};
pub use config::{ClientConfig, KeepAlive, ReconnectPolicy};
pub use consume::message::Message;
pub use consume::SubscriptionHandle;
pub use error::{ClientError, RequestResult};
pub use metrics::{ClientMetrics, ClientMetricsSnapshot};
pub use publish::PUBLISH_BATCH_MAX;

/// Fluent builder for `CreateConsumer` that validates invariants
/// (`max_subject_inflight` requires `AckPolicy::Explicit`, etc.) before
/// hitting the wire. Prefer this over [`Client::create_consumer_with_limits`]
/// at call sites — it's an explicit no-broker-round-trip cost when
/// validation fails, and the API stays readable as the config grows.
pub use consumer_builder::ConsumerBuilder;
pub use cron::{CronBuilder, CronContext, CronHandle};
pub use workflow::{WorkflowBuilder, WorkflowErrorContext, WorkflowHandle, WorkflowStepContext};

/// Consumer-config enums lifted from `arbitro-proto` so callers don't
/// have to depend on the proto crate directly to set `ack_policy`,
/// `deliver_policy`, or `deliver_mode` on the builder.
pub use arbitro_proto::config::{AckPolicy, DeliverMode, DeliverPolicy};
