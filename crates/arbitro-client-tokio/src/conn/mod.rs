//! Connection orchestration — `ConnState` machine, dial → handshake →
//! resub → run → teardown. Implementations land in Step 4 of the plan.

#![allow(dead_code)]

pub(crate) mod heartbeat;
pub(crate) mod reconnect;
pub(crate) mod session;
