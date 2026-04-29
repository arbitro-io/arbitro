//! Transport layer — frame definitions, encoder, single-writer task,
//! reader task. Filled in Step 2 of the plan.
#![allow(dead_code)]

pub(crate) mod encode;
pub(crate) mod frame;
pub(crate) mod reader;
pub(crate) mod writer;
