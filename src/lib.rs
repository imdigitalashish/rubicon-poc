//! A proof of concept of GitHub's "Project Rubicon" Git storage architecture.
//!
//! ```text
//!   git client ──► Spokes ──► virtual replicas (disposable compute; disk = cache)
//!                                 │ packs                 │ refs + pack manifest
//!                                 ▼                       ▼
//!                          Blob Storage  ◄──────  Ref-CAS (quorum log, per-ref CAS)
//!                                 ▲                       ▲
//!                                 └──── compaction nodes ─┘  (background, leased shards)
//! ```
//!
//! Everything runs in one process; network and storage latency are simulated.

pub mod agent;
pub mod blob;
pub mod cluster;
pub mod compaction;
pub mod object;
pub mod refcas;
pub mod replica;
pub mod spokes;
mod util;
