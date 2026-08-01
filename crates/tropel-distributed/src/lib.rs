//! # tropel-distributed
//!
//! Multi-node load testing: a `tropel-controller` partitions a job across N
//! `tropel-agent` workers using execution segments, then merges their
//! serialized hdr-histogram snapshots **losslessly** (🦀 Rust-opt: the
//! hdr-histogram V2 binary merge is exact — no percentile estimation, no
//! sampling — so the controller's p95/p99 are precisely the merged buckets).
//!
//! # Protocol
//!
//! TCP with length-prefixed JSON frames (u32 BE length + JSON bytes):
//!
//! - Controller → Agent: `Assign { config, segment, sequence, index }`
//! - Agent → Controller: `Snapshot { snapshot }`
//!
//! The controller computes N equal execution segments (`0:1/N`,
//! `1/N:2/N`, ... against sequence `0,1/N,...,1`) and dispatches one per
//! worker; each agent applies its segment (scaling VUs/iterations/rates
//! deterministically — see `tropel-core`'s `ExecutionSegment`), runs the
//! engine as a `distributed_worker` (no local reporting), and ships its raw
//! `MetricsSnapshot` back. The controller merges and reports.

pub mod agent;
pub mod controller;
pub mod protocol;

pub use agent::run_agent;
pub use controller::run_controller;
pub use protocol::{AssignMsg, SnapshotMsg};
