//! HedronDB Phase 0 core: Desired State, causal Event Log, vault isolation.
//!
//! Query current state and causal history through separate APIs. This crate is a
//! sync rusqlite library — not a server, not a Python/HQL product, not a
//! general-purpose database.

mod error;
mod store;
mod types;

pub use error::{Error, Result};
pub use store::Store;
pub use types::{
    Bootstrap, Condition, ConditionKind, DesiredState, DocsEodSpec, Edge, Event, Node, NodeType,
    Status, Tier, CAUSAL_CAUSED_BY, CAUSAL_RECONCILES, CAUSAL_SUPERSEDES, EDGE_GRANT,
};
