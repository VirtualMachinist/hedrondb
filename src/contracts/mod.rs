//! Generated shape contracts (plain Rust, no Nickel VM).

pub mod docs_eod;

use serde_yaml::Value;

use crate::error::Result;
use crate::reconcile::DOCS_EOD_KIND;

/// Reject specs that fail the kind shape contract before persisting.
pub fn validate_spec_shape(spec: &Value) -> Result<()> {
    if spec.get("kind").and_then(Value::as_str) == Some(DOCS_EOD_KIND) {
        docs_eod::check_shape(spec)?;
    }
    Ok(())
}
