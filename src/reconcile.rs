//! Kind-dispatched reconcilers.
//!
//! `Store` persists, isolates, versions and appends events. A `Reconciler`
//! only turns the vault's documents plus a spec into an observation. Which
//! impl runs is decided by `spec.kind`; a new kind is a new impl here, not a
//! branch inside `Store`.

use serde_yaml::Value;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::types::{Condition, ConditionKind, DocsEodSpec, Node, NodeType, Status};

/// `spec.kind` handled by [`DocsEod`].
pub const DOCS_EOD_KIND: &str = "docs_eod";

/// What a reconciler saw: the status to persist and the document ids that
/// produced it. `Store::reconcile` projects `caused_by` onto the event row.
#[derive(Debug, Clone, PartialEq)]
pub struct Observation {
    pub status: Status,
    pub caused_by: Vec<Uuid>,
}

pub trait Reconciler: Sync {
    fn kind(&self) -> &'static str;

    /// Observe `vault_docs` against `spec`. Pure: no store access, no clock.
    fn observe(&self, vault_docs: &[Node], spec: &Value) -> Result<Observation>;
}

/// Phase 0 reconciler: named briefs that should exist for a date.
/// Matches `Document` nodes on `extra.brief` + `extra.date`.
pub struct DocsEod;

impl Reconciler for DocsEod {
    fn kind(&self) -> &'static str {
        DOCS_EOD_KIND
    }

    fn observe(&self, vault_docs: &[Node], spec: &Value) -> Result<Observation> {
        let parsed: DocsEodSpec = serde_yaml::from_value(spec.clone())
            .map_err(|err| Error::Invalid(format!("{DOCS_EOD_KIND} spec: {err}")))?;

        let mut found: Vec<(Uuid, &str)> = Vec::new();
        for doc in vault_docs {
            if doc.node_type != NodeType::Document {
                continue;
            }
            let Some(brief) = doc.extra.get("brief").and_then(Value::as_str) else {
                continue;
            };
            if doc.extra.get("date").and_then(Value::as_str) != Some(parsed.date.as_str()) {
                continue;
            }
            if parsed.required_briefs.iter().any(|name| name == brief) {
                found.push((doc.id, brief));
            }
        }

        let present: Vec<String> = parsed
            .required_briefs
            .iter()
            .filter(|name| found.iter().any(|(_, brief)| brief == name))
            .cloned()
            .collect();
        let missing: Vec<String> = parsed
            .required_briefs
            .iter()
            .filter(|name| !present.contains(name))
            .cloned()
            .collect();

        let (kind, message) = if missing.is_empty() {
            (
                ConditionKind::Reconciled,
                Some("all required briefs are present".into()),
            )
        } else {
            (
                ConditionKind::Pending,
                Some(format!("missing briefs: {}", missing.join(", "))),
            )
        };

        let observed = serde_yaml::to_value(serde_yaml::Mapping::from_iter([
            (
                Value::String("present".into()),
                serde_yaml::to_value(&present)?,
            ),
            (
                Value::String("missing".into()),
                serde_yaml::to_value(&missing)?,
            ),
        ]))?;

        Ok(Observation {
            status: Status {
                observed,
                conditions: vec![Condition { kind, message }],
            },
            caused_by: found.into_iter().map(|(id, _)| id).collect(),
        })
    }
}

static RECONCILERS: &[&dyn Reconciler] = &[&DocsEod];

/// The reconciler for `spec.kind`. Missing or unknown kind is `Invalid`.
pub fn reconciler_for(spec: &Value) -> Result<&'static dyn Reconciler> {
    let kind = spec
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Invalid("desired state spec requires kind: <string>".into()))?;
    RECONCILERS
        .iter()
        .copied()
        .find(|r| r.kind() == kind)
        .ok_or_else(|| Error::Invalid(format!("unknown desired state kind {kind:?}")))
}
