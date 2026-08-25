use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{Error, Result};

pub const CAUSAL_CAUSED_BY: &str = "caused_by";
pub const CAUSAL_RECONCILES: &str = "reconciles";
pub const CAUSAL_SUPERSEDES: &str = "supersedes";
pub const EDGE_GRANT: &str = "grant";

const SECRET_KEYS: &[&str] = &["token", "api_key", "secret", "password", "authorization"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeType {
    Agent,
    Document,
    Vault,
}

impl NodeType {
    pub fn as_str(self) -> &'static str {
        match self {
            NodeType::Agent => "Agent",
            NodeType::Document => "Document",
            NodeType::Vault => "Vault",
        }
    }

    pub fn parse(raw: &str) -> Result<Self> {
        match raw {
            "Agent" => Ok(NodeType::Agent),
            "Document" => Ok(NodeType::Document),
            "Vault" => Ok(NodeType::Vault),
            other => Err(Error::Invalid(format!("unknown node type {other}"))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Hot,
    Warm,
    Cool,
    Cold,
}

impl Tier {
    pub fn as_str(self) -> &'static str {
        match self {
            Tier::Hot => "hot",
            Tier::Warm => "warm",
            Tier::Cool => "cool",
            Tier::Cold => "cold",
        }
    }

    pub fn parse(raw: &str) -> Result<Self> {
        match raw {
            "hot" => Ok(Tier::Hot),
            "warm" => Ok(Tier::Warm),
            "cool" => Ok(Tier::Cool),
            "cold" => Ok(Tier::Cold),
            other => Err(Error::Invalid(format!("unknown tier {other}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub id: Uuid,
    pub vault_id: Uuid,
    pub node_type: NodeType,
    pub content_hash: String,
    pub path: Option<String>,
    pub version: u64,
    pub tier: Tier,
    pub importance: f64,
    pub htec_path: Option<String>,
    pub extra: serde_yaml::Value,
}

impl Node {
    pub fn document(vault_id: Uuid, path: Option<&str>, extra: serde_yaml::Value) -> Result<Self> {
        reject_secrets(&extra)?;
        let node_type = NodeType::Document;
        let content_hash = content_hash(node_type, path, None, &extra);
        Ok(Self {
            id: Uuid::new_v4(),
            vault_id,
            node_type,
            content_hash,
            path: path.map(str::to_string),
            version: 1,
            tier: Tier::Warm,
            importance: 0.5,
            htec_path: None,
            extra,
        })
    }

    pub fn brief_document(vault_id: Uuid, name: &str, date: &str) -> Result<Self> {
        let extra = serde_yaml::to_value(serde_yaml::Mapping::from_iter([
            (
                serde_yaml::Value::String("brief".into()),
                serde_yaml::Value::String(name.into()),
            ),
            (
                serde_yaml::Value::String("date".into()),
                serde_yaml::Value::String(date.into()),
            ),
        ]))?;
        Self::document(vault_id, Some(&format!("briefs/{date}/{name}")), extra)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    pub id: Uuid,
    pub vault_id: Uuid,
    pub from: Uuid,
    pub to_id: Option<Uuid>,
    pub to_raw: Option<String>,
    pub edge_type: String,
    pub properties: serde_yaml::Value,
}

impl Edge {
    pub fn new(
        vault_id: Uuid,
        from: Uuid,
        to_id: Option<Uuid>,
        to_raw: Option<String>,
        edge_type: impl Into<String>,
    ) -> Result<Self> {
        let to_raw = to_raw.filter(|s| !s.is_empty());
        if to_id.is_none() && to_raw.is_none() {
            return Err(Error::Invalid(
                "edge target needs a dest UUID, a raw string, or both".into(),
            ));
        }
        Ok(Self {
            id: Uuid::new_v4(),
            vault_id,
            from,
            to_id,
            to_raw,
            edge_type: edge_type.into(),
            properties: serde_yaml::Value::Mapping(serde_yaml::Mapping::new()),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConditionKind {
    Reconciled,
    Pending,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Condition {
    #[serde(rename = "type")]
    pub kind: ConditionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub observed: serde_yaml::Value,
    pub conditions: Vec<Condition>,
}

impl Status {
    pub fn empty() -> Self {
        Self {
            observed: serde_yaml::Value::Mapping(serde_yaml::Mapping::new()),
            conditions: Vec::new(),
        }
    }
}

/// Option B recon spec: named briefs that should exist for a date.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DocsEodSpec {
    pub date: String,
    pub required_briefs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DesiredState {
    pub id: Uuid,
    pub vault_id: Uuid,
    pub state_version: u64,
    pub content_hash: String,
    pub last_reconciled: Option<i64>,
    pub reconciled_by: Option<Uuid>,
    pub importance: f64,
    pub spec: serde_yaml::Value,
    pub status: Status,
}

impl DesiredState {
    pub fn briefs_spec(date: &str, required_briefs: &[&str]) -> Result<serde_yaml::Value> {
        let spec = DocsEodSpec {
            date: date.to_string(),
            required_briefs: required_briefs.iter().map(|s| (*s).to_string()).collect(),
        };
        Ok(serde_yaml::to_value(spec)?)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub id: Uuid,
    pub vault_id: Uuid,
    pub ts: i64,
    pub actor: Uuid,
    pub event_type: String,
    pub data: serde_yaml::Value,
    pub caused_by: Vec<Uuid>,
    pub reconciles: Option<Uuid>,
    pub supersedes: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Bootstrap {
    pub vault: Node,
    pub agent: Node,
    pub token: String,
}

pub(crate) fn is_causal_type(edge_type: &str) -> bool {
    matches!(
        edge_type,
        CAUSAL_CAUSED_BY | CAUSAL_RECONCILES | CAUSAL_SUPERSEDES
    )
}

pub(crate) fn reject_secrets(extra: &serde_yaml::Value) -> Result<()> {
    let Some(map) = extra.as_mapping() else {
        return Ok(());
    };
    for key in map.keys() {
        let Some(name) = key.as_str() else {
            continue;
        };
        if SECRET_KEYS
            .iter()
            .any(|forbidden| name.eq_ignore_ascii_case(forbidden))
        {
            return Err(Error::Invalid(
                "secrets must not be stored in YAML/frontmatter".into(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn content_hash(
    node_type: NodeType,
    path: Option<&str>,
    htec_path: Option<&str>,
    extra: &serde_yaml::Value,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(node_type.as_str().as_bytes());
    hasher.update(b"\0");
    hasher.update(path.unwrap_or("").as_bytes());
    hasher.update(b"\0");
    hasher.update(htec_path.unwrap_or("").as_bytes());
    hasher.update(b"\0");
    if let Ok(blob) = serde_yaml::to_string(extra) {
        hasher.update(blob.as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

pub(crate) fn desired_state_hash(
    spec: &serde_yaml::Value,
    status: &Status,
    state_version: u64,
) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(serde_yaml::to_string(spec)?.as_bytes());
    hasher.update(b"\0");
    hasher.update(serde_yaml::to_string(status)?.as_bytes());
    hasher.update(b"\0");
    hasher.update(state_version.to_string().as_bytes());
    Ok(hasher.finalize().to_hex().to_string())
}

pub(crate) fn version_ref(id: Uuid, state_version: u64) -> String {
    format!("{id}@{state_version}")
}

pub(crate) fn validate_importance(importance: f64) -> Result<()> {
    if (0.0..=1.0).contains(&importance) {
        Ok(())
    } else {
        Err(Error::Invalid("importance must be in 0..=1".into()))
    }
}
