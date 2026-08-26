//! Query rows: a node, or a traverse walk (from + edge + optional to).

use std::collections::BTreeMap;
use std::fmt;

/// Scalar cell. Missing extra keys and absent walk fields are Null.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Str(String),
    Int(i64),
    Float(f64),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    /// Python `str(value)` for a non-None cell; Null is empty (CLI `_cell`).
    pub fn to_display(&self) -> String {
        match self {
            Value::Null => String::new(),
            Value::Str(s) => s.clone(),
            Value::Int(n) => n.to_string(),
            Value::Float(f) => format_float(*f),
        }
    }

    /// Python `str(left)` used by filter compare for a present value.
    pub fn py_str(&self) -> String {
        match self {
            Value::Null => String::new(),
            Value::Str(s) => s.clone(),
            Value::Int(n) => n.to_string(),
            Value::Float(f) => format_float(*f),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_display())
    }
}

/// Match Python `str(float)` / `json.dumps` for finite values (`1.0` not `1`).
pub(crate) fn format_float(v: f64) -> String {
    if v.is_nan() {
        return "nan".into();
    }
    if v.is_infinite() {
        return if v.is_sign_positive() {
            "inf".into()
        } else {
            "-inf".into()
        };
    }
    let s = format!("{v}");
    if s.contains('.') || s.contains('e') || s.contains('E') {
        s
    } else {
        format!("{s}.0")
    }
}

/// Parse YAML-ish `key: value` lines. Missing / empty values are Null.
pub fn parse_extra_map(extra: Option<&str>) -> BTreeMap<String, Option<String>> {
    let mut mapping = BTreeMap::new();
    let Some(extra) = extra else {
        return mapping;
    };
    if extra.is_empty() {
        return mapping;
    }
    for raw_line in extra.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line == "---" || line == "..." || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let key = key.trim();
        if key.is_empty() || key.starts_with('-') || key.chars().any(|ch| ch.is_whitespace()) {
            continue;
        }
        mapping.insert(key.to_string(), unquote_extra(value.trim()));
    }
    mapping
}

fn unquote_extra(value: &str) -> Option<String> {
    if value.is_empty() || matches!(value, "null" | "~" | "Null" | "NULL") {
        return None;
    }
    if value.len() >= 2 {
        let bytes = value.as_bytes();
        let first = bytes[0];
        if (first == b'\'' || first == b'"') && bytes[value.len() - 1] == first {
            let inner = &value[1..value.len() - 1];
            return if inner.is_empty() {
                None
            } else {
                Some(inner.to_string())
            };
        }
    }
    Some(value.to_string())
}

/// One pipeline row. Missing extra keys and absent walk fields are Null.
#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub path: Option<String>,
    pub extra: String,
    pub extra_map: BTreeMap<String, Option<String>>,
    pub node_id: Option<String>,
    pub vault_id: Option<String>,
    pub node_type: Option<String>,
    pub from_id: Option<String>,
    pub from_path: Option<String>,
    pub to_id: Option<String>,
    pub to_raw: Option<String>,
    pub to_path: Option<String>,
    pub edge_type: Option<String>,
    pub properties: String,
    pub spec: Option<String>,
    pub status: Option<String>,
    pub state_version: Option<i64>,
    pub reconciled_by: Option<String>,
    pub importance: Option<f64>,
    pub state_id: Option<String>,
    pub event_id: Option<String>,
    pub ts: Option<i64>,
    pub actor: Option<String>,
    pub event_type: Option<String>,
    pub caused_by: Option<String>,
    pub reconciles: Option<String>,
    pub supersedes: Option<String>,
    pub(crate) selected: Option<Vec<(String, Value)>>,
}

impl Default for Row {
    fn default() -> Self {
        Self {
            path: None,
            extra: String::new(),
            extra_map: BTreeMap::new(),
            node_id: None,
            vault_id: None,
            node_type: None,
            from_id: None,
            from_path: None,
            to_id: None,
            to_raw: None,
            to_path: None,
            edge_type: None,
            properties: String::new(),
            spec: None,
            status: None,
            state_version: None,
            reconciled_by: None,
            importance: None,
            state_id: None,
            event_id: None,
            ts: None,
            actor: None,
            event_type: None,
            caused_by: None,
            reconciles: None,
            supersedes: None,
            selected: None,
        }
    }
}

impl Row {
    pub fn selected_fields(&self) -> Option<Vec<String>> {
        self.selected
            .as_ref()
            .map(|pairs| pairs.iter().map(|(k, _)| k.clone()).collect())
    }

    pub fn get(&self, field: &str) -> Value {
        if let Some(pairs) = &self.selected {
            return pairs
                .iter()
                .find(|(k, _)| k == field)
                .map(|(_, v)| v.clone())
                .unwrap_or(Value::Null);
        }
        self.raw_get(field)
    }

    pub fn raw_get(&self, field: &str) -> Value {
        if let Some(key) = field.strip_prefix("extra.") {
            return match self.extra_map.get(key) {
                Some(Some(s)) => Value::Str(s.clone()),
                Some(None) | None => Value::Null,
            };
        }
        match field {
            "path" => opt_str(&self.path),
            "to_id" => opt_str(&self.to_id),
            "to_raw" => opt_str(&self.to_raw),
            "from_id" => opt_str(&self.from_id),
            "from.path" => opt_str(&self.from_path),
            "to.path" => opt_str(&self.to_path),
            "id" => {
                if let Some(id) = &self.event_id {
                    Value::Str(id.clone())
                } else if let Some(id) = &self.state_id {
                    Value::Str(id.clone())
                } else {
                    opt_str(&self.node_id)
                }
            }
            "vault_id" => opt_str(&self.vault_id),
            "node_type" => opt_str(&self.node_type),
            "type" => {
                if let Some(event_type) = &self.event_type {
                    Value::Str(event_type.clone())
                } else {
                    opt_str(&self.edge_type)
                }
            }
            "ts" => match self.ts {
                Some(n) => Value::Int(n),
                None => Value::Null,
            },
            "actor" => opt_str(&self.actor),
            "caused_by" => opt_str(&self.caused_by),
            "reconciles" => opt_str(&self.reconciles),
            "supersedes" => opt_str(&self.supersedes),
            "spec" => opt_str(&self.spec),
            "status" => opt_str(&self.status),
            "state_version" => match self.state_version {
                Some(n) => Value::Int(n),
                None => Value::Null,
            },
            "reconciled_by" => opt_str(&self.reconciled_by),
            "importance" => match self.importance {
                Some(n) => Value::Float(n),
                None => Value::Null,
            },
            _ => Value::Null,
        }
    }

    /// Warm overlay: latest desired_states fields. Does not touch events.
    pub fn with_state(&self, ds: &DesiredStateView) -> Row {
        let mut row = self.clone();
        row.spec = Some(ds.spec.clone());
        row.status = Some(ds.status.clone());
        row.state_version = Some(ds.state_version);
        row.reconciled_by = ds.reconciled_by.clone();
        row.importance = Some(ds.importance);
        row.state_id = Some(ds.id.clone());
        row.selected = None;
        row
    }

    /// Cool chain row from `Store::causal_chain`. No spec/status/data.
    pub fn from_history(event: &HistoryEventView) -> Row {
        let mut row = Row {
            vault_id: Some(event.vault_id.clone()),
            event_id: Some(event.id.clone()),
            ts: Some(event.ts),
            actor: Some(event.actor.clone()),
            event_type: Some(event.event_type.clone()),
            caused_by: event.caused_by.clone(),
            reconciles: event.reconciles.clone(),
            supersedes: event.supersedes.clone(),
            ..Row::default()
        };
        let selected = default_history_fields()
            .iter()
            .map(|field| ((*field).to_string(), row.raw_get(field)))
            .collect();
        row.selected = Some(selected);
        row
    }

    pub fn project(&self, fields: &[String]) -> Row {
        let selected = fields
            .iter()
            .map(|field| (field.clone(), self.raw_get(field)))
            .collect();
        let mut row = self.clone();
        row.selected = Some(selected);
        row
    }

    pub fn as_dict(&self, fields: Option<&[String]>) -> Vec<(String, Value)> {
        if let Some(pairs) = &self.selected {
            return pairs.clone();
        }
        let default = default_output_fields();
        let owned: Vec<String> = match fields {
            Some(f) => f.to_vec(),
            None => default.iter().map(|s| (*s).to_string()).collect(),
        };
        owned
            .into_iter()
            .map(|field| {
                let value = self.raw_get(&field);
                (field, value)
            })
            .collect()
    }

    pub fn searchable_text(&self) -> String {
        format!(
            "{}\n{}\n{}\n{}",
            self.path.as_deref().unwrap_or(""),
            self.extra,
            self.to_raw.as_deref().unwrap_or(""),
            self.properties
        )
    }
}

pub fn default_output_fields() -> &'static [&'static str] {
    &["path", "from.path", "to.path", "to_id", "to_raw", "from_id"]
}

/// Cool-path default columns. Event metadata only; no spec/status/data.
pub fn default_history_fields() -> &'static [&'static str] {
    &[
        "id",
        "ts",
        "actor",
        "type",
        "caused_by",
        "reconciles",
        "supersedes",
    ]
}

fn opt_str(value: &Option<String>) -> Value {
    match value {
        Some(s) => Value::Str(s.clone()),
        None => Value::Null,
    }
}

#[derive(Clone, Debug)]
pub struct DesiredStateView {
    pub id: String,
    pub vault_id: String,
    pub state_version: i64,
    pub reconciled_by: Option<String>,
    pub importance: f64,
    pub spec: String,
    pub status: String,
}

/// Cool-path event view. Drops `data` so tokens / payloads never reach HQL rows.
#[derive(Clone, Debug)]
pub struct HistoryEventView {
    pub id: String,
    pub vault_id: String,
    pub ts: i64,
    pub actor: String,
    pub event_type: String,
    pub caused_by: Option<String>,
    pub reconciles: Option<String>,
    pub supersedes: Option<String>,
}
