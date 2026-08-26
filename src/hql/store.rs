//! Read-only HedronDB store. rusqlite OPEN_READ_ONLY; never writes.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OpenFlags};

use crate::error::{Error, Result};
use crate::hql::row::{parse_extra_map, DesiredStateView, HistoryEventView, Row};
use crate::store::Store;
use crate::types::Event;
use uuid::Uuid;

/// Mirrors hedron-core `Store` SCHEMA (CREATE TABLE columns / types).
const EXPECTED_SCHEMA: &[(&str, &[(&str, &str)])] = &[
    (
        "nodes",
        &[
            ("id", "TEXT"),
            ("vault_id", "TEXT"),
            ("type", "TEXT"),
            ("content_hash", "TEXT"),
            ("path", "TEXT"),
            ("version", "INTEGER"),
            ("tier", "TEXT"),
            ("importance", "REAL"),
            ("htec_path", "TEXT"),
            ("extra", "TEXT"),
        ],
    ),
    (
        "edges",
        &[
            ("id", "TEXT"),
            ("vault_id", "TEXT"),
            ("from_id", "TEXT"),
            ("to_id", "TEXT"),
            ("to_raw", "TEXT"),
            ("type", "TEXT"),
            ("properties", "TEXT"),
        ],
    ),
    (
        "desired_states",
        &[
            ("id", "TEXT"),
            ("vault_id", "TEXT"),
            ("state_version", "INTEGER"),
            ("content_hash", "TEXT"),
            ("last_reconciled", "INTEGER"),
            ("reconciled_by", "TEXT"),
            ("importance", "REAL"),
            ("spec", "TEXT"),
            ("status", "TEXT"),
        ],
    ),
    (
        "events",
        &[
            ("id", "TEXT"),
            ("vault_id", "TEXT"),
            ("ts", "INTEGER"),
            ("actor", "TEXT"),
            ("type", "TEXT"),
            ("data", "TEXT"),
            ("caused_by", "TEXT"),
            ("reconciles", "TEXT"),
            ("supersedes", "TEXT"),
        ],
    ),
];

#[derive(Clone, Debug)]
pub(crate) struct EdgeRec {
    pub id: String,
    pub from_id: String,
    pub to_id: Option<String>,
    pub to_raw: Option<String>,
    pub edge_type: String,
    pub properties: String,
}

/// Read-only sqlite handle. Never migrate; report schema drift only.
pub struct RoStore {
    conn: Connection,
    path: PathBuf,
}

impl RoStore {
    pub fn open(db: impl AsRef<Path>) -> Result<Self> {
        let path = db.as_ref().to_path_buf();
        if !path.exists() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("store not found: {}", path.display()),
            )));
        }
        let conn = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        Ok(Self { conn, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn schema_mismatches(&self) -> Result<Vec<String>> {
        let existing = self.table_columns()?;
        let mut mismatches = Vec::new();
        let expected_names: Vec<&str> = EXPECTED_SCHEMA.iter().map(|(n, _)| *n).collect();
        for (table, cols) in EXPECTED_SCHEMA {
            let Some(got) = existing.get(*table) else {
                mismatches.push(format!("missing table {table}"));
                continue;
            };
            let want_names: Vec<&str> = cols.iter().map(|(n, _)| *n).collect();
            let got_names: Vec<&str> = got.iter().map(|(n, _)| n.as_str()).collect();
            if got_names != want_names {
                mismatches.push(format!(
                    "table {table} columns {got_names:?} != {want_names:?}"
                ));
            }
            for ((want_name, want_type), (got_name, got_type)) in cols.iter().zip(got.iter()) {
                if *want_name != got_name.as_str() {
                    continue;
                }
                if !want_type.eq_ignore_ascii_case(got_type) {
                    mismatches.push(format!(
                        "table {table} column {want_name} type {got_type:?} != {want_type:?}"
                    ));
                }
            }
        }
        for table in existing.keys() {
            if table.starts_with("sqlite_") {
                continue;
            }
            if !expected_names.contains(&table.as_str()) {
                mismatches.push(format!("unexpected table {table}"));
            }
        }
        Ok(mismatches)
    }

    pub fn nodes(&self) -> Result<Vec<Row>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, vault_id, type, path, extra FROM nodes")?;
        let rows = stmt.query_map([], |row| {
            let extra: Option<String> = row.get(4)?;
            let extra = extra.unwrap_or_default();
            let extra_map = parse_extra_map(Some(&extra));
            Ok(Row {
                path: row.get(3)?,
                extra,
                extra_map,
                node_id: row.get(0)?,
                vault_id: row.get(1)?,
                node_type: row.get(2)?,
                ..Row::default()
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)
    }

    pub(crate) fn edges(&self) -> Result<Vec<EdgeRec>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, vault_id, from_id, to_id, to_raw, type, properties FROM edges")?;
        let rows = stmt.query_map([], |row| {
            let properties: Option<String> = row.get(6)?;
            Ok(EdgeRec {
                id: row.get(0)?,
                from_id: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                to_id: row.get(3)?,
                to_raw: row.get(4)?,
                edge_type: row.get(5)?,
                properties: properties.unwrap_or_default(),
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)
    }

    /// Warm path: newest desired_states row per vault_id. Never reads events.
    pub fn latest_desired_states(&self) -> Result<HashMap<String, DesiredStateView>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, vault_id, state_version, reconciled_by, importance, spec, status \
             FROM desired_states",
        )?;
        let rows = stmt.query_map([], |row| {
            let spec: Option<String> = row.get(5)?;
            let status: Option<String> = row.get(6)?;
            Ok(DesiredStateView {
                id: row.get(0)?,
                vault_id: row.get(1)?,
                state_version: row.get(2)?,
                reconciled_by: row.get(3)?,
                importance: row.get(4)?,
                spec: spec.unwrap_or_default(),
                status: status.unwrap_or_default(),
            })
        })?;
        let mut latest: HashMap<String, DesiredStateView> = HashMap::new();
        for row in rows {
            let row = row?;
            if let Some(prev) = latest.get(&row.vault_id) {
                if row.state_version <= prev.state_version {
                    continue;
                }
            }
            latest.insert(row.vault_id.clone(), row);
        }
        Ok(latest)
    }

    /// Cool path: `Store::causal_chain` event rows for one Desired State.
    /// Drops `data`; does not return spec vs status.
    pub fn causal_chain(&self, desired_state_id: &str) -> Result<Vec<HistoryEventView>> {
        let id =
            Uuid::parse_str(desired_state_id).map_err(|err| Error::Invalid(err.to_string()))?;
        let events = Store::read_causal_chain(&self.conn, id)?;
        Ok(events.iter().map(history_view).collect())
    }

    pub fn vault_ids_named(&self, name: &str) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, path, extra FROM nodes WHERE type = 'Vault'")?;
        let rows = stmt.query_map([], |row| {
            let extra: Option<String> = row.get(2)?;
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                extra.unwrap_or_default(),
            ))
        })?;
        let mut ids = Vec::new();
        for row in rows {
            let (id, path, extra) = row?;
            let extra_map = parse_extra_map(Some(&extra));
            let extra_name = extra_map.get("name").and_then(|v| v.as_deref());
            if path.as_deref() == Some(name) || extra_name == Some(name) {
                ids.push(id);
            }
        }
        Ok(ids)
    }

    fn table_columns(&self) -> Result<HashMap<String, Vec<(String, String)>>> {
        let mut tables = Vec::new();
        let mut stmt = self
            .conn
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")?;
        let names = stmt.query_map([], |row| row.get::<_, String>(0))?;
        for name in names {
            tables.push(name?);
        }
        let mut out = HashMap::new();
        for table in tables {
            if !table.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                return Err(Error::Invalid(format!("unexpected table name {table}")));
            }
            let mut cols = Vec::new();
            let mut stmt = self.conn.prepare(&format!("PRAGMA table_info({table})"))?;
            let rows = stmt.query_map([], |row| {
                let name: String = row.get(1)?;
                let ty: String = row.get::<_, Option<String>>(2)?.unwrap_or_default();
                Ok((name, ty))
            })?;
            for row in rows {
                cols.push(row?);
            }
            out.insert(table, cols);
        }
        Ok(out)
    }
}

fn history_view(event: &Event) -> HistoryEventView {
    let caused_by = if event.caused_by.is_empty() {
        None
    } else {
        Some(
            event
                .caused_by
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(","),
        )
    };
    HistoryEventView {
        id: event.id.to_string(),
        vault_id: event.vault_id.to_string(),
        ts: event.ts,
        actor: event.actor.to_string(),
        event_type: event.event_type.clone(),
        caused_by,
        reconciles: event.reconciles.map(|id| id.to_string()),
        supersedes: event.supersedes.clone(),
    }
}
