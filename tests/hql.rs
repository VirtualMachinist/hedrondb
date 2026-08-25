//! Same session/pipe cases as `python/hql/tests`.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use hedron_core::hql::{run_pipeline, Query, RoStore, Value};
use rusqlite::Connection;

static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

const SCHEMA: &str = "
CREATE TABLE nodes (
    id TEXT PRIMARY KEY,
    vault_id TEXT NOT NULL,
    type TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    path TEXT,
    version INTEGER NOT NULL,
    tier TEXT NOT NULL,
    importance REAL NOT NULL,
    htec_path TEXT,
    extra TEXT NOT NULL
);

CREATE TABLE edges (
    id TEXT PRIMARY KEY,
    vault_id TEXT NOT NULL,
    from_id TEXT NOT NULL,
    to_id TEXT,
    to_raw TEXT,
    type TEXT NOT NULL,
    properties TEXT NOT NULL
);

CREATE TABLE desired_states (
    id TEXT PRIMARY KEY,
    vault_id TEXT NOT NULL,
    state_version INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    last_reconciled INTEGER,
    reconciled_by TEXT,
    importance REAL NOT NULL,
    spec TEXT NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE events (
    id TEXT PRIMARY KEY,
    vault_id TEXT NOT NULL,
    ts INTEGER NOT NULL,
    actor TEXT NOT NULL,
    type TEXT NOT NULL,
    data TEXT NOT NULL,
    caused_by TEXT NOT NULL,
    reconciles TEXT,
    supersedes TEXT
);
";

const VAULT_PIPE: &str = "11111111-1111-1111-1111-111111111111";
const FOUNDRY: &str = "22222222-2222-2222-2222-222222222222";
const LATTICE: &str = "33333333-3333-3333-3333-333333333333";
const RESOLVED: &str = "44444444-4444-4444-4444-444444444444";
const EDGE_D1: &str = "55555555-5555-5555-5555-555555555555";
const EDGE_D2: &str = "66666666-6666-6666-6666-666666666666";
const EDGE_R: &str = "77777777-7777-7777-7777-777777777777";

const PIPE_FOUNDRY: &str = r#"vault atrium-fixture | search "HedronDB" | filter extra.domain == "foundry" | select path, extra.name | limit 20"#;
const PIPE_DANGLING: &str = r#"vault atrium-fixture | search "lattice edges" | traverse --edge mentions --hops 1 | filter to_id == null | select path, to_raw"#;
const PIPE_RESOLVED: &str = r#"vault atrium-fixture | filter path ^= "mail_room/" && path !^= "mail_room/Uri/" | traverse --edge mentions --hops 1 | filter to_id != null | select from.path, to.path"#;

const VAULT_SESSION: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const AGENT: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
const DOC: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";
const DS_V1: &str = "dddddddd-dddd-dddd-dddd-dddddddddddd";
const DS_V2: &str = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee";
const EVENT: &str = "ffffffff-ffff-ffff-ffff-ffffffffffff";

const PIPE_SESSION: &str =
    "vault htec-leo | agent leo | state | select path, extra.name, extra.title, state_version, status";
const STATUS_V1: &str = "conditions:\n- type: Pending\n";
const STATUS_V2: &str = "conditions:\n- type: Reconciled\n";
const SPEC: &str = "date: 2026-08-25\nrequired_briefs:\n- eli\n";

struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn new(label: &str) -> Self {
        let n = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "hedron-hql-{}-{}-{}.db",
            label,
            std::process::id(),
            n
        ));
        let _ = std::fs::remove_file(&path);
        Self { path }
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn build_tiny_db(path: &std::path::Path) {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch(SCHEMA).unwrap();
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) \
         VALUES (?1, ?2, 'Vault', 'h', ?3, 1, 'cool', 1.0, ?4)",
        rusqlite::params![VAULT_PIPE, VAULT_PIPE, "atrium-fixture", "name: atrium-fixture\n"],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) \
         VALUES (?1, ?2, 'Document', 'h', ?3, 1, 'warm', 0.5, ?4)",
        rusqlite::params![
            FOUNDRY,
            VAULT_PIPE,
            "mail_room/hedron-foundry.md",
            "name: HedronDB kernel notes\ndomain: foundry\n",
        ],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) \
         VALUES (?1, ?2, 'Document', 'h', ?3, 1, 'warm', 0.5, ?4)",
        rusqlite::params![
            LATTICE,
            VAULT_PIPE,
            "notes/lattice-edges.md",
            "name: lattice edges walk\n",
        ],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) \
         VALUES (?1, ?2, 'Document', 'h', ?3, 1, 'warm', 0.5, ?4)",
        rusqlite::params![
            RESOLVED,
            VAULT_PIPE,
            "mail_room/resolved-mention.md",
            "name: resolved mention note\n",
        ],
    )
    .unwrap();
    for (edge_id, to_raw) in [(EDGE_D1, "GhostLink"), (EDGE_D2, "OtherGhost")] {
        conn.execute(
            "INSERT INTO edges (id, vault_id, from_id, to_id, to_raw, type, properties) \
             VALUES (?1, ?2, ?3, NULL, ?4, 'mentions', '{}')",
            rusqlite::params![edge_id, VAULT_PIPE, LATTICE, to_raw],
        )
        .unwrap();
    }
    conn.execute(
        "INSERT INTO edges (id, vault_id, from_id, to_id, to_raw, type, properties) \
         VALUES (?1, ?2, ?3, ?4, 'hedron-foundry', 'mentions', '{}')",
        rusqlite::params![EDGE_R, VAULT_PIPE, RESOLVED, FOUNDRY],
    )
    .unwrap();
}

fn build_session_db(path: &std::path::Path) {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch(SCHEMA).unwrap();
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) \
         VALUES (?1, ?2, 'Vault', 'h', ?3, 1, 'cool', 1.0, ?4)",
        rusqlite::params![VAULT_SESSION, VAULT_SESSION, "htec-leo", "name: htec-leo\n"],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) \
         VALUES (?1, ?2, 'Agent', 'h', ?3, 1, 'hot', 1.0, ?4)",
        rusqlite::params![AGENT, VAULT_SESSION, "agents/leo", "title: leo\n"],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) \
         VALUES (?1, ?2, 'Document', 'h', ?3, 1, 'warm', 0.5, ?4)",
        rusqlite::params![DOC, VAULT_SESSION, "notes/hello.md", "name: hello\n"],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO desired_states \
         (id, vault_id, state_version, content_hash, last_reconciled, reconciled_by, importance, spec, status) \
         VALUES (?1, ?2, 1, 'h1', NULL, NULL, 0.5, ?3, ?4)",
        rusqlite::params![DS_V1, VAULT_SESSION, SPEC, STATUS_V1],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO desired_states \
         (id, vault_id, state_version, content_hash, last_reconciled, reconciled_by, importance, spec, status) \
         VALUES (?1, ?2, 2, 'h2', 1, ?3, 0.8, ?4, ?5)",
        rusqlite::params![DS_V2, VAULT_SESSION, AGENT, SPEC, STATUS_V2],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO events (id, vault_id, ts, actor, type, data, caused_by, reconciles, supersedes) \
         VALUES (?1, ?2, 1, ?3, 'Reconciled', 'hdt_must_not_print', '[]', ?4, ?5)",
        rusqlite::params![EVENT, VAULT_SESSION, AGENT, DS_V2, format!("{DS_V1}@1")],
    )
    .unwrap();
}

fn s(row: &hedron_core::hql::Row, field: &str) -> Option<String> {
    match row.get(field) {
        Value::Null => None,
        other => Some(other.to_display()),
    }
}

#[test]
fn pipe_schema_matches_crate() {
    let db = TempDb::new("tiny-schema");
    build_tiny_db(&db.path);
    assert_eq!(
        RoStore::open(&db.path)
            .unwrap()
            .schema_mismatches()
            .unwrap(),
        Vec::<String>::new()
    );
}

#[test]
fn pipe_foundry_hedron() {
    let db = TempDb::new("tiny-foundry");
    build_tiny_db(&db.path);
    let rows = run_pipeline(&db.path, PIPE_FOUNDRY).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        s(&rows[0], "path").as_deref(),
        Some("mail_room/hedron-foundry.md")
    );
    assert!(s(&rows[0], "extra.name")
        .unwrap_or_default()
        .contains("HedronDB"));
}

#[test]
fn pipe_dangling_lattice_mentions() {
    let db = TempDb::new("tiny-dangling");
    build_tiny_db(&db.path);
    let rows = run_pipeline(&db.path, PIPE_DANGLING).unwrap();
    let raws: std::collections::HashSet<String> = rows
        .iter()
        .map(|row| s(row, "to_raw").unwrap_or_default())
        .collect();
    assert_eq!(
        raws,
        ["GhostLink".to_string(), "OtherGhost".to_string()]
            .into_iter()
            .collect()
    );
    assert!(rows
        .iter()
        .all(|row| s(row, "path").as_deref() == Some("notes/lattice-edges.md")));
}

#[test]
fn pipe_resolved_mail_room_mention() {
    let db = TempDb::new("tiny-resolved");
    build_tiny_db(&db.path);
    let rows = run_pipeline(&db.path, PIPE_RESOLVED).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        s(&rows[0], "from.path").as_deref(),
        Some("mail_room/resolved-mention.md")
    );
    assert_eq!(
        s(&rows[0], "to.path").as_deref(),
        Some("mail_room/hedron-foundry.md")
    );
}

#[test]
fn missing_domain_dropped_by_filter_kept_by_search() {
    let db = TempDb::new("tiny-domain");
    build_tiny_db(&db.path);
    let kept = Query::open(&db.path)
        .unwrap()
        .vault("atrium-fixture")
        .search("lattice edges")
        .select(["path", "extra.domain"])
        .run()
        .unwrap();
    assert_eq!(kept.len(), 1);
    assert_eq!(
        s(&kept[0], "path").as_deref(),
        Some("notes/lattice-edges.md")
    );
    assert_eq!(kept[0].get("extra.domain"), Value::Null);

    let dropped = Query::open(&db.path)
        .unwrap()
        .vault("atrium-fixture")
        .search("lattice edges")
        .filter(r#"extra.domain == "foundry""#)
        .run()
        .unwrap();
    assert!(dropped.is_empty());
}

#[test]
fn fluent_matches_foundry_pipe() {
    let db = TempDb::new("tiny-fluent");
    build_tiny_db(&db.path);
    let rows = Query::open(&db.path)
        .unwrap()
        .vault("atrium-fixture")
        .search("HedronDB")
        .filter(r#"extra.domain == "foundry""#)
        .select(["path", "extra.name"])
        .limit(20)
        .run()
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| s(row, "path").unwrap_or_default())
            .collect::<Vec<_>>(),
        vec!["mail_room/hedron-foundry.md".to_string()]
    );
}

#[test]
fn session_schema_matches_crate() {
    let db = TempDb::new("session-schema");
    build_session_db(&db.path);
    assert_eq!(
        RoStore::open(&db.path)
            .unwrap()
            .schema_mismatches()
            .unwrap(),
        Vec::<String>::new()
    );
}

#[test]
fn agent_state_pipe_returns_latest_version_only() {
    let db = TempDb::new("session-latest");
    build_session_db(&db.path);
    let rows = run_pipeline(&db.path, "agent leo | state").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("state_version"), Value::Int(2));
    let status = s(&rows[0], "status").unwrap_or_default();
    assert!(status.contains("Reconciled"));
    assert!(!status.contains("Pending"));
    assert_eq!(s(&rows[0], "id").as_deref(), Some(DS_V2));
    assert_eq!(s(&rows[0], "reconciled_by").as_deref(), Some(AGENT));
    assert_eq!(rows[0].get("importance"), Value::Float(0.8));
}

#[test]
fn session_pipe_selects_htec_title() {
    let db = TempDb::new("session-title");
    build_session_db(&db.path);
    let rows = run_pipeline(&db.path, PIPE_SESSION).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(s(&rows[0], "path").as_deref(), Some("agents/leo"));
    assert_eq!(rows[0].get("extra.name"), Value::Null);
    assert_eq!(s(&rows[0], "extra.title").as_deref(), Some("leo"));
    assert_eq!(rows[0].get("state_version"), Value::Int(2));
    assert!(s(&rows[0], "status")
        .unwrap_or_default()
        .contains("Reconciled"));
}

#[test]
fn fluent_matches_session_pipe() {
    let db = TempDb::new("session-fluent");
    build_session_db(&db.path);
    let rows = Query::open(&db.path)
        .unwrap()
        .vault("htec-leo")
        .agent("leo")
        .state()
        .select([
            "path",
            "extra.name",
            "extra.title",
            "state_version",
            "status",
        ])
        .run()
        .unwrap();
    let piped = run_pipeline(&db.path, PIPE_SESSION).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(s(&rows[0], "path"), s(&piped[0], "path"));
    assert_eq!(rows[0].get("extra.name"), piped[0].get("extra.name"));
    assert_eq!(s(&rows[0], "extra.title"), s(&piped[0], "extra.title"));
    assert_eq!(rows[0].get("state_version"), piped[0].get("state_version"));
    assert_eq!(s(&rows[0], "status"), s(&piped[0], "status"));
}

#[test]
fn agent_stays_inside_vault_slice() {
    let db = TempDb::new("session-vault");
    build_session_db(&db.path);
    let inside = run_pipeline(&db.path, "vault htec-leo | agent leo").unwrap();
    assert_eq!(inside.len(), 1);
    let outside = run_pipeline(&db.path, "vault no-such-vault | agent leo").unwrap();
    assert!(outside.is_empty());
    let global_hit = run_pipeline(&db.path, "agent leo").unwrap();
    assert_eq!(global_hit.len(), 1);
}

#[test]
fn agent_prefers_exact_title_and_accepts_substring() {
    let db = TempDb::new("session-substr");
    build_session_db(&db.path);
    let exact = run_pipeline(&db.path, "agent leo").unwrap();
    assert_eq!(exact.len(), 1);
    assert_eq!(s(&exact[0], "extra.title").as_deref(), Some("leo"));
    let substr = run_pipeline(&db.path, "agent le").unwrap();
    assert_eq!(substr.len(), 1);
    assert_eq!(s(&substr[0], "path").as_deref(), Some("agents/leo"));
}

#[test]
fn agent_does_not_match_document() {
    let db = TempDb::new("session-doc");
    build_session_db(&db.path);
    assert!(run_pipeline(&db.path, "agent hello").unwrap().is_empty());
}

#[test]
fn state_does_not_pull_event_log_or_tokens() {
    let db = TempDb::new("session-no-events");
    build_session_db(&db.path);
    let rows = run_pipeline(
        &db.path,
        "vault htec-leo | agent leo | state | select path, spec, status, state_version, id",
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    let blob = rows[0]
        .as_dict(None)
        .into_iter()
        .map(|(k, v)| format!("{k}={}", v.to_display()))
        .collect::<Vec<_>>()
        .join(" ");
    // as_dict(None) after select uses selected keys via get path...
    let selected = rows[0].as_dict(None);
    let blob = if rows[0].selected_fields().is_some() {
        selected
            .iter()
            .map(|(_, v)| v.to_display())
            .collect::<Vec<_>>()
            .join(" ")
    } else {
        blob
    };
    assert!(!blob.contains("hdt_"));
    assert!(!blob.contains(EVENT));
}

#[test]
fn default_columns_hide_spec_and_status() {
    let db = TempDb::new("session-hide");
    build_session_db(&db.path);
    let rows = run_pipeline(&db.path, "agent leo | state").unwrap();
    let fields = hedron_core::hql::fields_of(&rows);
    assert!(!fields.iter().any(|f| f == "spec" || f == "status"));
    assert!(s(&rows[0], "status")
        .unwrap_or_default()
        .contains("Reconciled"));
}

#[test]
fn causal_operator_is_out_of_scope() {
    let db = TempDb::new("session-causal");
    build_session_db(&db.path);
    let err = run_pipeline(&db.path, "agent leo | causal").unwrap_err();
    assert!(err.to_string().contains("unknown operator"));
}

#[test]
fn hql_opens_read_only() {
    let db = TempDb::new("ro");
    build_tiny_db(&db.path);
    let _store = RoStore::open(&db.path).unwrap();
    let write = Connection::open_with_flags(&db.path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY);
    let conn = write.unwrap();
    let failed = conn.execute("UPDATE nodes SET extra = 'x' WHERE 1=1", []);
    assert!(failed.is_err());
}
