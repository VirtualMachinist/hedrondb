//! Same session/pipe cases as `python/hql/tests`.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use hedron_core::hql::{run_pipeline, Query, RoStore, Value};
use hedron_core::{DesiredState, Node, Store};
use rusqlite::Connection;

static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// Fixtures execute the crate `schema.sql`, never a private copy.
const SCHEMA: &str = hedron_core::SCHEMA_SQL;

const VAULT_PIPE: &str = "11111111-1111-1111-1111-111111111111";
const FOUNDRY: &str = "22222222-2222-2222-2222-222222222222";
const LATTICE: &str = "33333333-3333-3333-3333-333333333333";
const RESOLVED: &str = "44444444-4444-4444-4444-444444444444";
const EDGE_D1: &str = "55555555-5555-5555-5555-555555555555";
const EDGE_D2: &str = "66666666-6666-6666-6666-666666666666";
const EDGE_R: &str = "77777777-7777-7777-7777-777777777777";

const PIPE_FOUNDRY: &str = r#"vault demo-vault | search "HedronDB" | filter extra.domain == "foundry" | select path, extra.name | limit 20"#;
const PIPE_DANGLING: &str = r#"vault demo-vault | search "lattice edges" | traverse --edge mentions --hops 1 | filter to_id == null | select path, to_raw"#;
const PIPE_RESOLVED: &str = r#"vault demo-vault | filter path ^= "inbox/" && path !^= "inbox/private/" | traverse --edge mentions --hops 1 | filter to_id != null | select from.path, to.path"#;

const VAULT_SESSION: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const AGENT: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
const DOC: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";
const DS_V1: &str = "dddddddd-dddd-dddd-dddd-dddddddddddd";
const DS_V2: &str = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee";
const EVENT: &str = "ffffffff-ffff-ffff-ffff-ffffffffffff";
const EVENT2: &str = "99999999-9999-9999-9999-999999999999";

const PIPE_SESSION: &str =
    "vault prod | agent deploy | state | select path, extra.name, extra.title, state_version, status";
const STATUS_V1: &str = "conditions:\n- type: Pending\n";
const STATUS_V2: &str = "conditions:\n- type: Reconciled\n";
const SPEC: &str = "date: 2026-08-25\nrequired_briefs:\n- alpha\n";

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
        rusqlite::params![VAULT_PIPE, VAULT_PIPE, "demo-vault", "name: demo-vault\n"],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) \
         VALUES (?1, ?2, 'Document', 'h', ?3, 1, 'warm', 0.5, ?4)",
        rusqlite::params![
            FOUNDRY,
            VAULT_PIPE,
            "inbox/hedron-foundry.md",
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
            "inbox/resolved-mention.md",
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
        rusqlite::params![VAULT_SESSION, VAULT_SESSION, "prod", "name: prod\n"],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) \
         VALUES (?1, ?2, 'Agent', 'h', ?3, 1, 'hot', 1.0, ?4)",
        rusqlite::params![AGENT, VAULT_SESSION, "agents/deploy", "title: deploy\n"],
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
         (id, vault_id, name, state_version, content_hash, last_reconciled, reconciled_by, importance, spec, status) \
         VALUES (?1, ?2, 'deploy-v1', 1, 'h1', NULL, NULL, 0.5, ?3, ?4)",
        rusqlite::params![DS_V1, VAULT_SESSION, SPEC, STATUS_V1],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO desired_states \
         (id, vault_id, name, state_version, content_hash, last_reconciled, reconciled_by, importance, spec, status) \
         VALUES (?1, ?2, 'deploy', 2, 'h2', 1, ?3, 0.8, ?4, ?5)",
        rusqlite::params![DS_V2, VAULT_SESSION, AGENT, SPEC, STATUS_V2],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO events (id, vault_id, ts, actor, type, data, caused_by, reconciles, supersedes) \
         VALUES (?1, ?2, 1, ?3, 'Reconciled', 'hdt_must_not_print', '[]', ?4, ?5)",
        rusqlite::params![EVENT, VAULT_SESSION, AGENT, DS_V2, format!("{DS_V1}@1")],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO events (id, vault_id, ts, actor, type, data, caused_by, reconciles, supersedes) \
         VALUES (?1, ?2, 2, ?3, 'Reconciled', 'raw_event_payload', '[]', ?4, ?5)",
        rusqlite::params![EVENT2, VAULT_SESSION, AGENT, DS_V2, format!("{DS_V2}@2")],
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
        Some("inbox/hedron-foundry.md")
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
fn pipe_resolved_inbox_mention() {
    let db = TempDb::new("tiny-resolved");
    build_tiny_db(&db.path);
    let rows = run_pipeline(&db.path, PIPE_RESOLVED).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        s(&rows[0], "from.path").as_deref(),
        Some("inbox/resolved-mention.md")
    );
    assert_eq!(
        s(&rows[0], "to.path").as_deref(),
        Some("inbox/hedron-foundry.md")
    );
}

#[test]
fn missing_domain_dropped_by_filter_kept_by_search() {
    let db = TempDb::new("tiny-domain");
    build_tiny_db(&db.path);
    let kept = Query::open(&db.path)
        .unwrap()
        .vault("demo-vault")
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
        .vault("demo-vault")
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
        .vault("demo-vault")
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
        vec!["inbox/hedron-foundry.md".to_string()]
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
    let rows = run_pipeline(&db.path, "agent deploy | state").unwrap();
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
fn session_pipe_selects_agent_title() {
    let db = TempDb::new("session-title");
    build_session_db(&db.path);
    let rows = run_pipeline(&db.path, PIPE_SESSION).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(s(&rows[0], "path").as_deref(), Some("agents/deploy"));
    assert_eq!(rows[0].get("extra.name"), Value::Null);
    assert_eq!(s(&rows[0], "extra.title").as_deref(), Some("deploy"));
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
        .vault("prod")
        .agent("deploy")
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
    let inside = run_pipeline(&db.path, "vault prod | agent deploy").unwrap();
    assert_eq!(inside.len(), 1);
    let outside = run_pipeline(&db.path, "vault no-such-vault | agent deploy").unwrap();
    assert!(outside.is_empty());
    let global_hit = run_pipeline(&db.path, "agent deploy").unwrap();
    assert_eq!(global_hit.len(), 1);
}

#[test]
fn agent_prefers_exact_title_and_accepts_substring() {
    let db = TempDb::new("session-substr");
    build_session_db(&db.path);
    let exact = run_pipeline(&db.path, "agent deploy").unwrap();
    assert_eq!(exact.len(), 1);
    assert_eq!(s(&exact[0], "extra.title").as_deref(), Some("deploy"));
    let substr = run_pipeline(&db.path, "agent dep").unwrap();
    assert_eq!(substr.len(), 1);
    assert_eq!(s(&substr[0], "path").as_deref(), Some("agents/deploy"));
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
        "vault prod | agent deploy | state | select path, spec, status, state_version, id",
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    let blob = rows[0]
        .as_dict(None)
        .into_iter()
        .map(|(_, v)| v.to_display())
        .collect::<Vec<_>>()
        .join(" ");
    assert!(!blob.contains("hdt_"));
    assert!(!blob.contains(EVENT));
}

#[test]
fn default_columns_hide_spec_and_status() {
    let db = TempDb::new("session-hide");
    build_session_db(&db.path);
    let rows = run_pipeline(&db.path, "agent deploy | state").unwrap();
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
    let err = run_pipeline(&db.path, "agent deploy | causal").unwrap_err();
    assert!(err.to_string().contains("unknown operator"));
}

#[test]
fn history_shows_latest_and_superseded_versions() {
    let db = TempDb::new("session-history");
    build_session_db(&db.path);
    let state = run_pipeline(&db.path, "vault prod | agent deploy | state").unwrap();
    assert_eq!(state.len(), 1);
    assert_eq!(state[0].get("state_version"), Value::Int(2));
    assert_eq!(s(&state[0], "id").as_deref(), Some(DS_V2));

    let rows = run_pipeline(&db.path, "vault prod | agent deploy | history").unwrap();
    assert_eq!(rows.len(), 2);
    let ids: Vec<String> = rows
        .iter()
        .map(|row| s(row, "id").unwrap_or_default())
        .collect();
    assert_eq!(ids, vec![EVENT.to_string(), EVENT2.to_string()]);
    let supersedes: Vec<String> = rows
        .iter()
        .map(|row| s(row, "supersedes").unwrap_or_default())
        .collect();
    assert!(supersedes.iter().any(|v| v.contains(DS_V1)));
    assert!(supersedes.iter().any(|v| v.contains(DS_V2)));
    assert!(rows
        .iter()
        .all(|row| s(row, "reconciles").as_deref() == Some(DS_V2)));
}

#[test]
fn history_does_not_bleed_spec_status_or_payloads() {
    let db = TempDb::new("session-history-hygiene");
    build_session_db(&db.path);
    let rows = run_pipeline(
        &db.path,
        "vault prod | agent deploy | history | select id, spec, status, state_version, data, reconciles, supersedes",
    )
    .unwrap();
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert_eq!(row.get("spec"), Value::Null);
        assert_eq!(row.get("status"), Value::Null);
        assert_eq!(row.get("state_version"), Value::Null);
        assert_eq!(row.get("data"), Value::Null);
        let blob = row
            .as_dict(None)
            .into_iter()
            .map(|(_, v)| v.to_display())
            .collect::<Vec<_>>()
            .join(" ");
        assert!(!blob.contains("hdt_"));
        assert!(!blob.contains("raw_event_payload"));
        assert!(!blob.contains("Pending"));
        assert!(!blob.contains("Reconciled\n") && !blob.contains("conditions:"));
    }
}

#[test]
fn fluent_matches_history_pipe() {
    let db = TempDb::new("session-history-fluent");
    build_session_db(&db.path);
    let rows = Query::open(&db.path)
        .unwrap()
        .vault("prod")
        .agent("deploy")
        .history()
        .select(["id", "ts", "reconciles", "supersedes"])
        .run()
        .unwrap();
    let piped = run_pipeline(
        &db.path,
        "vault prod | agent deploy | history | select id, ts, reconciles, supersedes",
    )
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(s(&rows[0], "id"), s(&piped[0], "id"));
    assert_eq!(rows[0].get("ts"), piped[0].get("ts"));
    assert_eq!(s(&rows[1], "id"), s(&piped[1], "id"));
}

#[test]
fn history_matches_store_causal_chain() {
    let n = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let path =
        std::env::temp_dir().join(format!("hedron-hql-chain-{}-{}.db", std::process::id(), n));
    let _ = std::fs::remove_file(&path);
    let mut store = Store::open(&path).unwrap();
    let boot = store.bootstrap("prod", "deploy", "agents/deploy").unwrap();
    let spec = DesiredState::briefs_spec("2026-08-25", &["alpha"]).unwrap();
    let ds = store.put_desired_state(&boot.token, spec, 0.5).unwrap();
    let doc = Node::brief_document(boot.vault.id, "alpha", "2026-08-25").unwrap();
    store.put_node(&boot.token, doc).unwrap();
    let (_, ev1) = store.reconcile(&boot.token, ds.id).unwrap();
    let (_, ev2) = store.reconcile(&boot.token, ds.id).unwrap();
    let chain = store.causal_chain(&boot.token, ds.id).unwrap();
    assert_eq!(chain.len(), 2);
    assert_eq!(chain[0].id, ev1.id);
    assert_eq!(chain[1].id, ev2.id);

    let state = run_pipeline(&path, "vault prod | agent deploy | state").unwrap();
    assert_eq!(state.len(), 1);
    assert_eq!(state[0].get("state_version"), Value::Int(3));

    let history = run_pipeline(&path, "vault prod | agent deploy | history").unwrap();
    assert_eq!(history.len(), 2);
    let ev1_id = ev1.id.to_string();
    let ev2_id = ev2.id.to_string();
    let prev1 = format!("{}@1", ds.id);
    let prev2 = format!("{}@2", ds.id);
    assert_eq!(s(&history[0], "id").as_deref(), Some(ev1_id.as_str()));
    assert_eq!(s(&history[1], "id").as_deref(), Some(ev2_id.as_str()));
    assert_eq!(
        s(&history[0], "supersedes").as_deref(),
        Some(prev1.as_str())
    );
    assert_eq!(
        s(&history[1], "supersedes").as_deref(),
        Some(prev2.as_str())
    );
    for row in &history {
        assert_eq!(row.get("spec"), Value::Null);
        assert_eq!(row.get("status"), Value::Null);
        let blob = row
            .as_dict(None)
            .into_iter()
            .map(|(_, v)| v.to_display())
            .collect::<Vec<_>>()
            .join(" ");
        assert!(!blob.contains("hdt_"));
        assert!(!blob.contains("present"));
        assert!(!blob.contains("missing"));
    }
    let _ = std::fs::remove_file(&path);
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
