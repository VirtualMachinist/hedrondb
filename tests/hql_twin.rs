//! Documented comparison: `hedron hql` and `python/hql` agree on fixture pipes.
//!
//! Same pipeline string against the same db must produce the same rows
//! (fields and values), not merely similar text. `cargo test --test hql_twin`
//! is the comparison.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

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

const PIPE_FOUNDRY: &str = r#"vault atrium-fixture | search "HedronDB" | filter extra.domain == "foundry" | select path, extra.name | limit 20"#;
const PIPE_DANGLING: &str = r#"vault atrium-fixture | search "lattice edges" | traverse --edge mentions --hops 1 | filter to_id == null | select path, to_raw"#;
const PIPE_RESOLVED: &str = r#"vault atrium-fixture | filter path ^= "mail_room/" && path !^= "mail_room/Uri/" | traverse --edge mentions --hops 1 | filter to_id != null | select from.path, to.path"#;
const PIPE_SESSION: &str =
    "vault htec-leo | agent leo | state | select path, extra.name, extra.title, state_version, status";
const PIPE_AGENT_STATE: &str = "agent leo | state | select path, extra.title, state_version, status, id, reconciled_by, importance";
const PIPE_NO_EVENTS: &str =
    "vault htec-leo | agent leo | state | select path, spec, status, state_version, id";

struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn new(label: &str) -> Self {
        let n = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "hedron-twin-{}-{}-{}.db",
            label,
            std::process::id(),
            n
        ));
        let _ = fs::remove_file(&path);
        Self { path }
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn python_hql_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("python/hql")
}

fn build_tiny_db(path: &Path) {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch(SCHEMA).unwrap();
    let vault = "11111111-1111-1111-1111-111111111111";
    let foundry = "22222222-2222-2222-2222-222222222222";
    let lattice = "33333333-3333-3333-3333-333333333333";
    let resolved = "44444444-4444-4444-4444-444444444444";
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) \
         VALUES (?1, ?1, 'Vault', 'h', 'atrium-fixture', 1, 'cool', 1.0, 'name: atrium-fixture\n')",
        [vault],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) \
         VALUES (?1, ?2, 'Document', 'h', 'mail_room/hedron-foundry.md', 1, 'warm', 0.5, \
         'name: HedronDB kernel notes\ndomain: foundry\n')",
        [foundry, vault],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) \
         VALUES (?1, ?2, 'Document', 'h', 'notes/lattice-edges.md', 1, 'warm', 0.5, \
         'name: lattice edges walk\n')",
        [lattice, vault],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) \
         VALUES (?1, ?2, 'Document', 'h', 'mail_room/resolved-mention.md', 1, 'warm', 0.5, \
         'name: resolved mention note\n')",
        [resolved, vault],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO edges (id, vault_id, from_id, to_id, to_raw, type, properties) \
         VALUES ('55555555-5555-5555-5555-555555555555', ?1, ?2, NULL, 'GhostLink', 'mentions', '{}')",
        [vault, lattice],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO edges (id, vault_id, from_id, to_id, to_raw, type, properties) \
         VALUES ('66666666-6666-6666-6666-666666666666', ?1, ?2, NULL, 'OtherGhost', 'mentions', '{}')",
        [vault, lattice],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO edges (id, vault_id, from_id, to_id, to_raw, type, properties) \
         VALUES ('77777777-7777-7777-7777-777777777777', ?1, ?2, ?3, 'hedron-foundry', 'mentions', '{}')",
        [vault, resolved, foundry],
    )
    .unwrap();
}

fn build_session_db(path: &Path) {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch(SCHEMA).unwrap();
    let vault = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    let agent = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) \
         VALUES (?1, ?1, 'Vault', 'h', 'htec-leo', 1, 'cool', 1.0, 'name: htec-leo\n')",
        [vault],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) \
         VALUES (?1, ?2, 'Agent', 'h', 'agents/leo', 1, 'hot', 1.0, 'title: leo\n')",
        [agent, vault],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) \
         VALUES ('cccccccc-cccc-cccc-cccc-cccccccccccc', ?1, 'Document', 'h', 'notes/hello.md', 1, 'warm', 0.5, 'name: hello\n')",
        [vault],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO desired_states \
         (id, vault_id, state_version, content_hash, last_reconciled, reconciled_by, importance, spec, status) \
         VALUES ('dddddddd-dddd-dddd-dddd-dddddddddddd', ?1, 1, 'h1', NULL, NULL, 0.5, \
         'date: 2026-08-25\nrequired_briefs:\n- eli\n', 'conditions:\n- type: Pending\n')",
        [vault],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO desired_states \
         (id, vault_id, state_version, content_hash, last_reconciled, reconciled_by, importance, spec, status) \
         VALUES ('eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee', ?1, 2, 'h2', 1, ?2, 0.8, \
         'date: 2026-08-25\nrequired_briefs:\n- eli\n', 'conditions:\n- type: Reconciled\n')",
        [vault, agent],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO events (id, vault_id, ts, actor, type, data, caused_by, reconciles, supersedes) \
         VALUES ('ffffffff-ffff-ffff-ffff-ffffffffffff', ?1, 1, ?2, 'Reconciled', 'hdt_must_not_print', '[]', \
         'eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee', 'dddddddd-dddd-dddd-dddd-dddddddddddd@1')",
        [vault, agent],
    )
    .unwrap();
}

fn run_hedron_hql(db: &Path, pipeline: &str) -> String {
    let out = Command::new(env!("CARGO_BIN_EXE_hedron"))
        .args([
            "hql",
            "--db",
            db.to_str().unwrap(),
            "--format",
            "json",
            pipeline,
        ])
        .output()
        .expect("run hedron hql");
    assert!(
        out.status.success(),
        "hedron hql failed: {}{}",
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout)
    );
    String::from_utf8(out.stdout).unwrap()
}

fn run_python_hql(db: &Path, pipeline: &str) -> String {
    let out = Command::new("python3")
        .args([
            "-m",
            "hql",
            "--db",
            db.to_str().unwrap(),
            "--format",
            "json",
            pipeline,
        ])
        .current_dir(python_hql_dir())
        .output()
        .expect("run python hql");
    assert!(
        out.status.success(),
        "python hql failed: {}{}",
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout)
    );
    String::from_utf8(out.stdout).unwrap()
}

fn json_values_equal(rust_json: &str, python_json: &str) -> Result<(), String> {
    let dir = std::env::temp_dir().join(format!(
        "hedron-twin-cmp-{}-{}",
        std::process::id(),
        TEMP_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&dir).unwrap();
    let rust_path = dir.join("rust.json");
    let py_path = dir.join("python.json");
    fs::write(&rust_path, rust_json).unwrap();
    fs::write(&py_path, python_json).unwrap();
    let script = r#"
import json, sys
a = json.load(open(sys.argv[1], encoding="utf-8"))
b = json.load(open(sys.argv[2], encoding="utf-8"))
if a != b:
    sys.stderr.write("rust=%r\npython=%r\n" % (a, b))
    sys.exit(1)
"#;
    let out = Command::new("python3")
        .args([
            "-c",
            script,
            rust_path.to_str().unwrap(),
            py_path.to_str().unwrap(),
        ])
        .output()
        .expect("compare json");
    let _ = fs::remove_dir_all(&dir);
    if out.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).into_owned())
    }
}

fn assert_twins(db: &Path, pipeline: &str) {
    let rust_json = run_hedron_hql(db, pipeline);
    let python_json = run_python_hql(db, pipeline);
    json_values_equal(&rust_json, &python_json).unwrap_or_else(|err| {
        panic!("twin mismatch for {pipeline:?}\nrust={rust_json}\npython={python_json}\n{err}")
    });
}

#[test]
fn rust_and_python_agree_on_citadel_pipes() {
    let db = TempDb::new("pipes");
    build_tiny_db(&db.path);
    for pipeline in [PIPE_FOUNDRY, PIPE_DANGLING, PIPE_RESOLVED] {
        assert_twins(&db.path, pipeline);
    }
}

#[test]
fn rust_and_python_agree_on_session_pipes() {
    let db = TempDb::new("session");
    build_session_db(&db.path);
    for pipeline in [PIPE_SESSION, PIPE_AGENT_STATE, PIPE_NO_EVENTS] {
        assert_twins(&db.path, pipeline);
    }
}

#[test]
fn rust_and_python_agree_agent_stays_in_vault() {
    let db = TempDb::new("vault-slice");
    build_session_db(&db.path);
    assert_twins(
        &db.path,
        "vault htec-leo | agent leo | select path, extra.title",
    );
    assert_twins(
        &db.path,
        "vault no-such-vault | agent leo | select path, extra.title",
    );
}
