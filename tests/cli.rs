//! Product CLI surface: `hedron import` / `hedron hql` help and import alias.

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

struct TempTree {
    path: PathBuf,
}

impl TempTree {
    fn new(label: &str) -> Self {
        let n = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("hedron-cli-{}-{}-{}", label, std::process::id(), n));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }
}

impl Drop for TempTree {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn hedron() -> Command {
    Command::new(env!("CARGO_BIN_EXE_hedron"))
}

#[test]
fn hedron_import_help() {
    let out = hedron()
        .args(["import", "--help"])
        .output()
        .expect("hedron import --help");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "help failed: {stderr}{stdout}");
    assert!(stdout.contains("hedron import"));
    assert!(stdout.contains("--src"));
    assert!(stdout.contains("thin alias"));
}

#[test]
fn hedron_hql_help() {
    let out = hedron()
        .args(["hql", "--help"])
        .output()
        .expect("hedron hql --help");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "help failed: {stderr}{stdout}");
    assert!(stdout.contains("hedron hql"));
    assert!(stdout.contains("--db"));
    assert!(stdout.contains("tsv|table|json"));
    assert!(stdout.contains("result-twin"));
}

#[test]
fn hedron_root_help() {
    let out = hedron().arg("--help").output().expect("hedron --help");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success());
    assert!(stdout.contains("import"));
    assert!(stdout.contains("hql"));
}

#[test]
fn hedron_import_subcommand_matches_alias() {
    let src = TempTree::new("src");
    fs::write(
        src.path.join("alpha.md"),
        "---\nname: Alpha\n---\nSee [[ghost]].\n",
    )
    .unwrap();

    let out_dir = TempTree::new("out");
    let db = out_dir.path.join("store.db");
    let result = hedron()
        .args([
            "import",
            "--src",
            src.path.to_str().unwrap(),
            "--db",
            db.to_str().unwrap(),
            "--vault",
            "fixture",
            "--agent",
            "importer",
        ])
        .output()
        .expect("hedron import");
    let stdout = String::from_utf8_lossy(&result.stdout);
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        result.status.success(),
        "hedron import failed: {stderr}{stdout}"
    );
    assert!(!stdout.contains("hdt_") && !stderr.contains("hdt_"));
    assert!(stdout.contains("documents: 1"));
    assert!(stdout.contains("mentions_dangling: 1"));
    assert!(stdout.contains("mode: 0600"));
}
