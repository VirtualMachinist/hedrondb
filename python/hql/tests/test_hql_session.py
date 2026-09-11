"""Warm session: agent | state over a tiny H.TEC-style fixture."""

from __future__ import annotations

import io
import sqlite3
import tempfile
import unittest
from pathlib import Path

from hql.cli import main
from hql.query import Query, run_pipeline
from hql.store import Store

SCHEMA = """
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
"""

VAULT = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
AGENT = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
DOC = "cccccccc-cccc-cccc-cccc-cccccccccccc"
DS_V1 = "dddddddd-dddd-dddd-dddd-dddddddddddd"
DS_V2 = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
EVENT = "ffffffff-ffff-ffff-ffff-ffffffffffff"
EVENT2 = "99999999-9999-9999-9999-999999999999"

PIPE_SESSION = (
    "vault prod | agent deploy | state | "
    "select path, extra.name, extra.title, state_version, status"
)

STATUS_V1 = "conditions:\n- type: Pending\n"
STATUS_V2 = "conditions:\n- type: Reconciled\n"
SPEC = "date: 2026-08-25\nrequired_briefs:\n- alpha\n"


def _build_session_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) "
        "VALUES (?, ?, 'Vault', 'h', ?, 1, 'cool', 1.0, ?)",
        (VAULT, VAULT, "prod", "name: prod\n"),
    )
    # H.TEC agents often set title and omit name.
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) "
        "VALUES (?, ?, 'Agent', 'h', ?, 1, 'hot', 1.0, ?)",
        (AGENT, VAULT, "agents/deploy", "title: deploy\n"),
    )
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) "
        "VALUES (?, ?, 'Document', 'h', ?, 1, 'warm', 0.5, ?)",
        (DOC, VAULT, "notes/hello.md", "name: hello\n"),
    )
    conn.execute(
        "INSERT INTO desired_states "
        "(id, vault_id, state_version, content_hash, last_reconciled, reconciled_by, importance, spec, status) "
        "VALUES (?, ?, 1, 'h1', NULL, NULL, 0.5, ?, ?)",
        (DS_V1, VAULT, SPEC, STATUS_V1),
    )
    conn.execute(
        "INSERT INTO desired_states "
        "(id, vault_id, state_version, content_hash, last_reconciled, reconciled_by, importance, spec, status) "
        "VALUES (?, ?, 2, 'h2', 1, ?, 0.8, ?, ?)",
        (DS_V2, VAULT, AGENT, SPEC, STATUS_V2),
    )
    conn.execute(
        "INSERT INTO events (id, vault_id, ts, actor, type, data, caused_by, reconciles, supersedes) "
        "VALUES (?, ?, 1, ?, 'Reconciled', 'hdt_must_not_print', '[]', ?, ?)",
        (EVENT, VAULT, AGENT, DS_V2, f"{DS_V1}@1"),
    )
    conn.execute(
        "INSERT INTO events (id, vault_id, ts, actor, type, data, caused_by, reconciles, supersedes) "
        "VALUES (?, ?, 2, ?, 'Reconciled', 'raw_event_payload', '[]', ?, ?)",
        (EVENT2, VAULT, AGENT, DS_V2, f"{DS_V2}@2"),
    )
    conn.commit()
    conn.close()


class HqlSessionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._tmp = tempfile.TemporaryDirectory()
        cls.db = str(Path(cls._tmp.name) / "session.db")
        _build_session_db(Path(cls.db))

    @classmethod
    def tearDownClass(cls) -> None:
        cls._tmp.cleanup()

    def test_schema_matches_crate(self) -> None:
        self.assertEqual(Store(self.db).schema_mismatches(), [])

    def test_agent_state_pipe_returns_latest_version_only(self) -> None:
        rows = run_pipeline(self.db, "agent deploy | state")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["state_version"], 2)
        self.assertIn("Reconciled", rows[0]["status"] or "")
        self.assertNotIn("Pending", rows[0]["status"] or "")
        self.assertEqual(rows[0]["id"], DS_V2)
        self.assertEqual(rows[0]["reconciled_by"], AGENT)
        self.assertEqual(rows[0]["importance"], 0.8)

    def test_session_pipe_selects_agent_title(self) -> None:
        rows = run_pipeline(self.db, PIPE_SESSION)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["path"], "agents/deploy")
        self.assertIsNone(rows[0]["extra.name"])
        self.assertEqual(rows[0]["extra.title"], "deploy")
        self.assertEqual(rows[0]["state_version"], 2)
        self.assertIn("Reconciled", rows[0]["status"] or "")

    def test_fluent_matches_session_pipe(self) -> None:
        rows = (
            Query.open(self.db)
            .vault("prod")
            .agent("deploy")
            .state()
            .select("path", "extra.name", "extra.title", "state_version", "status")
            .run()
        )
        piped = run_pipeline(self.db, PIPE_SESSION)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["path"], piped[0]["path"])
        self.assertEqual(rows[0]["extra.name"], piped[0]["extra.name"])
        self.assertEqual(rows[0]["extra.title"], piped[0]["extra.title"])
        self.assertEqual(rows[0]["state_version"], piped[0]["state_version"])
        self.assertEqual(rows[0]["status"], piped[0]["status"])

    def test_agent_stays_inside_vault_slice(self) -> None:
        inside = run_pipeline(self.db, "vault prod | agent deploy")
        self.assertEqual(len(inside), 1)
        outside = run_pipeline(self.db, "vault no-such-vault | agent deploy")
        self.assertEqual(outside, [])
        global_hit = run_pipeline(self.db, "agent deploy")
        self.assertEqual(len(global_hit), 1)

    def test_agent_prefers_exact_title_and_accepts_substring(self) -> None:
        exact = run_pipeline(self.db, "agent deploy")
        self.assertEqual(len(exact), 1)
        self.assertEqual(exact[0]["extra.title"], "deploy")
        substr = run_pipeline(self.db, "agent dep")
        self.assertEqual(len(substr), 1)
        self.assertEqual(substr[0]["path"], "agents/deploy")

    def test_agent_does_not_match_document(self) -> None:
        self.assertEqual(run_pipeline(self.db, "agent hello"), [])

    def test_state_does_not_pull_event_log_or_tokens(self) -> None:
        rows = run_pipeline(
            self.db,
            "vault prod | agent deploy | state | select path, spec, status, state_version, id",
        )
        self.assertEqual(len(rows), 1)
        blob = " ".join(str(rows[0].get(field) or "") for field in rows[0].as_dict())
        self.assertNotIn("hdt_", blob)
        self.assertNotIn(EVENT, blob)

    def test_causal_operator_is_out_of_scope(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            run_pipeline(self.db, "agent deploy | causal")
        self.assertIn("unknown operator", str(ctx.exception))

    def test_history_shows_latest_and_superseded_versions(self) -> None:
        state = run_pipeline(self.db, "vault prod | agent deploy | state")
        self.assertEqual(len(state), 1)
        self.assertEqual(state[0]["state_version"], 2)
        self.assertEqual(state[0]["id"], DS_V2)

        rows = run_pipeline(self.db, "vault prod | agent deploy | history")
        self.assertEqual(len(rows), 2)
        self.assertEqual([row["id"] for row in rows], [EVENT, EVENT2])
        supersedes = [row["supersedes"] for row in rows]
        self.assertTrue(any(DS_V1 in (value or "") for value in supersedes))
        self.assertTrue(any(DS_V2 in (value or "") for value in supersedes))
        self.assertTrue(all(row["reconciles"] == DS_V2 for row in rows))

    def test_history_does_not_bleed_spec_status_or_payloads(self) -> None:
        rows = run_pipeline(
            self.db,
            "vault prod | agent deploy | history | "
            "select id, spec, status, state_version, data, reconciles, supersedes",
        )
        self.assertEqual(len(rows), 2)
        for row in rows:
            self.assertIsNone(row["spec"])
            self.assertIsNone(row["status"])
            self.assertIsNone(row["state_version"])
            self.assertIsNone(row["data"])
            blob = " ".join(str(value or "") for value in row.as_dict().values())
            self.assertNotIn("hdt_", blob)
            self.assertNotIn("raw_event_payload", blob)
            self.assertNotIn("Pending", blob)
            self.assertNotIn("conditions:", blob)

    def test_fluent_matches_history_pipe(self) -> None:
        rows = (
            Query.open(self.db)
            .vault("prod")
            .agent("deploy")
            .history()
            .select("id", "ts", "reconciles", "supersedes")
            .run()
        )
        piped = run_pipeline(
            self.db,
            "vault prod | agent deploy | history | select id, ts, reconciles, supersedes",
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["id"], piped[0]["id"])
        self.assertEqual(rows[0]["ts"], piped[0]["ts"])
        self.assertEqual(rows[1]["id"], piped[1]["id"])

    def test_cli_session_pipe(self) -> None:
        buf = io.StringIO()
        code = main(
            ["--db", self.db, "--format", "tsv", PIPE_SESSION],
            out=buf,
        )
        self.assertEqual(code, 0)
        text = buf.getvalue()
        self.assertIn("state_version", text)
        self.assertIn("agents/deploy", text)
        self.assertIn("deploy", text)
        self.assertIn("2", text)
        self.assertIn("Reconciled", text)
        self.assertNotIn("hdt_", text)
        self.assertNotIn("Pending", text)


if __name__ == "__main__":
    unittest.main()
