"""Warm session: agent | state over a tiny H.TEC-style fixture."""

from __future__ import annotations

import io
import sqlite3
import tempfile
import unittest
from pathlib import Path

from hql.cli import main
from hql.query import Query, run_pipeline
from hql.store import Store, schema_sql


VAULT = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
AGENT = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
DOC = "cccccccc-cccc-cccc-cccc-cccccccccccc"
DS = "dddddddd-dddd-dddd-dddd-dddddddddddd"
EOD = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
EVENT = "ffffffff-ffff-ffff-ffff-ffffffffffff"
EVENT2 = "99999999-9999-9999-9999-999999999999"
EVENT3 = "88888888-8888-8888-8888-888888888888"

PIPE_SESSION = (
    "vault prod | agent deploy | state | "
    "select path, extra.name, extra.title, state_version, status"
)

STATUS = "conditions:\n- type: Reconciled\n"
SPEC = "kind: docs_eod\ndate: 2026-08-25\nrequired_briefs:\n- alpha\n"


def _build_session_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    # Fixtures execute the crate schema.sql, never a private copy.
    conn.executescript(schema_sql())
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
    # One named intent; versions live in the event log, not in extra ids.
    conn.execute(
        "INSERT INTO desired_states "
        "(id, vault_id, name, state_version, content_hash, last_reconciled, reconciled_by, importance, spec, status) "
        "VALUES (?, ?, 'deploy', 3, 'h3', 2, ?, 0.8, ?, ?)",
        (DS, VAULT, AGENT, SPEC, STATUS),
    )
    conn.execute(
        "INSERT INTO events (id, vault_id, ts, actor, type, data, caused_by, reconciles, supersedes) "
        "VALUES (?, ?, 1, ?, 'Reconciled', 'hdt_must_not_print', '[]', ?, ?)",
        (EVENT, VAULT, AGENT, DS, f"{DS}@1"),
    )
    conn.execute(
        "INSERT INTO events (id, vault_id, ts, actor, type, data, caused_by, reconciles, supersedes) "
        "VALUES (?, ?, 2, ?, 'Reconciled', 'raw_event_payload', '[]', ?, ?)",
        (EVENT2, VAULT, AGENT, DS, f"{DS}@2"),
    )
    # A second named intent in the same vault.
    conn.execute(
        "INSERT INTO desired_states "
        "(id, vault_id, name, state_version, content_hash, last_reconciled, reconciled_by, importance, spec, status) "
        "VALUES (?, ?, 'eod-2026-08-25', 2, 'h2', 3, ?, 0.4, ?, ?)",
        (EOD, VAULT, AGENT, SPEC, "conditions:\n- type: Pending\n"),
    )
    conn.execute(
        "INSERT INTO events (id, vault_id, ts, actor, type, data, caused_by, reconciles, supersedes) "
        "VALUES (?, ?, 3, ?, 'Reconciled', '{}', '[]', ?, ?)",
        (EVENT3, VAULT, AGENT, EOD, f"{EOD}@1"),
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

    def test_agent_state_pipe_returns_current_version(self) -> None:
        rows = run_pipeline(self.db, "agent deploy | state")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["state_version"], 3)
        self.assertIn("Reconciled", rows[0]["status"] or "")
        self.assertNotIn("Pending", rows[0]["status"] or "")
        self.assertEqual(rows[0]["id"], DS)
        self.assertEqual(rows[0]["reconciled_by"], AGENT)
        self.assertEqual(rows[0]["importance"], 0.8)

    def test_session_pipe_selects_agent_title(self) -> None:
        rows = run_pipeline(self.db, PIPE_SESSION)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["path"], "agents/deploy")
        self.assertIsNone(rows[0]["extra.name"])
        self.assertEqual(rows[0]["extra.title"], "deploy")
        self.assertEqual(rows[0]["state_version"], 3)
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

    def test_history_shows_every_superseded_version_of_one_id(self) -> None:
        state = run_pipeline(self.db, "vault prod | agent deploy | state")
        self.assertEqual(len(state), 1)
        self.assertEqual(state[0]["state_version"], 3)
        self.assertEqual(state[0]["id"], DS)

        rows = run_pipeline(self.db, "vault prod | agent deploy | history")
        self.assertEqual(len(rows), 2)
        self.assertEqual([row["id"] for row in rows], [EVENT, EVENT2])
        self.assertEqual([row["supersedes"] for row in rows], [f"{DS}@1", f"{DS}@2"])
        self.assertTrue(all(row["reconciles"] == DS for row in rows))

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

    def test_vault_state_lists_all_named_desired_states(self) -> None:
        rows = run_pipeline(self.db, "vault prod | state")
        self.assertEqual([row["name"] for row in rows], ["deploy", "eod-2026-08-25"])
        self.assertEqual([row["id"] for row in rows], [DS, EOD])
        self.assertTrue(all(row["path"] == "prod" for row in rows))
        one = run_pipeline(self.db, "vault prod | state eod-2026-08-25")
        self.assertEqual([row["id"] for row in one], [EOD])
        self.assertEqual(run_pipeline(self.db, "vault prod | state no-such"), [])

    def test_agent_state_selects_desired_state_named_after_agent(self) -> None:
        rows = run_pipeline(self.db, "vault prod | agent deploy | state")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["name"], "deploy")
        self.assertEqual(rows[0]["id"], DS)

    def test_state_name_works_without_agent_or_vault_node(self) -> None:
        rows = run_pipeline(self.db, 'vault prod | filter path ^= "notes/" | state deploy')
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0]["path"])
        self.assertEqual(rows[0]["id"], DS)
        history = run_pipeline(self.db, 'vault prod | filter path ^= "notes/" | history deploy')
        self.assertEqual([row["id"] for row in history], [EVENT, EVENT2])
        self.assertTrue(all(row["name"] is None for row in history))

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
        self.assertIn("3", text)
        self.assertIn("Reconciled", text)
        self.assertNotIn("hdt_", text)
        self.assertNotIn("Pending", text)


if __name__ == "__main__":
    unittest.main()
