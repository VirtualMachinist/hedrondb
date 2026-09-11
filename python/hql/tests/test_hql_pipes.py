"""Three demo-vault pipes plus a missing-domain unit test."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from hql.query import Query, run_pipeline
from hql.store import Store, schema_sql


VAULT = "11111111-1111-1111-1111-111111111111"
FOUNDRY = "22222222-2222-2222-2222-222222222222"
LATTICE = "33333333-3333-3333-3333-333333333333"
RESOLVED = "44444444-4444-4444-4444-444444444444"
EDGE_D1 = "55555555-5555-5555-5555-555555555555"
EDGE_D2 = "66666666-6666-6666-6666-666666666666"
EDGE_R = "77777777-7777-7777-7777-777777777777"
NESTED = "88888888-8888-8888-8888-888888888888"

PIPE_FOUNDRY = (
    'vault demo-vault | search "HedronDB" | filter extra.domain == "foundry" '
    "| select path, extra.name | limit 20"
)
PIPE_DANGLING = (
    'vault demo-vault | search "lattice edges" | traverse --edge mentions --hops 1 '
    "| filter to_id == null | select path, to_raw"
)
PIPE_RESOLVED = (
    'vault demo-vault | filter path ^= "inbox/" && path !^= "inbox/private/" '
    "| traverse --edge mentions --hops 1 | filter to_id != null | select from.path, to.path"
)


def _build_tiny_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    # Fixtures execute the crate schema.sql, never a private copy.
    conn.executescript(schema_sql())
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) "
        "VALUES (?, ?, 'Vault', 'h', ?, 1, 'cool', 1.0, ?)",
        (VAULT, VAULT, "demo-vault", "name: demo-vault\n"),
    )
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) "
        "VALUES (?, ?, 'Document', 'h', ?, 1, 'warm', 0.5, ?)",
        (
            FOUNDRY,
            VAULT,
            "inbox/hedron-foundry.md",
            "name: HedronDB kernel notes\ndomain: foundry\n",
        ),
    )
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) "
        "VALUES (?, ?, 'Document', 'h', ?, 1, 'warm', 0.5, ?)",
        (
            LATTICE,
            VAULT,
            "notes/lattice-edges.md",
            "name: lattice edges walk\n",
        ),
    )
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) "
        "VALUES (?, ?, 'Document', 'h', ?, 1, 'warm', 0.5, ?)",
        (
            RESOLVED,
            VAULT,
            "inbox/resolved-mention.md",
            "name: resolved mention note\n",
        ),
    )
    for edge_id, to_raw in ((EDGE_D1, "GhostLink"), (EDGE_D2, "OtherGhost")):
        conn.execute(
            "INSERT INTO edges (id, vault_id, from_id, to_id, to_raw, type, properties) "
            "VALUES (?, ?, ?, NULL, ?, 'mentions', '{}')",
            (edge_id, VAULT, LATTICE, to_raw),
        )
    conn.execute(
        "INSERT INTO nodes (id, vault_id, type, content_hash, path, version, tier, importance, extra) "
        "VALUES (?, ?, 'Document', 'h', ?, 1, 'warm', 0.5, ?)",
        (
            NESTED,
            VAULT,
            "notes/nested.md",
            "tags:\n- a\n- b\nversion: 2\nmeta:\n  k: v\nflag: true\nempty: ~\nwhen: 2026-08-25\n",
        ),
    )
    conn.execute(
        "INSERT INTO edges (id, vault_id, from_id, to_id, to_raw, type, properties) "
        "VALUES (?, ?, ?, ?, 'hedron-foundry', 'mentions', '{}')",
        (EDGE_R, VAULT, RESOLVED, FOUNDRY),
    )
    conn.commit()
    conn.close()


class HqlPipesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._tmp = tempfile.TemporaryDirectory()
        cls.db = str(Path(cls._tmp.name) / "tiny.db")
        _build_tiny_db(Path(cls.db))

    @classmethod
    def tearDownClass(cls) -> None:
        cls._tmp.cleanup()

    def test_schema_matches_crate(self) -> None:
        self.assertEqual(Store(self.db).schema_mismatches(), [])

    def test_pipe_foundry_hedron(self) -> None:
        rows = run_pipeline(self.db, PIPE_FOUNDRY)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["path"], "inbox/hedron-foundry.md")
        self.assertIn("HedronDB", rows[0]["extra.name"] or "")

    def test_pipe_dangling_lattice_mentions(self) -> None:
        rows = run_pipeline(self.db, PIPE_DANGLING)
        self.assertEqual({row["to_raw"] for row in rows}, {"GhostLink", "OtherGhost"})
        self.assertTrue(all(row["path"] == "notes/lattice-edges.md" for row in rows))

    def test_pipe_resolved_inbox_mention(self) -> None:
        rows = run_pipeline(self.db, PIPE_RESOLVED)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["from.path"], "inbox/resolved-mention.md")
        self.assertEqual(rows[0]["to.path"], "inbox/hedron-foundry.md")

    def test_missing_domain_dropped_by_filter_kept_by_search(self) -> None:
        kept = (
            Query.open(self.db)
            .vault("demo-vault")
            .search("lattice edges")
            .select("path", "extra.domain")
            .run()
        )
        self.assertEqual(len(kept), 1)
        self.assertEqual(kept[0]["path"], "notes/lattice-edges.md")
        self.assertIsNone(kept[0]["extra.domain"])

        dropped = (
            Query.open(self.db)
            .vault("demo-vault")
            .search("lattice edges")
            .filter('extra.domain == "foundry"')
            .run()
        )
        self.assertEqual(dropped, [])

    def test_fluent_matches_foundry_pipe(self) -> None:
        rows = (
            Query.open(self.db)
            .vault("demo-vault")
            .search("HedronDB")
            .filter('extra.domain == "foundry"')
            .select("path", "extra.name")
            .limit(20)
            .run()
        )
        self.assertEqual([row["path"] for row in rows], ["inbox/hedron-foundry.md"])

    def test_extra_is_real_yaml(self) -> None:
        rows = run_pipeline(
            self.db,
            'vault demo-vault | filter path == "notes/nested.md" '
            "| select extra.tags, extra.version, extra.meta, extra.flag, extra.empty, extra.when, extra.missing",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["extra.tags"], "[a, b]")
        self.assertEqual(rows[0]["extra.version"], "2")
        self.assertEqual(rows[0]["extra.meta"], "{k: v}")
        self.assertEqual(rows[0]["extra.flag"], "true")
        self.assertIsNone(rows[0]["extra.empty"])
        self.assertEqual(rows[0]["extra.when"], "2026-08-25")
        self.assertIsNone(rows[0]["extra.missing"])


if __name__ == "__main__":
    unittest.main()
