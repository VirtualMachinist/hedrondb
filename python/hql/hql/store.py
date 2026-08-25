"""Read-only HedronDB store. Opens sqlite via URI mode=ro; never writes."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional
from urllib.parse import quote

from hql.row import Row, parse_extra_map

# Mirrors hedron-core src/store.rs SCHEMA (CREATE TABLE columns / types).
EXPECTED_SCHEMA: dict[str, list[tuple[str, str]]] = {
    "nodes": [
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
    "edges": [
        ("id", "TEXT"),
        ("vault_id", "TEXT"),
        ("from_id", "TEXT"),
        ("to_id", "TEXT"),
        ("to_raw", "TEXT"),
        ("type", "TEXT"),
        ("properties", "TEXT"),
    ],
    "desired_states": [
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
    "events": [
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
}


class Store:
    """Read-only sqlite3 handle. Never migrate; report schema drift only."""

    def __init__(self, db: str | Path) -> None:
        path = Path(db).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"store not found: {path}")
        # sqlite URI: file:/abs/path?mode=ro — never CREATE, never write.
        uri = f"file:{quote(path.as_posix(), safe='/')}?mode=ro"
        self.path = path
        self.conn = sqlite3.connect(uri, uri=True)
        self.conn.row_factory = sqlite3.Row

    def schema_mismatches(self) -> list[str]:
        existing = self._table_columns()
        mismatches: list[str] = []
        for table, cols in EXPECTED_SCHEMA.items():
            if table not in existing:
                mismatches.append(f"missing table {table}")
                continue
            got = existing[table]
            want_names = [name for name, _ in cols]
            got_names = [name for name, _ in got]
            if got_names != want_names:
                mismatches.append(
                    f"table {table} columns {got_names} != {want_names}"
                )
            for (want_name, want_type), (got_name, got_type) in zip(cols, got):
                if want_name != got_name:
                    continue
                if want_type.upper() != got_type.upper():
                    mismatches.append(
                        f"table {table} column {want_name} type {got_type!r} != {want_type!r}"
                    )
        for table in existing:
            if table.startswith("sqlite_"):
                continue
            if table not in EXPECTED_SCHEMA:
                mismatches.append(f"unexpected table {table}")
        return mismatches

    def nodes(self) -> list[Row]:
        rows = self.conn.execute(
            "SELECT id, vault_id, type, path, extra FROM nodes"
        ).fetchall()
        return [
            Row(
                path=row["path"],
                extra=row["extra"] or "",
                extra_map=parse_extra_map(row["extra"]),
                node_id=row["id"],
                vault_id=row["vault_id"],
                node_type=row["type"],
            )
            for row in rows
        ]

    def edges(self) -> list[dict[str, Optional[str]]]:
        rows = self.conn.execute(
            "SELECT id, vault_id, from_id, to_id, to_raw, type, properties FROM edges"
        ).fetchall()
        return [
            {
                "id": row["id"],
                "vault_id": row["vault_id"],
                "from_id": row["from_id"],
                "to_id": row["to_id"],
                "to_raw": row["to_raw"],
                "type": row["type"],
                "properties": row["properties"] or "",
            }
            for row in rows
        ]

    def latest_desired_states(self) -> dict[str, dict]:
        """Warm path: newest desired_states row per vault_id. Never reads events."""
        rows = self.conn.execute(
            "SELECT id, vault_id, state_version, reconciled_by, importance, spec, status "
            "FROM desired_states"
        ).fetchall()
        latest: dict[str, dict] = {}
        for row in rows:
            vault_id = row["vault_id"]
            version = int(row["state_version"])
            prev = latest.get(vault_id)
            if prev is not None and version <= prev["state_version"]:
                continue
            latest[vault_id] = {
                "id": row["id"],
                "vault_id": vault_id,
                "state_version": version,
                "reconciled_by": row["reconciled_by"],
                "importance": row["importance"],
                "spec": row["spec"] or "",
                "status": row["status"] or "",
            }
        return latest

    def vault_ids_named(self, name: str) -> list[str]:
        ids: list[str] = []
        for row in self.conn.execute(
            "SELECT id, path, extra FROM nodes WHERE type = 'Vault'"
        ).fetchall():
            extra_map = parse_extra_map(row["extra"])
            if row["path"] == name or extra_map.get("name") == name:
                ids.append(row["id"])
        return ids

    def _table_columns(self) -> dict[str, list[tuple[str, str]]]:
        tables = [
            row[0]
            for row in self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        ]
        out: dict[str, list[tuple[str, str]]] = {}
        for table in tables:
            cols = [
                (row[1], row[2] or "")
                for row in self.conn.execute(f"PRAGMA table_info({table})").fetchall()
            ]
            out[table] = cols
        return out

    def close(self) -> None:
        self.conn.close()
