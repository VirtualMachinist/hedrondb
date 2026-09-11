"""Read-only HedronDB store. Opens sqlite via URI mode=ro; never writes."""

from __future__ import annotations

import sqlite3
from functools import lru_cache
from pathlib import Path
from typing import Optional
from urllib.parse import quote

from hql.row import Row, parse_extra_map

# Crate-root schema.sql is the only schema source (copy of vault store.sql).
# The Python twin reads that file; it never carries its own column list.
SCHEMA_SQL_PATH = Path(__file__).resolve().parents[3] / "schema.sql"

_CONSTRAINT_KEYWORDS = {"UNIQUE", "PRIMARY", "FOREIGN", "CHECK", "CONSTRAINT"}


def schema_sql() -> str:
    """Contents of crate-root schema.sql (executed by Store and by fixtures)."""
    return SCHEMA_SQL_PATH.read_text(encoding="utf-8")


def _split_top_level(body: str) -> list[str]:
    """Split on commas outside parentheses so UNIQUE (a, b) stays one piece."""
    pieces: list[str] = []
    depth = 0
    start = 0
    for i, ch in enumerate(body):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(depth - 1, 0)
        elif ch == "," and depth == 0:
            pieces.append(body[start:i])
            start = i + 1
    pieces.append(body[start:])
    return pieces


def parse_schema_sql(sql: str) -> dict[str, list[tuple[str, str]]]:
    """Table name -> (column, declared type) pairs in declaration order.

    Table constraints (UNIQUE, PRIMARY KEY, ...) are not columns and are
    skipped; `--` comments are stripped first. Mirrors the Rust reader.
    """
    stripped = "\n".join(line.split("--", 1)[0] for line in sql.splitlines())
    tables: dict[str, list[tuple[str, str]]] = {}
    rest = stripped
    while True:
        start = rest.find("CREATE TABLE")
        if start < 0:
            break
        after = rest[start + len("CREATE TABLE") :]
        open_ = after.find("(")
        close = after.find(");")
        if open_ < 0 or close < 0:
            break
        name = after[:open_].split()[-1]
        cols: list[tuple[str, str]] = []
        for piece in _split_top_level(after[open_ + 1 : close]):
            words = piece.split()
            if not words or words[0].upper() in _CONSTRAINT_KEYWORDS:
                continue
            cols.append((words[0], words[1] if len(words) > 1 else ""))
        tables[name] = cols
        rest = after[close + 2 :]
    return tables


@lru_cache(maxsize=1)
def expected_schema() -> dict[str, list[tuple[str, str]]]:
    return parse_schema_sql(schema_sql())


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
        expected = expected_schema()
        for table, cols in expected.items():
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
            if table not in expected:
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

    def causal_chain(self, desired_state_id: str) -> list[dict]:
        """Cool path: events that reconcile this Desired State. No spec/status/data."""
        rows = self.conn.execute(
            "SELECT id, vault_id, ts, actor, type, caused_by, reconciles, supersedes "
            "FROM events "
            "WHERE reconciles = ? "
            "ORDER BY ts ASC, id ASC",
            (desired_state_id,),
        ).fetchall()
        return [
            {
                "id": row["id"],
                "vault_id": row["vault_id"],
                "ts": int(row["ts"]),
                "actor": row["actor"],
                "type": row["type"],
                "caused_by": _format_caused_by(row["caused_by"]),
                "reconciles": row["reconciles"],
                "supersedes": row["supersedes"],
            }
            for row in rows
        ]

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


def _format_caused_by(raw: Optional[str]) -> Optional[str]:
    text = (raw or "").strip()
    if not text or text in ("[]", "null", "~"):
        return None
    ids: list[str] = []
    for line in text.splitlines():
        line = line.strip().strip(",")
        if line.startswith("- "):
            ids.append(line[2:].strip().strip("'").strip('"'))
        elif line.startswith("[") and line.endswith("]"):
            inner = line[1:-1].strip()
            if inner:
                ids.extend(
                    part.strip().strip("'").strip('"')
                    for part in inner.split(",")
                    if part.strip()
                )
    return ",".join(ids) if ids else None
