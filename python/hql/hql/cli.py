"""CLI: python3 -m hql --db FILE [--format tsv|table|json] PIPELINE."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Iterable, Optional, TextIO

from hql.query import Query
from hql.row import Row


def main(argv: Optional[list[str]] = None, out: Optional[TextIO] = None) -> int:
    parser = argparse.ArgumentParser(prog="hql", description="HQL v0 read-only pipes")
    parser.add_argument("--db", required=True, help="HedronDB sqlite file")
    parser.add_argument(
        "--format",
        choices=("tsv", "table", "json"),
        default="tsv",
        help="output format (default tsv)",
    )
    parser.add_argument(
        "pipeline",
        nargs="+",
        help='pipeline, e.g. vault atrium-fixture | search "HedronDB"',
    )
    args = parser.parse_args(argv)
    pipeline = " ".join(args.pipeline)
    rows = Query.open(args.db).pipe(pipeline).run()
    stream = out or sys.stdout
    fields = _fields_of(rows)
    if args.format == "json":
        stream.write(json.dumps([row.as_dict(fields) for row in rows], ensure_ascii=True))
        stream.write("\n")
    elif args.format == "table":
        _write_table(stream, fields, rows)
    else:
        _write_tsv(stream, fields, rows)
    return 0


def _fields_of(rows: list[Row]) -> list[str]:
    if not rows:
        return []
    first = rows[0]
    if first._selected is not None:
        return list(first._selected.keys())
    return [
        "path",
        "from.path",
        "to.path",
        "to_id",
        "to_raw",
        "from_id",
    ]


def _cell(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _write_tsv(stream: TextIO, fields: list[str], rows: Iterable[Row]) -> None:
    if not fields:
        return
    stream.write("\t".join(fields) + "\n")
    for row in rows:
        stream.write("\t".join(_cell(row.get(field)) for field in fields) + "\n")


def _write_table(stream: TextIO, fields: list[str], rows: list[Row]) -> None:
    if not fields:
        return
    widths = [len(field) for field in fields]
    cells = [[_cell(row.get(field)) for field in fields] for row in rows]
    for line in cells:
        for i, value in enumerate(line):
            widths[i] = max(widths[i], len(value))
    stream.write("  ".join(field.ljust(widths[i]) for i, field in enumerate(fields)) + "\n")
    for line in cells:
        stream.write("  ".join(line[i].ljust(widths[i]) for i in range(len(fields))) + "\n")
