"""HQL v0 pipeline: vault | agent | state | history | search | traverse | filter | select | limit."""

from __future__ import annotations

from typing import Optional

from hql.expr import parse_filter
from hql.row import Row
from hql.store import Store


class Query:
    def __init__(self, store: Store) -> None:
        self._store = store
        self._ops: list[tuple] = []

    @classmethod
    def open(cls, db: str) -> "Query":
        return cls(Store(db))

    def vault(self, name: str) -> "Query":
        self._ops.append(("vault", name))
        return self

    def agent(self, name: str) -> "Query":
        self._ops.append(("agent", name))
        return self

    def state(self) -> "Query":
        self._ops.append(("state",))
        return self

    def history(self) -> "Query":
        self._ops.append(("history",))
        return self

    def search(self, text: str) -> "Query":
        self._ops.append(("search", text))
        return self

    def traverse(self, edge: str, hops: int = 1) -> "Query":
        self._ops.append(("traverse", edge, hops))
        return self

    def filter(self, expr: str) -> "Query":
        self._ops.append(("filter", expr))
        return self

    def select(self, *fields: str) -> "Query":
        parsed: list[str] = []
        for field in fields:
            parsed.extend(_split_fields(field))
        self._ops.append(("select", parsed))
        return self

    def limit(self, n: int) -> "Query":
        self._ops.append(("limit", int(n)))
        return self

    def pipe(self, pipeline: str) -> "Query":
        for stage in split_pipeline(pipeline):
            self._apply_stage(stage)
        return self

    def run(self) -> list[Row]:
        nodes = self._store.nodes()
        edges = self._store.edges()
        by_id = {row.node_id: row for row in nodes if row.node_id}
        outgoing: dict[str, list[dict]] = {}
        for edge in edges:
            outgoing.setdefault(edge["from_id"] or "", []).append(edge)

        rows = list(nodes)
        for op in self._ops:
            kind = op[0]
            if kind == "vault":
                vault_ids = set(self._store.vault_ids_named(op[1]))
                rows = [row for row in rows if row.vault_id in vault_ids]
            elif kind == "agent":
                rows = _filter_agents(rows, op[1])
            elif kind == "state":
                rows = _apply_state(rows, self._store.latest_desired_states())
            elif kind == "history":
                rows = _apply_history(rows, self._store)
            elif kind == "search":
                needle = op[1]
                rows = [row for row in rows if _search_hit(row, needle, outgoing, by_id)]
            elif kind == "traverse":
                rows = _traverse(rows, op[1], op[2], outgoing, by_id)
            elif kind == "filter":
                expr = parse_filter(op[1])
                rows = [row for row in rows if expr.eval(row)]
            elif kind == "select":
                rows = [row.project(op[1]) for row in rows]
            elif kind == "limit":
                rows = rows[: op[1]]
            else:
                raise ValueError(f"unknown operator {kind!r}")
        return rows

    def _apply_stage(self, stage: str) -> None:
        stage = stage.strip()
        if not stage:
            return
        name, _, rest = stage.partition(" ")
        rest = rest.strip()
        if name == "vault":
            self.vault(_unquote_arg(rest))
        elif name == "agent":
            if not rest:
                raise ValueError("agent requires a name")
            self.agent(_unquote_arg(rest))
        elif name == "state":
            if rest:
                raise ValueError("state takes no arguments")
            self.state()
        elif name == "history":
            if rest:
                raise ValueError("history takes no arguments")
            self.history()
        elif name == "search":
            self.search(_unquote_arg(rest))
        elif name == "traverse":
            edge, hops = _parse_traverse(rest)
            self.traverse(edge, hops)
        elif name == "filter":
            self.filter(rest)
        elif name == "select":
            self.select(rest)
        elif name == "limit":
            self.limit(int(rest))
        else:
            raise ValueError(f"unknown operator {name!r}")


def run_pipeline(db: str, pipeline: str) -> list[Row]:
    return Query.open(db).pipe(pipeline).run()


def split_pipeline(pipeline: str) -> list[str]:
    stages: list[str] = []
    buf: list[str] = []
    quote: Optional[str] = None
    i = 0
    while i < len(pipeline):
        ch = pipeline[i]
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
            elif ch == "\\" and i + 1 < len(pipeline):
                buf.append(pipeline[i + 1])
                i += 1
            i += 1
            continue
        if ch in ('"', "'"):
            quote = ch
            buf.append(ch)
            i += 1
            continue
        if ch == "|":
            stages.append("".join(buf).strip())
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        stages.append(tail)
    return stages


def _search_hit(
    row: Row,
    needle: str,
    outgoing: dict[str, list[dict]],
    by_id: dict[str, Row],
) -> bool:
    if needle in row.searchable_text():
        return True
    source_id = row.node_id or row.from_id
    if not source_id:
        return False
    for edge in outgoing.get(source_id, []):
        blob = f"{edge.get('to_raw') or ''}\n{edge.get('properties') or ''}"
        if needle in blob:
            return True
        to_id = edge.get("to_id")
        if to_id and to_id in by_id and needle in by_id[to_id].searchable_text():
            return True
    return False


def _traverse(
    rows: list[Row],
    edge_type: str,
    hops: int,
    outgoing: dict[str, list[dict]],
    by_id: dict[str, Row],
) -> list[Row]:
    if hops < 1:
        raise ValueError("traverse --hops must be >= 1")
    frontier: list[Row] = []
    for row in rows:
        if row.node_id:
            frontier.append(row)
        elif row.to_id and row.to_id in by_id:
            frontier.append(by_id[row.to_id])
    emitted: list[Row] = []
    seen_edges: set[str] = set()
    for _ in range(hops):
        nxt: list[Row] = []
        for src in frontier:
            if not src.node_id:
                continue
            for edge in outgoing.get(src.node_id, []):
                if edge["type"] != edge_type:
                    continue
                edge_id = edge["id"]
                if edge_id in seen_edges:
                    continue
                seen_edges.add(edge_id)
                dest = by_id.get(edge["to_id"]) if edge["to_id"] else None
                walk = Row(
                    path=src.path,
                    extra=src.extra,
                    extra_map=src.extra_map,
                    node_id=src.node_id,
                    vault_id=src.vault_id,
                    node_type=src.node_type,
                    from_id=edge["from_id"],
                    from_path=src.path,
                    to_id=edge["to_id"],
                    to_raw=edge["to_raw"],
                    to_path=dest.path if dest else None,
                    edge_type=edge["type"],
                    properties=edge["properties"],
                )
                emitted.append(walk)
                if dest is not None:
                    nxt.append(dest)
        frontier = nxt
    return emitted


def _filter_agents(rows: list[Row], name: str) -> list[Row]:
    """Keep Agent rows matching extra.name, extra.title, or path. Stay in current slice."""
    agents = [row for row in rows if row.node_type == "Agent"]
    exact = [row for row in agents if _agent_exact(row, name)]
    if exact:
        return exact
    return [row for row in agents if _agent_substring(row, name)]


def _agent_exact(row: Row, name: str) -> bool:
    extra_name = row.extra_map.get("name")
    extra_title = row.extra_map.get("title")
    if extra_name == name or extra_title == name:
        return True
    path = row.path or ""
    if path == name:
        return True
    return _path_basename(path) == name


def _agent_substring(row: Row, name: str) -> bool:
    extra_name = row.extra_map.get("name") or ""
    extra_title = row.extra_map.get("title") or ""
    path = row.path or ""
    return name in extra_name or name in extra_title or name in path


def _path_basename(path: str) -> str:
    if not path:
        return ""
    return path.rsplit("/", 1)[-1]


def _apply_state(rows: list[Row], latest: dict[str, dict]) -> list[Row]:
    """Warm only: latest desired_states per vault_id. Does not read the event log."""
    emitted: list[Row] = []
    for row in rows:
        if row.node_type not in ("Agent", "Vault"):
            continue
        ds = latest.get(row.vault_id or "")
        if ds is None:
            continue
        emitted.append(row.with_state(ds))
    return emitted


def _apply_history(rows: list[Row], store: Store) -> list[Row]:
    """Cool only: causal_chain for the latest Desired State. No spec/status/data."""
    latest = store.latest_desired_states()
    emitted: list[Row] = []
    for row in rows:
        if row.node_type not in ("Agent", "Vault"):
            continue
        ds = latest.get(row.vault_id or "")
        if ds is None:
            continue
        for event in store.causal_chain(ds["id"]):
            emitted.append(Row.from_history(event))
    return emitted


def _parse_traverse(rest: str) -> tuple[str, int]:
    tokens = _tokenize_args(rest)
    edge: Optional[str] = None
    hops = 1
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok == "--edge" and i + 1 < len(tokens):
            edge = _unquote_arg(tokens[i + 1])
            i += 2
            continue
        if tok == "--hops" and i + 1 < len(tokens):
            hops = int(tokens[i + 1])
            i += 2
            continue
        raise ValueError(f"unknown traverse argument {tok!r}")
    if not edge:
        raise ValueError("traverse requires --edge")
    return edge, hops


def _tokenize_args(text: str) -> list[str]:
    tokens: list[str] = []
    buf: list[str] = []
    quote: Optional[str] = None
    for ch in text:
        if quote:
            if ch == quote:
                quote = None
            else:
                buf.append(ch)
            continue
        if ch in ('"', "'"):
            quote = ch
            continue
        if ch.isspace():
            if buf:
                tokens.append("".join(buf))
                buf = []
            continue
        buf.append(ch)
    if buf:
        tokens.append("".join(buf))
    return tokens


def _unquote_arg(text: str) -> str:
    text = text.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in ('"', "'"):
        return text[1:-1]
    return text


def _split_fields(text: str) -> list[str]:
    return [part.strip() for part in text.split(",") if part.strip()]
