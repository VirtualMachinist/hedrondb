"""Query rows: a node, or a traverse walk (from + edge + optional to)."""

from __future__ import annotations

from typing import Any, Optional


def parse_extra_map(extra: Optional[str]) -> dict[str, Optional[str]]:
    """Parse YAML-ish `key: value` lines. Missing / empty values are None."""
    mapping: dict[str, Optional[str]] = {}
    if not extra:
        return mapping
    for raw_line in extra.splitlines():
        line = raw_line.strip()
        if not line or line in ("---", "...") or line.startswith("#"):
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        if not key or key.startswith("-") or any(ch.isspace() for ch in key):
            continue
        mapping[key] = _unquote(value.strip())
    return mapping


def _unquote(value: str) -> Optional[str]:
    if value == "" or value in ("null", "~", "Null", "NULL"):
        return None
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        inner = value[1:-1]
        return inner if inner else None
    return value


class Row:
    """One pipeline row. Missing extra keys and absent walk fields are None."""

    def __init__(
        self,
        *,
        path: Optional[str] = None,
        extra: Optional[str] = None,
        extra_map: Optional[dict[str, Optional[str]]] = None,
        node_id: Optional[str] = None,
        vault_id: Optional[str] = None,
        node_type: Optional[str] = None,
        from_id: Optional[str] = None,
        from_path: Optional[str] = None,
        to_id: Optional[str] = None,
        to_raw: Optional[str] = None,
        to_path: Optional[str] = None,
        edge_type: Optional[str] = None,
        properties: Optional[str] = None,
        spec: Optional[str] = None,
        status: Optional[str] = None,
        state_version: Optional[int] = None,
        reconciled_by: Optional[str] = None,
        importance: Optional[float] = None,
        state_id: Optional[str] = None,
        selected: Optional[dict[str, Any]] = None,
    ) -> None:
        self.path = path
        self.extra = extra or ""
        self.extra_map = extra_map if extra_map is not None else parse_extra_map(extra)
        self.node_id = node_id
        self.vault_id = vault_id
        self.node_type = node_type
        self.from_id = from_id
        self.from_path = from_path
        self.to_id = to_id
        self.to_raw = to_raw
        self.to_path = to_path
        self.edge_type = edge_type
        self.properties = properties or ""
        self.spec = spec
        self.status = status
        self.state_version = state_version
        self.reconciled_by = reconciled_by
        self.importance = importance
        self.state_id = state_id
        self._selected = selected

    def get(self, field: str) -> Any:
        if self._selected is not None:
            return self._selected.get(field)
        return self._raw_get(field)

    def _raw_get(self, field: str) -> Any:
        if field.startswith("extra."):
            return self.extra_map.get(field[6:])
        if field == "path":
            return self.path
        if field == "to_id":
            return self.to_id
        if field == "to_raw":
            return self.to_raw
        if field == "from_id":
            return self.from_id
        if field == "from.path":
            return self.from_path
        if field == "to.path":
            return self.to_path
        if field == "id":
            return self.state_id if self.state_id is not None else self.node_id
        if field == "vault_id":
            return self.vault_id
        if field == "node_type":
            return self.node_type
        if field == "type":
            return self.edge_type
        if field == "spec":
            return self.spec
        if field == "status":
            return self.status
        if field == "state_version":
            return self.state_version
        if field == "reconciled_by":
            return self.reconciled_by
        if field == "importance":
            return self.importance
        return None

    def with_state(self, ds: dict[str, Any]) -> "Row":
        """Warm overlay: latest desired_states fields. Does not touch events."""
        return Row(
            path=self.path,
            extra=self.extra,
            extra_map=self.extra_map,
            node_id=self.node_id,
            vault_id=self.vault_id,
            node_type=self.node_type,
            from_id=self.from_id,
            from_path=self.from_path,
            to_id=self.to_id,
            to_raw=self.to_raw,
            to_path=self.to_path,
            edge_type=self.edge_type,
            properties=self.properties,
            spec=ds.get("spec"),
            status=ds.get("status"),
            state_version=ds.get("state_version"),
            reconciled_by=ds.get("reconciled_by"),
            importance=ds.get("importance"),
            state_id=ds.get("id"),
        )

    def project(self, fields: list[str]) -> "Row":
        selected = {field: self._raw_get(field) for field in fields}
        return Row(
            path=self.path,
            extra=self.extra,
            extra_map=self.extra_map,
            node_id=self.node_id,
            vault_id=self.vault_id,
            node_type=self.node_type,
            from_id=self.from_id,
            from_path=self.from_path,
            to_id=self.to_id,
            to_raw=self.to_raw,
            to_path=self.to_path,
            edge_type=self.edge_type,
            properties=self.properties,
            spec=self.spec,
            status=self.status,
            state_version=self.state_version,
            reconciled_by=self.reconciled_by,
            importance=self.importance,
            state_id=self.state_id,
            selected=selected,
        )

    def as_dict(self, fields: Optional[list[str]] = None) -> dict[str, Any]:
        if self._selected is not None:
            return dict(self._selected)
        if fields is None:
            fields = [
                "path",
                "from.path",
                "to.path",
                "to_id",
                "to_raw",
                "from_id",
            ]
        return {field: self._raw_get(field) for field in fields}

    def __getitem__(self, field: str) -> Any:
        return self.get(field)

    def searchable_text(self) -> str:
        parts = [self.path or "", self.extra, self.to_raw or "", self.properties]
        return "\n".join(parts)
