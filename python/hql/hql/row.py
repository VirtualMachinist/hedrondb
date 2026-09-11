"""Query rows: a node, a traverse walk, a warm desired state, or a cool event."""

from __future__ import annotations

import re
from typing import Any, Optional

import yaml


class _CoreLoader(yaml.SafeLoader):
    """SafeLoader with YAML 1.2 core-schema scalars, matching serde_yaml.

    PyYAML defaults to YAML 1.1 (yes/no booleans, timestamps, octal 017,
    underscores in numbers). The Rust twin has none of that, so the resolvers
    are replaced: null, true/false, decimal / 0x / 0o / 0b ints, floats.
    """


_CoreLoader.yaml_implicit_resolvers = {}
_CoreLoader.add_implicit_resolver(
    "tag:yaml.org,2002:null", re.compile(r"^(?:~|null|Null|NULL|)$"), ["~", "n", "N", ""]
)
_CoreLoader.add_implicit_resolver(
    "tag:yaml.org,2002:bool",
    re.compile(r"^(?:true|True|TRUE|false|False|FALSE)$"),
    list("tTfF"),
)
_CoreLoader.add_implicit_resolver(
    "tag:yaml.org,2002:int",
    re.compile(r"^(?:[-+]?[0-9]+|0x[0-9a-fA-F]+|0o[0-7]+|0b[01]+)$"),
    list("-+0123456789"),
)
_CoreLoader.add_implicit_resolver(
    "tag:yaml.org,2002:float",
    re.compile(
        r"^(?:[-+]?(?:\.[0-9]+|[0-9]+(?:\.[0-9]*)?)(?:[eE][-+]?[0-9]+)?"
        r"|[-+]?\.(?:inf|Inf|INF)|\.(?:nan|NaN|NAN))$"
    ),
    list("-+0123456789."),
)


def _construct_core_int(loader: yaml.SafeLoader, node: yaml.Node) -> int:
    text = loader.construct_scalar(node)
    sign = -1 if text.startswith("-") else 1
    body = text.lstrip("+-")
    if body.startswith("0x"):
        return sign * int(body[2:], 16)
    if body.startswith("0o"):
        return sign * int(body[2:], 8)
    if body.startswith("0b"):
        return sign * int(body[2:], 2)
    return sign * int(body, 10)


_CoreLoader.add_constructor("tag:yaml.org,2002:int", _construct_core_int)


def parse_extra(extra: Optional[str]) -> dict[str, Optional[str]]:
    """`nodes.extra` as a YAML mapping, every value rendered to text.

    Top-level null is None. Scalars are themselves; sequences / mappings
    render in YAML flow style (`[a, b]`, `{k: v}`) — `extra.k` v0 has no JSON
    path. Non-mapping or invalid YAML is an empty mapping.
    """
    if not extra:
        return {}
    try:
        loaded = yaml.load(extra, Loader=_CoreLoader)
    except yaml.YAMLError:
        return {}
    if not isinstance(loaded, dict):
        return {}
    return {_flow(key): (None if value is None else _flow(value)) for key, value in loaded.items()}


def _flow(value: Any) -> str:
    """YAML flow rendering shared with the Rust twin."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return _float_text(value)
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_flow(item) for item in value) + "]"
    if isinstance(value, dict):
        return "{" + ", ".join(f"{_flow(k)}: {_flow(v)}" for k, v in value.items()) + "}"
    return str(value)


def _float_text(value: float) -> str:
    if value != value:
        return "nan"
    if value in (float("inf"), float("-inf")):
        return "inf" if value > 0 else "-inf"
    return str(value)


class Row:
    """One pipeline row. Missing extra keys and absent fields are None."""

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
        name: Optional[str] = None,
        state_version: Optional[int] = None,
        reconciled_by: Optional[str] = None,
        importance: Optional[float] = None,
        state_id: Optional[str] = None,
        event_id: Optional[str] = None,
        ts: Optional[int] = None,
        actor: Optional[str] = None,
        event_type: Optional[str] = None,
        caused_by: Optional[str] = None,
        reconciles: Optional[str] = None,
        supersedes: Optional[str] = None,
        selected: Optional[dict[str, Any]] = None,
    ) -> None:
        self.path = path
        self.extra = extra or ""
        self.extra_map = extra_map if extra_map is not None else parse_extra(extra)
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
        self.name = name
        self.state_version = state_version
        self.reconciled_by = reconciled_by
        self.importance = importance
        self.state_id = state_id
        self.event_id = event_id
        self.ts = ts
        self.actor = actor
        self.event_type = event_type
        self.caused_by = caused_by
        self.reconciles = reconciles
        self.supersedes = supersedes
        self._selected = selected

    # Row kind, mirroring the Rust enum: history / state / walk / node.
    def is_history(self) -> bool:
        return self.event_id is not None

    def is_state(self) -> bool:
        return self.state_id is not None

    def get(self, field: str) -> Any:
        if self._selected is not None:
            return self._selected.get(field)
        return self._raw_get(field)

    def _raw_get(self, field: str) -> Any:
        if self.is_history():
            return {
                "id": self.event_id,
                "vault_id": self.vault_id,
                "ts": self.ts,
                "actor": self.actor,
                "type": self.event_type,
                "caused_by": self.caused_by,
                "reconciles": self.reconciles,
                "supersedes": self.supersedes,
            }.get(field)
        if self.is_state():
            warm = {
                "id": self.state_id,
                "vault_id": self.vault_id,
                "name": self.name,
                "state_version": self.state_version,
                "reconciled_by": self.reconciled_by,
                "importance": self.importance,
                "spec": self.spec,
                "status": self.status,
            }
            if field in warm:
                return warm[field]
        if field.startswith("extra."):
            return self.extra_map.get(field[6:])
        return {
            "path": self.path,
            "id": self.node_id,
            "vault_id": self.vault_id,
            "node_type": self.node_type,
            "from_id": self.from_id,
            "from.path": self.from_path,
            "to_id": self.to_id,
            "to_raw": self.to_raw,
            "to.path": self.to_path,
            "type": self.edge_type,
        }.get(field)

    def with_state(self, ds: dict[str, Any]) -> "Row":
        """Warm overlay: this node as subject of a named desired state."""
        return Row(
            path=self.path,
            extra=self.extra,
            extra_map=self.extra_map,
            node_id=self.node_id,
            vault_id=ds.get("vault_id"),
            node_type=self.node_type,
            spec=ds.get("spec"),
            status=ds.get("status"),
            name=ds.get("name"),
            state_version=ds.get("state_version"),
            reconciled_by=ds.get("reconciled_by"),
            importance=ds.get("importance"),
            state_id=ds.get("id"),
        )

    @classmethod
    def state_only(cls, ds: dict[str, Any]) -> "Row":
        """Warm row with no subject node (`state NAME` over a document slice)."""
        return cls(
            vault_id=ds.get("vault_id"),
            spec=ds.get("spec"),
            status=ds.get("status"),
            name=ds.get("name"),
            state_version=ds.get("state_version"),
            reconciled_by=ds.get("reconciled_by"),
            importance=ds.get("importance"),
            state_id=ds.get("id"),
        )

    @classmethod
    def from_history(cls, event: dict[str, Any]) -> "Row":
        """Cool chain row. No spec/status/data."""
        return cls(
            vault_id=event.get("vault_id"),
            event_id=event.get("id"),
            ts=event.get("ts"),
            actor=event.get("actor"),
            event_type=event.get("event_type") or event.get("type"),
            caused_by=event.get("caused_by"),
            reconciles=event.get("reconciles"),
            supersedes=event.get("supersedes"),
        )

    def project(self, fields: list[str]) -> "Row":
        row = self._copy()
        row._selected = {field: self._raw_get(field) for field in fields}
        return row

    def _copy(self) -> "Row":
        row = Row.__new__(Row)
        row.__dict__.update(self.__dict__)
        row._selected = None
        return row

    def default_fields(self) -> list[str]:
        if self.is_history():
            return default_history_fields()
        if self.is_state():
            return default_state_fields()
        return default_output_fields()

    def as_dict(self, fields: Optional[list[str]] = None) -> dict[str, Any]:
        if self._selected is not None:
            return dict(self._selected)
        if fields is None:
            fields = self.default_fields()
        return {field: self._raw_get(field) for field in fields}

    def __getitem__(self, field: str) -> Any:
        return self.get(field)

    def searchable_text(self) -> str:
        parts = [self.path or "", self.extra, self.to_raw or "", self.properties]
        return "\n".join(parts)


def default_output_fields() -> list[str]:
    return ["path", "from.path", "to.path", "to_id", "to_raw", "from_id"]


def default_state_fields() -> list[str]:
    return ["path", "name", "state_version", "reconciled_by", "importance", "id"]


def default_history_fields() -> list[str]:
    return ["id", "ts", "actor", "type", "caused_by", "reconciles", "supersedes"]
