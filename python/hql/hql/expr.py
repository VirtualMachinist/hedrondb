"""HQL v0 filter expressions: == != ^= !^= with and / &&. null literal."""

from __future__ import annotations

from typing import Any, Optional, Union

from hql.row import Row

_OPS = ("!^=", "==", "!=", "^=")


class FilterError(ValueError):
    pass


def parse_filter(expr: str) -> "FilterExpr":
    tokens = _tokenize(expr)
    if not tokens:
        raise FilterError("empty filter")
    return _parse_and(tokens)


def _parse_and(tokens: list[str]) -> "FilterExpr":
    parts: list[Comparison] = [_parse_comparison(tokens)]
    while tokens:
        join = tokens.pop(0)
        if join not in ("and", "&&"):
            raise FilterError(f"expected 'and' or '&&', got {join!r}")
        parts.append(_parse_comparison(tokens))
    if len(parts) == 1:
        return parts[0]
    return And(parts)


def _parse_comparison(tokens: list[str]) -> "Comparison":
    if len(tokens) < 3:
        raise FilterError("expected field OP value")
    field = tokens.pop(0)
    op = tokens.pop(0)
    if op not in _OPS:
        raise FilterError(f"unknown operator {op!r}")
    raw = tokens.pop(0)
    if raw == "null":
        value: Optional[str] = None
    else:
        value = raw
    return Comparison(field, op, value)


def _tokenize(expr: str) -> list[str]:
    tokens: list[str] = []
    i = 0
    n = len(expr)
    while i < n:
        ch = expr[i]
        if ch.isspace():
            i += 1
            continue
        if expr.startswith("&&", i):
            tokens.append("&&")
            i += 2
            continue
        matched_op = next((op for op in _OPS if expr.startswith(op, i)), None)
        if matched_op:
            tokens.append(matched_op)
            i += len(matched_op)
            continue
        if ch in ('"', "'"):
            quote = ch
            i += 1
            buf: list[str] = []
            while i < n and expr[i] != quote:
                if expr[i] == "\\" and i + 1 < n:
                    buf.append(expr[i + 1])
                    i += 2
                    continue
                buf.append(expr[i])
                i += 1
            if i >= n:
                raise FilterError("unterminated string")
            i += 1
            tokens.append("".join(buf))
            continue
        start = i
        while i < n and not expr[i].isspace() and not _starts_op(expr, i):
            i += 1
        word = expr[start:i]
        if word:
            tokens.append(word)
    return tokens


def _starts_op(expr: str, i: int) -> bool:
    if expr.startswith("&&", i):
        return True
    return any(expr.startswith(op, i) for op in _OPS)


class Comparison:
    def __init__(self, field: str, op: str, value: Optional[str]) -> None:
        self.field = field
        self.op = op
        self.value = value

    def eval(self, row: Row) -> bool:
        left = row.get(self.field) if row._selected is not None else row._raw_get(self.field)
        return _compare(left, self.op, self.value)


class And:
    def __init__(self, parts: list[Comparison]) -> None:
        self.parts = parts

    def eval(self, row: Row) -> bool:
        return all(part.eval(row) for part in self.parts)


FilterExpr = Union[Comparison, And]


def _compare(left: Any, op: str, right: Optional[str]) -> bool:
    if op == "==":
        if right is None:
            return left is None or left == ""
        return left is not None and str(left) == right
    if op == "!=":
        if right is None:
            return left is not None and left != ""
        return left is None or str(left) != right
    left_s = "" if left is None else str(left)
    if right is None:
        return False
    if op == "^=":
        return left is not None and left_s.startswith(right)
    if op == "!^=":
        return left is None or not left_s.startswith(right)
    return False
