"""Pure policy resolution: the `when:` mini-language and per-table policy merge.

No I/O, no catalog — plain functions over the schema and TableMetrics, so the whole thing
is unit-testable with a matrix. The `when:` language is deliberately tiny (one comparison,
no eval, no dependency): predictability beats power in a config that deletes files.
"""

from __future__ import annotations

import fnmatch
import re
from dataclasses import dataclass
from typing import Callable, Optional

from ..errors import IceopsError
from ..models import TableMetrics
from .schema import PolicySpec, PolicyDoc

# comparison operators, longest first so '>=' is matched before '>'
_OPS: dict[str, Callable[[float, float], bool]] = {
    ">=": lambda a, b: a >= b,
    "<=": lambda a, b: a <= b,
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
    ">": lambda a, b: a > b,
    "<": lambda a, b: a < b,
}
_WHEN_RE = re.compile(r"^\s*([a-z][a-z0-9-]*)\s*(>=|<=|==|!=|>|<)\s*(-?\d+(?:\.\d+)?)\s*$")

# metric name (kebab) -> TableMetrics field (snake); only numeric fields are conditionable
_METRIC_FIELDS = {
    name.replace("_", "-"): name
    for name, field in TableMetrics.model_fields.items()
    if field.annotation in (int, float, Optional[int], Optional[float])
}


@dataclass(frozen=True)
class Condition:
    metric: str  # kebab-case, as written
    field: str  # snake-case TableMetrics attribute
    op: str
    value: float

    def evaluate(self, metrics: TableMetrics) -> bool:
        actual = getattr(metrics, self.field, None)
        if actual is None:  # unknown metric value → never act on unknown
            return False
        return _OPS[self.op](float(actual), self.value)

    def describe(self, metrics: TableMetrics) -> str:
        actual = getattr(metrics, self.field, None)
        return f"{self.metric} {actual} {self.op} {self.value:g}"


def parse_when(expr: str) -> Condition:
    """'small-file-ratio > 0.3' -> Condition. Raises on syntax or unknown-metric errors
    (at load time, not at 2am)."""
    match = _WHEN_RE.match(expr)
    if not match:
        raise IceopsError(
            f"invalid when expression '{expr}' (expected 'metric OP number', "
            f"OP one of > >= < <= == !=)"
        )
    metric, op, number = match.groups()
    if metric not in _METRIC_FIELDS:
        known = ", ".join(sorted(_METRIC_FIELDS))
        raise IceopsError(f"unknown metric '{metric}' in when expression. Known: {known}")
    return Condition(metric=metric, field=_METRIC_FIELDS[metric], op=op, value=float(number))


def validate_policy(doc: PolicyDoc) -> None:
    """Parse every `when:` in the document so typos fail when the file loads."""
    for spec in (doc.defaults, *doc.tables.values()):
        for op_policy in _op_policies(spec):
            when = getattr(op_policy, "when", None)
            if when is not None:
                parse_when(when)


@dataclass
class ResolvedPolicy:
    identifier: str
    engine: Optional[str]
    spec: PolicySpec  # merged op policies


def _specificity(pattern: str) -> tuple[int, int]:
    """More specific patterns win later in the merge. Exact names beat globs; among
    globs, more literal characters wins."""
    is_glob = any(c in pattern for c in "*?[")
    literal = len(pattern.replace("*", "").replace("?", ""))
    return (0 if is_glob else 1, literal)


def resolve(doc: PolicyDoc, identifier: str) -> Optional[ResolvedPolicy]:
    """Merge defaults + matching table overrides (least specific first). None if the table
    is disabled or matches no scope at all."""
    matches = [
        (pattern, tp) for pattern, tp in doc.tables.items() if fnmatch.fnmatch(identifier, pattern)
    ]
    if any(tp.disabled for _, tp in matches):
        return None
    # a table is in scope if defaults define any op, or it has an explicit entry
    if not matches and not _has_any_op(doc.defaults):
        return None

    matches.sort(key=lambda pair: _specificity(pair[0]))
    engine = doc.engine
    merged = _clone_spec(doc.defaults)
    for _, table_policy in matches:
        _merge_into(merged, table_policy)
        if table_policy.engine is not None:
            engine = table_policy.engine
    return ResolvedPolicy(identifier=identifier, engine=engine, spec=merged)


def _op_policies(spec: PolicySpec) -> list[object]:
    return [
        p
        for p in (spec.compact, spec.rewrite_manifests, spec.expire_snapshots, spec.clean_orphans)
        if p is not None
    ]


def _has_any_op(spec: PolicySpec) -> bool:
    return bool(_op_policies(spec))


_OP_ATTRS = ("compact", "rewrite_manifests", "expire_snapshots", "clean_orphans")


def _clone_spec(spec: PolicySpec) -> PolicySpec:
    return PolicySpec.model_validate(spec.model_dump())


def _merge_into(base: PolicySpec, override: PolicySpec) -> None:
    """Per-op deep merge: an override op replaces base's op, but merges field-by-field so
    a table setting only retain-last keeps defaults' older-than."""
    for attr in _OP_ATTRS:
        over = getattr(override, attr)
        if over is None:
            continue
        cur = getattr(base, attr)
        if cur is None:
            setattr(base, attr, over.model_copy())
        else:
            merged = cur.model_dump()
            merged.update({k: v for k, v in over.model_dump(exclude_unset=True).items()})
            setattr(base, attr, type(cur).model_validate(merged))
