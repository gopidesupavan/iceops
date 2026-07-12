"""Trino engine: submits ALTER TABLE ... EXECUTE procedures over the Trino Python client.

Requires: pip install iceops[trino]
"""

from __future__ import annotations

import math
from typing import Any

from ..errors import IceopsError
from ..models import Action, ActionResult, Plan
from .spark import parse_engine_rows


def build_trino_compact_sql(action: Action) -> str:
    qualified = _qualified_table_for(action)
    target = int(action.params["target_file_size_bytes"])
    return (
        f"ALTER TABLE {qualified} EXECUTE optimize(file_size_threshold => '{_trino_size(target)}')"
    )


def build_trino_expire_sql(action: Action) -> str:
    qualified = _qualified_table_for(action)
    retention = _trino_duration(int(action.params["older_than_seconds"]))
    return f"ALTER TABLE {qualified} EXECUTE expire_snapshots(retention_threshold => '{retention}')"


def build_trino_clean_orphans_sql(action: Action) -> str:
    qualified = _qualified_table_for(action)
    retention = _trino_duration(int(action.params["older_than_seconds"]))
    return (
        f"ALTER TABLE {qualified} EXECUTE remove_orphan_files(retention_threshold => '{retention}')"
    )


def build_trino_rewrite_manifests_sql(action: Action) -> str:
    return f"ALTER TABLE {_qualified_table_for(action)} EXECUTE optimize_manifests"


TRINO_SQL_BUILDERS = {
    "compact": build_trino_compact_sql,
    "expire": build_trino_expire_sql,
    "clean_orphans": build_trino_clean_orphans_sql,
    "rewrite_manifests": build_trino_rewrite_manifests_sql,
}


def _qualified_table_for(action: Action) -> str:
    catalog = str(action.params.get("engine_catalog") or "")
    table = str(action.params.get("table") or action.table)
    return _qualified_table(catalog, table)


def _trino_duration(seconds: int) -> str:
    """Trino retention_threshold is a duration string; it hard-refuses values below the
    catalog's min-retention (default 7d) — the caller/test catalog must allow small
    values. Use seconds so 0s and 7d both round-trip exactly."""
    return f"{seconds}s"


def _qualified_table(catalog: str, table: str) -> str:
    parts = table.split(".")
    if catalog and (not parts or parts[0] != catalog):
        parts = [catalog, *parts]
    return ".".join(_quote_identifier(part) for part in parts)


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _trino_size(size_bytes: int) -> str:
    mb = max(1, math.ceil(size_bytes / (1024 * 1024)))
    return f"{mb}MB"


class TrinoEngine:
    name = "trino"

    def __init__(self, connection: Any | None = None, **connect_kwargs: Any) -> None:
        self._connection = connection
        self._connect_kwargs = connect_kwargs

    def execute(self, plan: Plan) -> list[ActionResult]:
        connection = self._connection or self._connect()
        results: list[ActionResult] = []
        for action in plan.actions:
            builder = TRINO_SQL_BUILDERS.get(action.op)
            if builder is None:
                raise IceopsError(f"trino engine cannot execute '{action.op}'")
            cursor = connection.cursor()
            cursor.execute(builder(action))
            rows = cursor.fetchall()
            results.append(
                ActionResult(action=action, status="submitted", details=parse_engine_rows(rows))
            )
        return results

    def _connect(self) -> Any:
        if not self._connect_kwargs:
            raise IceopsError(
                "trino engine requires connection settings; configure [engines.trino] "
                "or pass a connection"
            )
        try:
            import trino
        except Exception as exc:  # pragma: no cover - exercised without trino extra installed
            raise IceopsError(
                "trino engine requires trino; install with `pip install iceops[trino]`"
            ) from exc
        return trino.dbapi.connect(**self._connect_kwargs)
