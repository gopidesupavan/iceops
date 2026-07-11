"""Spark engine: submits CALL system.rewrite_data_files(...) to the user's Spark.

Requires: pip install iceops[spark]
"""

from __future__ import annotations

from typing import Any

from ..errors import IceopsError
from ..models import Action, ActionResult, Plan


def build_spark_compact_sql(action: Action) -> str:
    if action.op != "compact":
        raise IceopsError(f"spark engine cannot execute '{action.op}'")
    catalog = str(action.params.get("engine_catalog") or "")
    table = str(action.params.get("table") or action.table)
    target = int(action.params["target_file_size_bytes"])
    if not catalog:
        raise IceopsError("spark compact action is missing engine_catalog")
    return (
        f"CALL {_quote_identifier(catalog)}.system.rewrite_data_files("
        f"table => {_sql_string(_spark_table_arg(catalog, table))}, "
        "options => map("
        f"'target-file-size-bytes', '{target}', "
        "'min-input-files', '2'"
        ")"
        ")"
    )


def parse_engine_rows(rows: list[Any]) -> dict[str, Any]:
    details: dict[str, Any] = {}
    for i, row in enumerate(rows):
        if hasattr(row, "asDict"):
            data = row.asDict(recursive=True)
        elif isinstance(row, dict):
            data = row
        else:
            values = list(row) if isinstance(row, tuple) else [row]
            data = {f"col{j}": value for j, value in enumerate(values)}

        if {"metric_name", "metric_value"} <= set(data):
            details[str(data["metric_name"])] = data["metric_value"]
        else:
            details[f"row_{i}"] = {str(k): v for k, v in data.items()}
    return details


def _quote_identifier(identifier: str) -> str:
    return ".".join(f"`{part.replace('`', '``')}`" for part in identifier.split("."))


def _spark_table_arg(catalog: str, table: str) -> str:
    return table if table.split(".", 1)[0] == catalog else f"{catalog}.{table}"


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class SparkEngine:
    name = "spark"

    def __init__(
        self,
        remote_uri: str | None = None,
        connect_uri: str | None = None,
        session: Any | None = None,
        master: str | None = None,
        conf: dict[str, Any] | None = None,
        **spark_conf: Any,
    ) -> None:
        if connect_uri and not remote_uri:
            remote_uri = connect_uri
        self.remote_uri = remote_uri
        self._session = session
        self.master = master
        self.conf = {**(conf or {}), **spark_conf}

    def execute(self, plan: Plan) -> list[ActionResult]:
        session = self._session or self._build_session()
        results: list[ActionResult] = []
        for action in plan.actions:
            sql = build_spark_compact_sql(action)
            rows = session.sql(sql).collect()
            results.append(
                ActionResult(action=action, status="submitted", details=parse_engine_rows(rows))
            )
        return results

    def _build_session(self) -> Any:
        try:
            from pyspark.sql import SparkSession
        except Exception as exc:  # pragma: no cover - exercised without spark extra installed
            raise IceopsError(
                "spark engine requires pyspark; install with `pip install iceops[spark]`"
            ) from exc

        builder = SparkSession.builder.appName("iceops-compact")
        if self.master:
            builder = builder.master(self.master)
        for key, value in self.conf.items():
            builder = builder.config(str(key), str(value))
        if self.remote_uri:
            remote = getattr(builder, "remote", None)
            if remote is None:
                raise IceopsError("installed pyspark does not support Spark Connect remote()")
            builder = remote(self.remote_uri)
        return builder.getOrCreate()
