"""Spark engine: submits Iceberg system procedures to the user's Spark.

Requires: pip install iceops[spark]
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from ..errors import IceopsError
from ..models import Action, ActionResult, Plan


def _procedure_target(action: Action) -> tuple[str, str]:
    """(catalog, table-argument) shared by every procedure call."""
    catalog = str(action.params.get("engine_catalog") or "")
    table = str(action.params.get("table") or action.table)
    if not catalog:
        raise IceopsError(f"spark {action.op} action is missing engine_catalog")
    return catalog, _spark_table_arg(catalog, table)


def build_spark_compact_sql(action: Action) -> str:
    catalog, table_arg = _procedure_target(action)
    target = int(action.params["target_file_size_bytes"])
    return (
        f"CALL {_quote_identifier(catalog)}.system.rewrite_data_files("
        f"table => {_sql_string(table_arg)}, "
        "options => map("
        f"'target-file-size-bytes', '{target}', "
        "'min-input-files', '2'"
        ")"
        ")"
    )


def build_spark_expire_sql(action: Action) -> str:
    catalog, table_arg = _procedure_target(action)
    older_than = _spark_timestamp(int(action.params["older_than_seconds"]))
    retain_last = int(action.params["retain_last"])
    return (
        f"CALL {_quote_identifier(catalog)}.system.expire_snapshots("
        f"table => {_sql_string(table_arg)}, "
        f"older_than => TIMESTAMP '{older_than}', "
        f"retain_last => {retain_last})"
    )


def build_spark_clean_orphans_sql(action: Action) -> str:
    catalog, table_arg = _procedure_target(action)
    older_than = _spark_timestamp(int(action.params["older_than_seconds"]))
    return (
        f"CALL {_quote_identifier(catalog)}.system.remove_orphan_files("
        f"table => {_sql_string(table_arg)}, "
        f"older_than => TIMESTAMP '{older_than}')"
    )


def build_spark_rewrite_manifests_sql(action: Action) -> str:
    catalog, table_arg = _procedure_target(action)
    return (
        f"CALL {_quote_identifier(catalog)}.system.rewrite_manifests("
        f"table => {_sql_string(table_arg)})"
    )


SPARK_SQL_BUILDERS = {
    "compact": build_spark_compact_sql,
    "expire": build_spark_expire_sql,
    "clean_orphans": build_spark_clean_orphans_sql,
    "rewrite_manifests": build_spark_rewrite_manifests_sql,
}


def _spark_timestamp(older_than_seconds: int) -> str:
    """Spark procedures take an absolute cutoff TIMESTAMP; convert an age to now-age."""
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=older_than_seconds)
    return cutoff.strftime("%Y-%m-%d %H:%M:%S")


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
        # our older_than literals are UTC; pin the session so Spark interprets them as UTC
        # (otherwise expire/remove_orphan_files compare against the wrong absolute time and
        # silently do nothing). Restore the user's setting afterwards.
        previous_tz = session.conf.get("spark.sql.session.timeZone", None)
        session.conf.set("spark.sql.session.timeZone", "UTC")
        try:
            for action in plan.actions:
                builder = SPARK_SQL_BUILDERS.get(action.op)
                if builder is None:
                    raise IceopsError(f"spark engine cannot execute '{action.op}'")
                statement = builder(action)
                rows = session.sql(statement).collect()
                details = parse_engine_rows(rows)
                details.setdefault("statement", statement)
                results.append(ActionResult(action=action, status="submitted", details=details))
        finally:
            if previous_tz is not None:
                session.conf.set("spark.sql.session.timeZone", previous_tz)
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
