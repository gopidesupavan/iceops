from __future__ import annotations

import pytest

from iceops.engines import build_statement
from iceops.engines.spark import (
    build_spark_clean_orphans_sql,
    build_spark_compact_sql,
    build_spark_expire_sql,
    build_spark_rewrite_manifests_sql,
    parse_engine_rows,
)
from iceops.engines.trino import (
    build_trino_clean_orphans_sql,
    build_trino_compact_sql,
    build_trino_expire_sql,
    build_trino_rewrite_manifests_sql,
)
from iceops.errors import IceopsError
from iceops.models import Action, CompactPlan, VerificationStatus
from iceops.operators import compact
from iceops.operators.compact import verify_row_count


def _op_action(op: str, **params) -> Action:
    base = {"table": "db.events", "engine_catalog": "demo"}
    return Action(op=op, table="db.events", params={**base, **params})


class TestSparkMaintenanceSql:
    def test_rewrite_manifests(self):
        assert build_spark_rewrite_manifests_sql(_op_action("rewrite_manifests")) == (
            "CALL `demo`.system.rewrite_manifests(table => 'demo.db.events')"
        )

    def test_expire_uses_timestamp_and_retain_last(self):
        sql = build_spark_expire_sql(_op_action("expire", older_than_seconds=604800, retain_last=5))
        assert "CALL `demo`.system.expire_snapshots(table => 'demo.db.events'" in sql
        assert "older_than => TIMESTAMP '" in sql
        assert "retain_last => 5)" in sql

    def test_clean_orphans_uses_timestamp(self):
        sql = build_spark_clean_orphans_sql(_op_action("clean_orphans", older_than_seconds=259200))
        assert "CALL `demo`.system.remove_orphan_files(table => 'demo.db.events'" in sql
        assert "older_than => TIMESTAMP '" in sql


class TestTrinoMaintenanceSql:
    def test_rewrite_manifests(self):
        assert build_trino_rewrite_manifests_sql(_op_action("rewrite_manifests")) == (
            'ALTER TABLE "demo"."db"."events" EXECUTE optimize_manifests'
        )

    def test_expire_uses_retention_threshold(self):
        sql = build_trino_expire_sql(_op_action("expire", older_than_seconds=604800, retain_last=5))
        assert sql == (
            'ALTER TABLE "demo"."db"."events" '
            "EXECUTE expire_snapshots(retention_threshold => '604800s')"
        )

    def test_clean_orphans_uses_retention_threshold(self):
        sql = build_trino_clean_orphans_sql(_op_action("clean_orphans", older_than_seconds=0))
        assert sql == (
            'ALTER TABLE "demo"."db"."events" '
            "EXECUTE remove_orphan_files(retention_threshold => '0s')"
        )


class TestRowCountVerification:
    def test_mismatch_raises_with_rollback_guidance(self):
        with pytest.raises(IceopsError, match=r"changed the row count.*9000 -> 8999"):
            verify_row_count("db.events", 9000, 8999, snapshot_id=42)

    def test_equal_counts_pass(self):
        result = verify_row_count("db.events", 9000, 9000, snapshot_id=42)
        assert result.status == VerificationStatus.PASSED
        assert result.before == 9000
        assert result.after == 9000

    def test_unknown_count_returns_skipped_result(self):
        # a missing signal must never fabricate a failure
        assert (
            verify_row_count("db.events", None, 9000, snapshot_id=42).status
            == VerificationStatus.SKIPPED
        )
        assert (
            verify_row_count("db.events", 9000, None, snapshot_id=42).status
            == VerificationStatus.SKIPPED
        )


class TestEngineValidationIsFailFast:
    # engine is validated before any catalog I/O, so a dummy catalog is never touched
    def test_native_engine_not_available_yet(self):
        with pytest.raises(IceopsError, match="native compaction is not available"):
            compact(catalog=None, identifier="db.events", engine="native")  # type: ignore[arg-type]

    def test_unknown_engine_rejected(self):
        with pytest.raises(IceopsError, match="unknown compact engine 'mystery'"):
            compact(catalog=None, identifier="db.events", engine="mystery")  # type: ignore[arg-type]


def _action() -> Action:
    return Action(
        op="compact",
        table="db.events",
        params={
            "table": "db.events",
            "engine_catalog": "demo",
            "target_file_size_bytes": 128 * 1024 * 1024,
        },
    )


def test_spark_compact_sql_uses_iceberg_rewrite_data_files_procedure():
    sql = build_spark_compact_sql(_action())

    assert sql == (
        "CALL `demo`.system.rewrite_data_files("
        "table => 'demo.db.events', "
        "options => map('target-file-size-bytes', '134217728', 'min-input-files', '2'))"
    )


def test_build_statement_uses_the_engine_sql_builder():
    assert build_statement("spark", _action()) == build_spark_compact_sql(_action())
    assert build_statement("trino", _action()) == build_trino_compact_sql(_action())


def test_spark_compact_sql_escapes_catalog_and_table_strings():
    action = Action(
        op="compact",
        table="db.events",
        params={
            "table": "db.weird'table",
            "engine_catalog": "cat`alog",
            "target_file_size_bytes": 64,
        },
    )

    sql = build_spark_compact_sql(action)

    assert "`cat``alog`" in sql
    assert "table => 'cat`alog.db.weird''table'" in sql


def test_trino_compact_sql_uses_optimize_execute():
    sql = build_trino_compact_sql(_action())

    assert sql == (
        'ALTER TABLE "demo"."db"."events" EXECUTE optimize(file_size_threshold => \'128MB\')'
    )


def test_trino_compact_sql_rounds_up_to_mb():
    action = Action(
        op="compact",
        table="db.events",
        params={
            "table": "db.events",
            "engine_catalog": "demo",
            "target_file_size_bytes": 1,
        },
    )

    assert "file_size_threshold => '1MB'" in build_trino_compact_sql(action)


def test_parse_engine_rows_preserves_metric_rows():
    rows = [
        {"metric_name": "rewritten_data_files_count", "metric_value": 12},
        {"metric_name": "added_data_files_count", "metric_value": 2},
    ]

    assert parse_engine_rows(rows) == {
        "rewritten_data_files_count": 12,
        "added_data_files_count": 2,
    }


def test_compact_plan_actionable_for_small_files_or_deletes():
    assert not CompactPlan(identifier="db.empty").actionable
    assert not CompactPlan(identifier="db.one", small_file_count=1).actionable
    assert CompactPlan(identifier="db.small", small_file_count=2).actionable
    assert CompactPlan(identifier="db.deletes", delete_file_count=1).actionable
