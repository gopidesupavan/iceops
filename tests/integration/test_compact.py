"""Compact operator guards and planning — no engine required (dry-run/plan only).

The real Spark rewrite lives in test_spark_compact_lab.py (gated). These cover the
operator's own logic against a real catalog: managed-table refusal, plan actionability,
and the small-file estimate — all without submitting anything to an engine.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from iceops.errors import IceopsError
from iceops.models import CompactPlan
from iceops.operators import compact


def _make(catalog, name: str, appends: int = 5):
    try:
        catalog.drop_table(name)
    except Exception:
        pass
    batch = pa.table({"id": pa.array(range(50), type=pa.int64())})
    table = catalog.create_table(name, schema=batch.schema)
    for _ in range(appends):
        table.append(batch)
    return catalog.load_table(name)


def test_dry_run_plans_small_files_without_an_engine(seeded_catalog):
    _make(seeded_catalog, "db.compactme")
    plan = compact(seeded_catalog, "db.compactme", engine="spark")
    assert isinstance(plan, CompactPlan)
    assert plan.engine == "spark"
    assert plan.small_file_count == 5  # all tiny appends are well under 75% of target
    assert plan.actionable
    assert plan.action is not None and plan.action.op == "compact"
    assert plan.engine_contract is not None
    assert plan.engine_contract.plan_kind == "delegated"
    assert "rewrite_data_files" in plan.engine_contract.statement
    # dry run resolves the engine catalog from the table's catalog
    assert plan.engine_catalog == "test"


def test_managed_table_refused_without_force(seeded_catalog):
    table = _make(seeded_catalog, "db.compactmanaged")
    with table.transaction() as tx:
        tx.set_properties({"self-optimizing.enabled": "true"})
    with pytest.raises(IceopsError, match="managed by amoro"):
        compact(seeded_catalog, "db.compactmanaged", engine="spark")
    # --force overrides and produces a plan (still no execution)
    plan = compact(seeded_catalog, "db.compactmanaged", engine="spark", force=True)
    assert isinstance(plan, CompactPlan)


def test_missing_table_raises(seeded_catalog):
    with pytest.raises(IceopsError):
        compact(seeded_catalog, "db.nope", engine="spark")
