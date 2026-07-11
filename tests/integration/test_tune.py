"""tune against a real seeded catalog (native path — no engine).

Verifies composition: the right operators run in the right order, data survives, and the
compact step is honestly skipped without an engine.
"""

from __future__ import annotations

import datetime as dt

import pyarrow as pa
import pytest

from iceops.errors import IceopsError
from iceops.models import TunePlan, TuneResult
from iceops.operators import tune


def _make_fragmented(catalog, name: str, appends: int = 8):
    try:
        catalog.drop_table(name)
    except Exception:
        pass
    batch = pa.table({"id": pa.array(range(50), type=pa.int64())})
    table = catalog.create_table(name, schema=batch.schema)
    for _ in range(appends):
        table.append(batch)
    return catalog.load_table(name)


def test_dry_run_composes_native_three_and_skips_compact(seeded_catalog):
    _make_fragmented(seeded_catalog, "db.tuneme")
    plan = tune(seeded_catalog, "db.tuneme")  # no engine
    assert isinstance(plan, TunePlan)
    assert plan.compact is None
    assert "compact" in plan.skipped
    assert plan.rewrite_manifests is not None and plan.rewrite_manifests.actionable
    assert plan.expire is not None  # planned (may or may not be actionable)
    assert plan.clean_orphans is not None
    assert plan.actionable
    # dry run changed nothing
    assert seeded_catalog.load_table("db.tuneme").inspect.manifests().num_rows == 8


def test_execute_runs_steps_in_order_and_preserves_data(seeded_catalog):
    table = _make_fragmented(seeded_catalog, "db.tunerun")
    rows_before = table.scan().to_arrow().num_rows

    # older_than_expire=0 + retain_last=2 so expire has candidates on this fresh table
    result = tune(
        seeded_catalog,
        "db.tunerun",
        retain_last=2,
        older_than_expire=dt.timedelta(0),
        execute=True,
    )
    assert isinstance(result, TuneResult)
    assert result.status == "tuned"
    # rewrite-manifests and expire both had work; both ran, in order
    assert result.executed[:2] == ["rewrite_manifests", "expire"]
    assert result.rewrite_manifests is not None
    assert result.expire is not None

    table = seeded_catalog.load_table("db.tunerun")
    assert table.scan().to_arrow().num_rows == rows_before  # data intact
    assert table.inspect.manifests().num_rows == 1  # consolidated
    # retain_last=2 after expire; rewrite + expire each add a snapshot on top
    assert len(table.metadata.snapshots) <= 4


def test_managed_table_refused_without_force(seeded_catalog):
    table = _make_fragmented(seeded_catalog, "db.tunemanaged")
    with table.transaction() as tx:
        tx.set_properties({"self-optimizing.enabled": "true"})
    with pytest.raises(IceopsError, match="managed by amoro"):
        tune(seeded_catalog, "db.tunemanaged")
    plan = tune(seeded_catalog, "db.tunemanaged", force=True)
    assert isinstance(plan, TunePlan)


def test_healthy_table_is_nothing_to_do(seeded_catalog):
    # one append: not fragmented, nothing to expire (7d default), no orphans
    try:
        seeded_catalog.drop_table("db.tuneclean")
    except Exception:
        pass
    batch = pa.table({"id": pa.array(range(1000), type=pa.int64())})
    t = seeded_catalog.create_table("db.tuneclean", schema=batch.schema)
    t.append(batch)
    result = tune(seeded_catalog, "db.tuneclean", execute=True)
    assert isinstance(result, TuneResult)
    assert result.status == "nothing-to-do"
