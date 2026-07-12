"""Engine-mode routing against a real catalog (no engine actually invoked).

Verifies the dry-run engine-mode plans are built correctly and that execute without an
engine catalog fails clearly — without needing a Spark/Trino cluster. Real engine
execution is covered by the gated Spark/Trino labs.
"""

from __future__ import annotations

import datetime as dt

import pyarrow as pa
import pytest

from iceops.errors import IceopsError
from iceops.models import CleanOrphansPlan, ExpirePlan, RewriteManifestsPlan
from iceops.operators import clean_orphans, expire, rewrite_manifests


def _table(catalog, name: str, appends: int = 5):
    try:
        catalog.drop_table(name)
    except Exception:
        pass
    batch = pa.table({"id": pa.array(range(50), type=pa.int64())})
    t = catalog.create_table(name, schema=batch.schema)
    for _ in range(appends):
        t.append(batch)
    return catalog.load_table(name)


def test_expire_engine_dry_run_builds_engine_plan(seeded_catalog):
    _table(seeded_catalog, "db.engexpire")
    plan = expire(seeded_catalog, "db.engexpire", engine="spark")
    assert isinstance(plan, ExpirePlan)
    assert plan.engine == "spark"
    assert plan.candidates == []  # delegated — no native enumeration
    assert plan.actionable  # snapshots exist
    assert plan.action is not None
    assert plan.engine_contract is not None
    assert "expire_snapshots" in plan.engine_contract.statement


def test_rewrite_engine_dry_run_builds_engine_plan(seeded_catalog):
    _table(seeded_catalog, "db.engrewrite")
    plan = rewrite_manifests(seeded_catalog, "db.engrewrite", engine="trino")
    assert isinstance(plan, RewriteManifestsPlan)
    assert plan.engine == "trino"
    assert plan.manifest_count == 5
    assert plan.actionable
    assert plan.action is not None
    assert plan.engine_contract is not None
    assert "optimize_manifests" in plan.engine_contract.statement


def test_clean_engine_dry_run_skips_native_listing(seeded_catalog):
    _table(seeded_catalog, "db.engclean")
    plan = clean_orphans(seeded_catalog, "db.engclean", engine="spark")
    assert isinstance(plan, CleanOrphansPlan)
    assert plan.engine == "spark"
    assert plan.candidates == []  # no native storage listing in engine mode
    assert plan.listed_count == 0
    assert plan.actionable
    assert plan.action is not None
    assert plan.engine_contract is not None
    assert "remove_orphan_files" in plan.engine_contract.statement


def test_engine_catalog_is_inferred_for_dry_run(seeded_catalog):
    _table(seeded_catalog, "db.engnocat")
    plan = expire(seeded_catalog, "db.engnocat", engine="spark", engine_catalog=None)
    assert isinstance(plan, ExpirePlan)
    assert plan.action is not None
    assert plan.action.params["engine_catalog"] == "test"
    assert plan.engine_contract is not None
    assert "`test`.system.expire_snapshots" in plan.engine_contract.statement


def test_unknown_engine_rejected(seeded_catalog):
    _table(seeded_catalog, "db.engbad")
    with pytest.raises(IceopsError, match="unknown engine"):
        expire(
            seeded_catalog,
            "db.engbad",
            engine="bogus",
            engine_catalog="test",
            older_than=dt.timedelta(0),
            execute=True,
        )
