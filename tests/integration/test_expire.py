from __future__ import annotations

import datetime as dt
from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pytest

from iceops.errors import IceopsError
from iceops.models import ExpirePlan, ExpireResult
from iceops.operators.expire import expire


@pytest.fixture()
def expirable_table(seeded_catalog):
    """A dedicated table so expire tests never mutate shared fixtures."""
    name = "db.expireme"
    try:
        seeded_catalog.drop_table(name)
    except Exception:
        pass
    batch = pa.table({"id": pa.array(range(10), type=pa.int64())})
    table = seeded_catalog.create_table(name, schema=batch.schema)
    for _ in range(8):
        table.append(batch)
    return name


class TestExpireIntegration:
    def test_dry_run_changes_nothing(self, seeded_catalog, expirable_table):
        plan = expire(
            seeded_catalog,
            expirable_table,
            retain_last=3,
            older_than=dt.timedelta(0),
        )
        assert isinstance(plan, ExpirePlan)
        assert len(plan.candidates) == 5
        table = seeded_catalog.load_table(expirable_table)
        assert len(table.metadata.snapshots) == 8  # untouched

    def test_default_age_threshold_yields_nothing(self, seeded_catalog, expirable_table):
        plan = expire(seeded_catalog, expirable_table)  # older-than 7d, all snapshots fresh
        assert isinstance(plan, ExpirePlan)
        assert plan.candidates == []

    def test_execute_expires_exactly_the_plan(self, seeded_catalog, expirable_table):
        table = seeded_catalog.load_table(expirable_table)
        rows_before = table.scan().to_arrow().num_rows
        files_before = [str(p) for p in table.inspect.all_files().column("file_path").to_pylist()]
        current_id = table.current_snapshot().snapshot_id

        result = expire(
            seeded_catalog,
            expirable_table,
            retain_last=3,
            older_than=dt.timedelta(0),
            execute=True,
        )
        assert isinstance(result, ExpireResult)
        assert len(result.expired_snapshot_ids) == 5
        assert result.snapshot_count_after == 3

        table = seeded_catalog.load_table(expirable_table)
        remaining_ids = {s.snapshot_id for s in table.metadata.snapshots}
        assert current_id in remaining_ids  # current snapshot always survives
        assert not (set(result.expired_snapshot_ids) & remaining_ids)
        assert table.scan().to_arrow().num_rows == rows_before  # data intact

        # PHASE-1 PINNING: PyIceberg expiration is metadata-only and must delete NO
        # files. If a PyIceberg upgrade starts deleting files, this fails and forces a
        # conscious decision (double-deletion risk with clean-orphans).
        for file_path in files_before:
            local = urlparse(file_path)
            assert Path(local.path).exists(), f"expire deleted a file: {file_path}"

    def test_nothing_to_do_execute_is_safe(self, seeded_catalog, expirable_table):
        result = expire(
            seeded_catalog,
            expirable_table,
            retain_last=100,
            older_than=dt.timedelta(0),
            execute=True,
        )
        assert isinstance(result, ExpireResult)
        assert result.status == "nothing-to-do"
        assert result.expired_snapshot_ids == []

    def test_managed_table_refused_without_force(self, seeded_catalog, expirable_table):
        table = seeded_catalog.load_table(expirable_table)
        with table.transaction() as tx:
            tx.set_properties({"self-optimizing.enabled": "true"})
        with pytest.raises(IceopsError, match="managed by amoro"):
            expire(seeded_catalog, expirable_table, older_than=dt.timedelta(0))
        # --force overrides, dry-run works
        plan = expire(seeded_catalog, expirable_table, older_than=dt.timedelta(0), force=True)
        assert isinstance(plan, ExpirePlan)
