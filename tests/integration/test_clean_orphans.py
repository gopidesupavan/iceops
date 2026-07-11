from __future__ import annotations

import datetime as dt
import os
import shutil
from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pytest

from iceops.models import CleanOrphansPlan, CleanOrphansResult
from iceops.operators.clean_orphans import clean_orphans, execute_plan
from iceops.operators.expire import expire


def _table_dir(table) -> Path:
    return Path(urlparse(table.location()).path)


def _backdate(path: Path, days: int = 30) -> None:
    old = (dt.datetime.now() - dt.timedelta(days=days)).timestamp()
    os.utime(path, (old, old))


def _plant(table_dir: Path, rel: str, backdate_days: int | None = 30) -> Path:
    source = sorted((table_dir / "data").glob("*.parquet"))[0]
    target = table_dir / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(source, target)
    if backdate_days is not None:
        _backdate(target, backdate_days)
    return target


@pytest.fixture()
def gauntlet_table(seeded_catalog):
    """A table surrounded by orphans and decoys. The decoys MUST survive."""
    name = "db.gauntlet"
    try:
        seeded_catalog.drop_table(name)
    except Exception:
        pass
    batch = pa.table({"id": pa.array(range(100), type=pa.int64())})
    table = seeded_catalog.create_table(name, schema=batch.schema)
    for _ in range(3):
        table.append(batch)
    table = seeded_catalog.load_table(name)
    d = _table_dir(table)

    targets = {  # should be deleted
        "old_data_orphan": _plant(d, "data/00000-0-dead.parquet", 30),
        "old_metadata_junk": _plant(d, "metadata/dead-manifest-m0.avro", 30),
    }
    decoys = {  # must survive
        "young_orphan": _plant(d, "data/00000-0-inflight.parquet", None),
        "excluded_marker": _plant(d, "data/_SUCCESS", 30),
        "fake_metadata_json": _plant(d, "metadata/99999-dead.metadata.json", 30),
    }
    return name, targets, decoys


class TestGauntlet:
    def test_dry_run_lists_exactly_the_targets(self, seeded_catalog, gauntlet_table):
        name, targets, _ = gauntlet_table
        plan = clean_orphans(seeded_catalog, name, exclude=("_SUCCESS",))
        assert isinstance(plan, CleanOrphansPlan)
        got = {Path(urlparse(c.path).path).name for c in plan.candidates}
        assert got == {t.name for t in targets.values()}
        assert plan.skipped["young"] >= 1
        assert plan.skipped["excluded"] >= 1
        assert plan.skipped["metadata-json"] >= 1
        # dry run deleted nothing
        for f in list(targets.values()):
            assert f.exists()

    def test_execute_deletes_targets_and_spares_every_decoy(self, seeded_catalog, gauntlet_table):
        name, targets, decoys = gauntlet_table
        table = seeded_catalog.load_table(name)
        rows_before = table.scan().to_arrow().num_rows

        result = clean_orphans(seeded_catalog, name, exclude=("_SUCCESS",), execute=True)
        assert isinstance(result, CleanOrphansResult)
        assert result.status == "cleaned"
        assert len(result.deleted) == len(targets)
        assert result.freed_bytes > 0

        for f in targets.values():
            assert not f.exists(), f"target survived: {f}"
        for label, f in decoys.items():
            assert f.exists(), f"DECOY DELETED: {label} ({f})"

        table = seeded_catalog.load_table(name)
        assert table.scan().to_arrow().num_rows == rows_before

        again = clean_orphans(seeded_catalog, name, exclude=("_SUCCESS",))
        assert isinstance(again, CleanOrphansPlan)
        assert not again.actionable  # idempotent


class TestExpireThenClean:
    def test_expire_unreferences_clean_reclaims(self, seeded_catalog):
        """The E2->E4 story: expire drops history, clean-orphans reclaims the bytes."""
        name = "db.lifecycle"
        try:
            seeded_catalog.drop_table(name)
        except Exception:
            pass
        batch = pa.table({"id": pa.array(range(100), type=pa.int64())})
        table = seeded_catalog.create_table(name, schema=batch.schema)
        for _ in range(6):
            table.append(batch)
        table = seeded_catalog.load_table(name)
        d = _table_dir(table)
        rows_before = table.scan().to_arrow().num_rows
        snaps_on_disk_before = len(list((d / "metadata").glob("snap-*.avro")))
        metadata_json_before = len(list((d / "metadata").glob("*.metadata.json")))

        expire(seeded_catalog, name, retain_last=2, older_than=dt.timedelta(0), execute=True)

        # age threshold 0 so the just-unreferenced files qualify immediately
        result = clean_orphans(seeded_catalog, name, older_than=dt.timedelta(0), execute=True)
        assert isinstance(result, CleanOrphansResult)
        assert result.status == "cleaned"
        assert len(result.deleted) >= 4  # at least the 4 expired snap-*.avro

        snaps_on_disk_after = len(list((d / "metadata").glob("snap-*.avro")))
        assert snaps_on_disk_after < snaps_on_disk_before
        # metadata.json chain untouched (only grew by the expire commit)
        assert len(list((d / "metadata").glob("*.metadata.json"))) >= metadata_json_before

        table = seeded_catalog.load_table(name)
        assert table.scan().to_arrow().num_rows == rows_before


class TestConcurrencyRecheck:
    def test_file_referenced_mid_run_is_spared(self, seeded_catalog):
        name = "db.race"
        try:
            seeded_catalog.drop_table(name)
        except Exception:
            pass
        batch = pa.table({"id": pa.array(range(100), type=pa.int64())})
        table = seeded_catalog.create_table(name, schema=batch.schema)
        table.append(batch)
        table = seeded_catalog.load_table(name)
        d = _table_dir(table)

        planted = _plant(d, "data/00000-0-racer.parquet", 30)
        plan = clean_orphans(seeded_catalog, name)
        assert isinstance(plan, CleanOrphansPlan)
        assert any("racer" in c.path for c in plan.candidates)

        # a "concurrent writer" registers the planted file between plan and execute
        table.add_files([planted.as_uri()])

        result = execute_plan(seeded_catalog.load_table(name), plan)
        assert any("racer" in p for p in result.spared)
        assert not any("racer" in p for p in result.deleted)
        assert planted.exists()
        table = seeded_catalog.load_table(name)
        assert table.scan().to_arrow().num_rows == 200  # both files now live
