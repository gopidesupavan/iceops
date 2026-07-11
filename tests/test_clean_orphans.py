from __future__ import annotations

import datetime as dt
import os
import shutil
from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pytest

from iceops.models import CleanOrphansPlan, CleanOrphansResult, OrphanFile
from iceops.operators.clean_orphans import (
    clean_orphans,
    execute_plan,
    filter_candidates,
    normalize_path,
)
from iceops.operators.expire import expire

OLD = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30)
NOW = dt.datetime.now(dt.timezone.utc)


class TestNormalizePath:
    def test_file_uri_equals_bare_path(self):
        assert normalize_path("file:///wh/t/data/f.parquet") == "/wh/t/data/f.parquet"
        assert normalize_path("/wh/t/data/f.parquet") == "/wh/t/data/f.parquet"

    def test_s3_uri_equals_bucket_key(self):
        assert normalize_path("s3://bucket/wh/t/f.parquet") == "bucket/wh/t/f.parquet"

    def test_url_encoding(self):
        assert normalize_path("file:///wh/my%20table/f.parquet") == "/wh/my table/f.parquet"

    def test_gs_and_abfs_schemes(self):
        assert normalize_path("gs://b/k") == "b/k"
        assert normalize_path("abfss://container@acct.dfs.core.windows.net/k").endswith("/k")


def orphan(path: str, mtime: dt.datetime = OLD, size: int = 100) -> OrphanFile:
    return OrphanFile(path=path, size_bytes=size, modified_at=mtime)


class TestFunnel:
    LOC = "/wh/t/"
    CUTOFF = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=3)

    def run(self, files, reachable=frozenset(), exclude=()):
        return filter_candidates(files, set(reachable), self.LOC, self.CUTOFF, exclude)

    def test_reachable_files_never_candidates(self):
        candidates, _ = self.run(
            [orphan("/wh/t/data/live.parquet")], reachable={"/wh/t/data/live.parquet"}
        )
        assert candidates == []

    def test_young_files_skipped(self):
        candidates, skipped = self.run([orphan("/wh/t/data/f.parquet", mtime=NOW)])
        assert candidates == [] and skipped["young"] == 1

    def test_unknown_mtime_treated_as_young(self):
        candidates, skipped = self.run([OrphanFile(path="/wh/t/data/f.parquet")])
        assert candidates == [] and skipped["young"] == 1

    def test_metadata_json_hard_protected_even_when_old_and_unreachable(self):
        files = [
            orphan("/wh/t/metadata/00001-abc.metadata.json"),
            orphan("/wh/t/metadata/version-hint.text"),
        ]
        candidates, skipped = self.run(files)
        assert candidates == [] and skipped["metadata-json"] == 2

    def test_exclude_globs(self):
        candidates, skipped = self.run(
            [orphan("/wh/t/data/_SUCCESS"), orphan("/wh/t/data/f.parquet")],
            exclude=("_SUCCESS",),
        )
        assert [c.path for c in candidates] == ["/wh/t/data/f.parquet"]
        assert skipped["excluded"] == 1

    def test_out_of_scope_skipped(self):
        candidates, skipped = self.run([orphan("/other/place/f.parquet")])
        assert candidates == [] and skipped["out-of-scope"] == 1

    def test_true_orphan_passes_all_stages(self):
        candidates, _ = self.run([orphan("/wh/t/data/dead.parquet")])
        assert [c.path for c in candidates] == ["/wh/t/data/dead.parquet"]


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
