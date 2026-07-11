"""The compatibility matrix: every operator against every Iceberg table shape.

Tables come from examples/table_factory.py (partition transforms, spec/schema evolution,
copy-on-write overwrites). Shapes the installed PyIceberg cannot build are skipped
LOUDLY with the reason — an unsupported shape is a finding we want visible, not hidden.
"""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import pytest
from pyiceberg.catalog import load_catalog

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "examples"))
from table_factory import VARIATIONS, build_all  # noqa: E402

from iceops.inspect import collect  # noqa: E402
from iceops.models import CleanOrphansResult, ExpirePlan, RewriteManifestsResult  # noqa: E402
from iceops.operators import clean_orphans, cost, doctor, expire, rewrite_manifests, scan  # noqa: E402

ZERO = dt.timedelta(0)
PARTITIONED = {"part_identity", "part_day", "part_month", "part_bucket", "part_truncate"}
ALL_NAMES = [v.name for v in VARIATIONS]


@pytest.fixture(scope="module")
def lab(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("lab_warehouse")
    catalog = load_catalog(
        "lab",
        type="sql",
        uri=f"sqlite:///{warehouse}/catalog.db",
        warehouse=f"file://{warehouse}",
    )
    results = build_all(catalog, "lab")
    return catalog, results


def table_or_skip(lab, name: str) -> str:
    _, results = lab
    identifier = f"lab.{name}"
    status = results[identifier]
    if status != "ok":
        pytest.skip(f"variation '{name}' not buildable on this PyIceberg: {status}")
    return identifier


class TestFactoryItself:
    def test_all_variations_report_a_status(self, lab):
        _, results = lab
        assert set(results) == {f"lab.{n}" for n in ALL_NAMES}

    def test_unsupported_variations_are_known(self, lab):
        """If a shape stops (or starts!) building, we want a loud diff here."""
        _, results = lab
        unsupported = sorted(k for k, v in results.items() if v != "ok")
        # current expectation on PyIceberg 0.11.x: everything builds
        assert unsupported == [], f"newly unsupported shapes: {unsupported}"


class TestDiagnoseAcrossShapes:
    @pytest.mark.parametrize("name", ALL_NAMES)
    def test_doctor_handles_every_shape(self, lab, name):
        catalog, _ = lab
        identifier = table_or_skip(lab, name)
        report = doctor(catalog, identifier)
        assert report.metrics.data_file_count > 0
        assert report.metrics.snapshot_count > 0
        assert sum(report.metrics.file_size_histogram.values()) == report.metrics.data_file_count

    @pytest.mark.parametrize("name", sorted(PARTITIONED))
    def test_partitioned_tables_report_multiple_partitions(self, lab, name):
        catalog, _ = lab
        identifier = table_or_skip(lab, name)
        report = doctor(catalog, identifier)
        assert report.metrics.partition_count > 1, (
            f"{name}: expected >1 partition, metrics saw {report.metrics.partition_count}"
        )

    @pytest.mark.parametrize("name", ALL_NAMES)
    def test_cost_never_crashes_and_buckets_are_sane(self, lab, name):
        catalog, _ = lab
        identifier = table_or_skip(lab, name)
        report = cost(catalog, identifier)
        assert report.live_bytes > 0
        if report.stale_bytes is not None and report.reachable_bytes is not None:
            assert report.stale_bytes <= report.reachable_bytes

    def test_scan_sees_the_whole_lab(self, lab):
        catalog, results = lab
        built = {k for k, v in results.items() if v == "ok"}
        report = scan(catalog, "lab")
        assert {r.identifier for r in report.reports} >= built
        assert not report.errors


class TestRewriteManifestsAcrossShapes:
    @pytest.mark.parametrize("name", ALL_NAMES)
    def test_rewrite_preserves_everything_on_every_shape(self, lab, name):
        catalog, _ = lab
        identifier = table_or_skip(lab, name)
        table = catalog.load_table(identifier)
        rows_before = table.scan().to_arrow().num_rows
        paths_before = {str(p) for p in table.inspect.files().column("file_path").to_pylist()}
        manifests_before = table.inspect.manifests().num_rows
        if manifests_before <= 1:
            pytest.skip("nothing to consolidate")

        result = rewrite_manifests(catalog, identifier, execute=True)
        assert isinstance(result, RewriteManifestsResult)
        assert result.status == "rewritten"
        assert result.manifests_after < manifests_before

        table = catalog.load_table(identifier)
        assert table.scan().to_arrow().num_rows == rows_before
        paths_after = {str(p) for p in table.inspect.files().column("file_path").to_pylist()}
        assert paths_after == paths_before

    def test_partition_pruning_still_works_after_rewrite(self, lab):
        """The stats-preservation test: consolidation must not break pruning."""
        catalog, _ = lab
        identifier = table_or_skip(lab, "part_identity")
        table = catalog.load_table(identifier)
        expected = table.scan(row_filter="category = 'alpha'").to_arrow().num_rows
        assert expected > 0
        # rewrite may already have run in the test above; run again defensively
        rewrite_manifests(catalog, identifier, execute=True)
        table = catalog.load_table(identifier)
        assert table.scan(row_filter="category = 'alpha'").to_arrow().num_rows == expected


class TestLifecycleAcrossShapes:
    @pytest.mark.parametrize("name", ALL_NAMES)
    def test_expire_then_clean_keeps_table_readable(self, lab, name):
        catalog, _ = lab
        identifier = table_or_skip(lab, name)
        table = catalog.load_table(identifier)
        rows_before = table.scan().to_arrow().num_rows

        expire(catalog, identifier, retain_last=1, older_than=ZERO, execute=True)
        result = clean_orphans(catalog, identifier, older_than=ZERO, execute=True)
        assert isinstance(result, CleanOrphansResult)

        table = catalog.load_table(identifier)
        assert table.scan().to_arrow().num_rows == rows_before
        assert len(table.metadata.snapshots) >= 1

    def test_overwrite_is_where_expire_actually_frees_data(self, lab):
        """The stale-bytes path, never exercised by append-only tables."""
        catalog, _ = lab
        identifier = table_or_skip(lab, "overwritten")
        # rebuild fresh: earlier lifecycle tests may have consumed the history
        from table_factory import VARIATIONS as V

        build = next(v for v in V if v.name == "overwritten").build
        catalog.drop_table(identifier)
        build(catalog, identifier)

        table = catalog.load_table(identifier)
        metrics = collect(table, identifier)
        assert metrics.reachable_bytes is not None
        stale = metrics.reachable_bytes - metrics.total_data_bytes
        assert stale > 0, "overwrite must leave stale bytes behind"

        plan = expire(catalog, identifier, retain_last=1, older_than=ZERO)
        assert isinstance(plan, ExpirePlan)
        assert plan.unreferenced_data_bytes and plan.unreferenced_data_bytes > 0

        expire(catalog, identifier, retain_last=1, older_than=ZERO, execute=True)
        result = clean_orphans(catalog, identifier, older_than=ZERO, execute=True)
        assert isinstance(result, CleanOrphansResult)
        parquet_deleted = [p for p in result.deleted if p.endswith(".parquet")]
        assert parquet_deleted, "clean-orphans must reclaim the overwritten parquet files"

        table = catalog.load_table(identifier)
        assert table.scan().to_arrow().num_rows == 300  # exactly the overwrite rows
