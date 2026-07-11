from __future__ import annotations

from conftest import MESSY_APPENDS

from iceops.inspect import collect


def test_collect_messy_table(seeded_catalog):
    table = seeded_catalog.load_table("db.messy")
    metrics = collect(table, "db.messy")

    assert metrics.identifier == "db.messy"
    assert metrics.snapshot_count == MESSY_APPENDS
    assert metrics.data_file_count == MESSY_APPENDS
    assert metrics.total_data_bytes > 0
    assert metrics.small_file_ratio == 1.0  # every file is tiny
    assert sum(metrics.file_size_histogram.values()) == metrics.data_file_count
    assert metrics.file_size_histogram["<1MB"] == metrics.data_file_count


def test_collect_reachable_and_orphans(seeded_catalog):
    table = seeded_catalog.load_table("db.messy")
    metrics = collect(table, "db.messy")

    # append-only table: everything ever written is still reachable
    if metrics.reachable_bytes is not None:
        assert metrics.reachable_bytes >= metrics.total_data_bytes
    # the planted orphan (2MB+) must show up in the local estimate
    assert metrics.orphan_bytes_estimate is not None
    assert metrics.orphan_bytes_estimate > 1024 * 1024


def test_collect_empty_metrics_are_safe(seeded_catalog):
    table = seeded_catalog.load_table("db.healthy")
    metrics = collect(table, "db.healthy")
    assert metrics.data_file_count > 0
    assert metrics.delete_file_count == 0
    assert metrics.delete_ratio == 0.0
