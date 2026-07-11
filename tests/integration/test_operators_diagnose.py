from __future__ import annotations

import pytest

from iceops.errors import TableNotFoundError
from iceops.operators import cost, doctor, scan


def test_doctor_flags_messy_not_healthy(seeded_catalog):
    messy = doctor(seeded_catalog, "db.messy")
    healthy = doctor(seeded_catalog, "db.healthy")

    assert messy.status.value in ("warn", "critical")
    assert any(f.check_id == "small-files" for f in messy.findings)
    assert healthy.status.value == "healthy"


def test_doctor_missing_table(seeded_catalog):
    with pytest.raises(TableNotFoundError):
        doctor(seeded_catalog, "db.nope")


def test_scan_covers_all_tables(seeded_catalog):
    report = scan(seeded_catalog, "test")
    identifiers = {r.identifier for r in report.reports}
    # other test modules may add tables to the session catalog; scan must see at least these
    assert identifiers >= {"db.messy", "db.healthy"}
    assert not report.errors
    assert set(report.status_counts) >= {"healthy"}


def test_scan_pattern_filters(seeded_catalog):
    report = scan(seeded_catalog, "test", pattern="db.mes*")
    assert [r.identifier for r in report.reports] == ["db.messy"]


def test_cost_reports_waste(seeded_catalog):
    report = cost(seeded_catalog, "db.messy")
    assert report.live_bytes > 0
    if report.stale_bytes is not None:
        assert report.stale_bytes >= 0
    assert report.orphan_bytes_estimate and report.orphan_bytes_estimate > 1024 * 1024
    assert report.monthly_waste_dollars is not None
