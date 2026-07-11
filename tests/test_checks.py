from __future__ import annotations

from iceops.checks import all_checks
from iceops.checks.manifests import manifest_fragmentation
from iceops.checks.metadata_config import metadata_cleanup_disabled
from iceops.checks.small_files import small_files
from iceops.checks.snapshots import snapshot_bloat
from iceops.models import HealthReport, Severity, TableMetrics


def metrics(**overrides) -> TableMetrics:
    return TableMetrics(identifier="db.t", **overrides)


def test_small_files_warn_and_critical():
    assert small_files.run(metrics(data_file_count=10, small_file_ratio=0.9)) is None

    warn = small_files.run(
        metrics(data_file_count=50, small_file_count=20, small_file_ratio=0.4)
    )
    assert warn is not None and warn.severity == Severity.WARN

    critical = small_files.run(
        metrics(data_file_count=200, small_file_count=180, small_file_ratio=0.9)
    )
    assert critical is not None and critical.severity == Severity.CRITICAL


def test_snapshot_bloat_by_count_and_age():
    assert snapshot_bloat.run(metrics(snapshot_count=5)) is None
    assert snapshot_bloat.run(metrics(snapshot_count=60)).severity == Severity.WARN
    assert snapshot_bloat.run(metrics(snapshot_count=600)).severity == Severity.CRITICAL
    aged = snapshot_bloat.run(metrics(snapshot_count=5, oldest_snapshot_age_days=90.0))
    assert aged is not None and aged.severity == Severity.WARN


def test_manifest_fragmentation_needs_low_density():
    dense = metrics(manifest_count=100, data_file_count=100_000)
    assert manifest_fragmentation.run(dense) is None
    sparse = metrics(manifest_count=100, data_file_count=300)
    assert manifest_fragmentation.run(sparse).severity == Severity.WARN


def test_metadata_cleanup_is_info_only():
    finding = metadata_cleanup_disabled.run(metrics(snapshot_count=20))
    assert finding is not None and finding.severity == Severity.INFO
    enabled = metrics(
        snapshot_count=20,
        properties={"write.metadata.delete-after-commit.enabled": "true"},
    )
    assert metadata_cleanup_disabled.run(enabled) is None


def test_status_is_worst_finding_severity():
    def report(findings):
        return HealthReport(
            identifier="db.t", findings=[f for f in findings if f], metrics=metrics()
        )

    no_findings = [c.run(metrics()) for c in all_checks()]
    assert report(no_findings).status.value == "healthy"

    info_only = [metadata_cleanup_disabled.run(metrics(snapshot_count=20))]
    assert report(info_only).status.value == "healthy"  # info is advice, not a problem

    one_warn = [
        small_files.run(metrics(data_file_count=50, small_file_count=20, small_file_ratio=0.4))
    ]
    assert report(one_warn).status.value == "warn"

    one_critical = [snapshot_bloat.run(metrics(snapshot_count=600))]
    assert report(one_critical).status.value == "critical"
