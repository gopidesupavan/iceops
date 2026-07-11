"""Best-effort detection of tables iceops should not touch.

Two signals matter for fix operators:
- another optimizer already manages the table (double-optimizing causes commit fights)
- a streaming writer commits frequently (maintenance must expect commit conflicts)

Detection is heuristic by design; `doctor` surfaces it as information, and future fix
operators skip managed tables unless forced.
"""

from __future__ import annotations

from ..models import TableMetrics

# property-key substring -> manager name
_MANAGED_PROPERTY_MARKERS: list[tuple[str, str]] = [
    ("self-optimizing.enabled", "amoro"),
    ("amoro.", "amoro"),
    ("optimizer-group", "amoro"),
    ("s3tables", "s3-tables"),
    ("snowflake.", "snowflake"),
    ("delta.universalFormat", "databricks"),
    ("databricks.", "databricks"),
]

_STREAMING_SUMMARY_MARKERS = ("flink.job-id", "flink.operator-id", "spark.app.id")

# more than one commit an hour sustained looks like a streaming writer
STREAMING_SNAPSHOTS_PER_DAY = 24.0


def managed_by(properties: dict[str, str], location: str | None = None) -> str | None:
    for key, value in properties.items():
        lowered = key.lower()
        for marker, manager in _MANAGED_PROPERTY_MARKERS:
            if marker in lowered:
                if marker == "self-optimizing.enabled" and str(value).lower() != "true":
                    continue
                return manager
    if location and "--table-s3" in location:
        return "s3-tables"
    return None


def is_streaming_writer(metrics: TableMetrics) -> bool:
    for marker in _STREAMING_SUMMARY_MARKERS:
        if marker in metrics.last_snapshot_summary:
            return True
    cadence = metrics.snapshots_per_day
    return cadence is not None and cadence > STREAMING_SNAPSHOTS_PER_DAY
