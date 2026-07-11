from __future__ import annotations

from typing import Optional

from ..models import Finding, Severity, TableMetrics
from .base import check

PROPERTY = "write.metadata.delete-after-commit.enabled"
MIN_SNAPSHOTS_TO_CARE = 10


@check("metadata-cleanup-disabled")
def metadata_cleanup_disabled(m: TableMetrics) -> Optional[Finding]:
    if m.properties.get(PROPERTY, "").lower() == "true":
        return None
    if m.snapshot_count < MIN_SNAPSHOTS_TO_CARE:
        return None
    return Finding(
        check_id="metadata-cleanup-disabled",
        severity=Severity.INFO,
        message=f"{PROPERTY} is not enabled; metadata.json files accumulate on every commit",
        recommendation=f"set {PROPERTY}=true (and write.metadata.previous-versions-max) so "
        "the writer prunes old metadata files automatically",
        data={"snapshot_count": m.snapshot_count},
    )
