from __future__ import annotations

from typing import Optional

from ..models import Finding, Severity, TableMetrics
from .base import check

WARN_COUNT = 32
CRITICAL_COUNT = 200


@check("manifest-fragmentation")
def manifest_fragmentation(m: TableMetrics) -> Optional[Finding]:
    if m.manifest_count < WARN_COUNT:
        return None
    # many manifests is only fragmentation if there are few files per manifest
    files_per_manifest = (
        (m.data_file_count + m.delete_file_count) / m.manifest_count if m.manifest_count else 0
    )
    if files_per_manifest >= 100:
        return None
    severity = Severity.CRITICAL if m.manifest_count >= CRITICAL_COUNT else Severity.WARN
    return Finding(
        check_id="manifest-fragmentation",
        severity=severity,
        message=(
            f"{m.manifest_count} manifest files averaging "
            f"{files_per_manifest:.0f} data files each — query planning reads every one"
        ),
        recommendation="rewrite manifests (iceops rewrite-manifests) to consolidate; planning overhead "
        "grows linearly with manifest count",
        data={
            "manifest_count": m.manifest_count,
            "files_per_manifest": round(files_per_manifest, 1),
        },
    )
