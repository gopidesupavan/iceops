from __future__ import annotations

from typing import Optional

from ..models import Finding, Severity, TableMetrics, human_bytes
from .base import check

WARN_RATIO = 0.3
CRITICAL_RATIO = 0.6
MIN_FILES_WARN = 20
MIN_FILES_CRITICAL = 100


@check("small-files")
def small_files(m: TableMetrics) -> Optional[Finding]:
    if m.data_file_count < MIN_FILES_WARN or m.small_file_ratio < WARN_RATIO:
        return None
    severity = Severity.WARN
    if m.small_file_ratio >= CRITICAL_RATIO and m.data_file_count >= MIN_FILES_CRITICAL:
        severity = Severity.CRITICAL
    return Finding(
        check_id="small-files",
        severity=severity,
        message=(
            f"{m.small_file_count} of {m.data_file_count} data files "
            f"({m.small_file_ratio:.0%}) are under 32MB (avg {human_bytes(m.avg_file_bytes)})"
        ),
        recommendation="compact the table (iceops compact --engine spark|trino); "
        "queries pay an open-file cost per small file",
        data={
            "small_file_count": m.small_file_count,
            "data_file_count": m.data_file_count,
            "small_file_ratio": round(m.small_file_ratio, 3),
        },
    )
