from __future__ import annotations

from typing import Optional

from ..models import Finding, Severity, TableMetrics
from .base import check

WARN_RATIO = 0.1
CRITICAL_RATIO = 0.3


@check("delete-files")
def delete_files(m: TableMetrics) -> Optional[Finding]:
    if m.delete_file_count == 0 or m.delete_ratio < WARN_RATIO:
        return None
    severity = Severity.CRITICAL if m.delete_ratio >= CRITICAL_RATIO else Severity.WARN
    return Finding(
        check_id="delete-files",
        severity=severity,
        message=(
            f"{m.delete_file_count} delete files against {m.data_file_count} data files "
            f"({m.delete_ratio:.0%}) — reads must merge deletes on the fly"
        ),
        recommendation="compact to fold deletes into data files; merge-on-read tables "
        "degrade steadily without it",
        data={
            "delete_file_count": m.delete_file_count,
            "delete_ratio": round(m.delete_ratio, 3),
        },
    )
