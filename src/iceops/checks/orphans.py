from __future__ import annotations

from typing import Optional

from ..models import Finding, Severity, TableMetrics, human_bytes
from .base import check

WARN_BYTES = 1 * 1024 * 1024  # local-warehouse heuristic; object stores come in v0.2


@check("orphan-files")
def orphan_files(m: TableMetrics) -> Optional[Finding]:
    if m.orphan_bytes_estimate is None or m.orphan_bytes_estimate < WARN_BYTES:
        return None
    return Finding(
        check_id="orphan-files",
        severity=Severity.WARN,
        message=(
            f"~{human_bytes(m.orphan_bytes_estimate)} in the table location is not "
            f"referenced by any snapshot"
        ),
        recommendation="clean orphan files (iceops clean-orphans); failed writes and "
        "past compactions leave dead files that storage still bills for",
        data={"orphan_bytes_estimate": m.orphan_bytes_estimate},
    )
