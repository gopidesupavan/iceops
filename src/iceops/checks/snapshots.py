from __future__ import annotations

from typing import Optional

from ..models import Finding, Severity, TableMetrics
from .base import check

WARN_COUNT = 50
CRITICAL_COUNT = 500
WARN_OLDEST_DAYS = 30.0


@check("snapshot-bloat")
def snapshot_bloat(m: TableMetrics) -> Optional[Finding]:
    old = m.oldest_snapshot_age_days or 0.0
    if m.snapshot_count < WARN_COUNT and old < WARN_OLDEST_DAYS:
        return None
    severity = Severity.CRITICAL if m.snapshot_count >= CRITICAL_COUNT else Severity.WARN
    reasons = []
    if m.snapshot_count >= WARN_COUNT:
        reasons.append(f"{m.snapshot_count} snapshots retained")
    if old >= WARN_OLDEST_DAYS:
        reasons.append(f"oldest snapshot is {old:.0f} days old")
    return Finding(
        check_id="snapshot-bloat",
        severity=severity,
        message="; ".join(reasons),
        recommendation="expire old snapshots (iceops expire); every retained snapshot "
        "keeps its data files alive and grows metadata",
        data={
            "snapshot_count": m.snapshot_count,
            "oldest_snapshot_age_days": round(old, 1),
        },
    )
