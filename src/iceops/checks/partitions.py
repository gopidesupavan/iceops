from __future__ import annotations

from typing import Optional

from ..models import Finding, Severity, TableMetrics
from .base import check

WARN_SKEW = 10.0
MIN_PARTITIONS = 5


@check("partition-skew")
def partition_skew(m: TableMetrics) -> Optional[Finding]:
    if (
        m.partition_file_skew is None
        or m.partition_count < MIN_PARTITIONS
        or m.partition_file_skew < WARN_SKEW
    ):
        return None
    return Finding(
        check_id="partition-skew",
        severity=Severity.WARN,
        message=(
            f"hottest partition has {m.partition_file_skew:.0f}x the median file count "
            f"across {m.partition_count} partitions"
        ),
        recommendation="revisit the partition spec (Iceberg supports in-place partition "
        "evolution) or compact the hot partitions",
        data={
            "partition_count": m.partition_count,
            "partition_file_skew": round(m.partition_file_skew, 1),
        },
    )
