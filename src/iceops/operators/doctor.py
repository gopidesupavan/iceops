"""Single-table health diagnosis (read-only).

THE FLOW
    1. load the table through the catalog (reads metadata.json — never data files)
    2. `collect(table)` → TableMetrics: all numbers, from metadata only (see collector.py)
    3. run every registered check (or a caller-supplied subset via `checks=`); each is a
       pure function TableMetrics → Finding | None
    4. detect externally-managed tables (Amoro/S3 Tables/…) and streaming writers —
       informational here; fix operators use the same detection to refuse
    5. assemble HealthReport; `status` is computed as the worst finding severity
       (info findings are advice and never change it)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence

from ..catalog.detect import is_streaming_writer, managed_by
from ..checks import Check, all_checks
from ..errors import TableNotFoundError
from ..inspect import collect
from ..models import HealthReport

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog


def doctor(
    catalog: "Catalog",
    identifier: str,
    checks: Optional[Sequence[Check]] = None,
) -> HealthReport:
    try:
        table = catalog.load_table(identifier)
    except Exception as exc:
        raise TableNotFoundError(f"could not load table '{identifier}': {exc}") from exc

    metrics = collect(table, identifier)
    findings = []
    for chk in checks if checks is not None else all_checks():
        finding = chk.run(metrics)
        if finding is not None:
            findings.append(finding)

    return HealthReport(
        identifier=identifier,
        findings=findings,
        metrics=metrics,
        managed_by=managed_by(metrics.properties, metrics.location),
        streaming_writer=is_streaming_writer(metrics),
    )
