"""Single-table health diagnosis."""

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
