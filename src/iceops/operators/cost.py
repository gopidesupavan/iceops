"""Wasted-storage cost estimate for a table (read-only).

Every byte gets one of three buckets:
- live:   referenced by the current snapshot — this IS your table
- stale:  only reachable through older snapshots — freed by expire (+ clean-orphans)
- orphan: referenced by nothing — freed by clean-orphans

THE FLOW
    1. `collect(table)` reuses the doctor pipeline's metrics
    2. stale = reachable(all snapshots) − (current data + current delete files),
       clamped ≥ 0. This is an UPPER BOUND: it assumes expiring every old snapshot.
    3. orphan estimate comes from the collector (local warehouses only; object stores
       report "unavailable" as an explicit note — never a silent zero)
    4. monthly waste = (stale + orphan) / 1024³ × --dollars-per-gb-month
       (default 0.023, S3 Standard). Unknowable buckets are excluded and noted,
       so the total is a floor, never an exaggeration.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..errors import TableNotFoundError
from ..inspect import collect
from ..models import CostReport

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog

DEFAULT_DOLLARS_PER_GB_MONTH = 0.023  # S3 standard, us-east-1

_GB = 1024**3


def cost(
    catalog: "Catalog",
    identifier: str,
    dollars_per_gb_month: float = DEFAULT_DOLLARS_PER_GB_MONTH,
) -> CostReport:
    try:
        table = catalog.load_table(identifier)
    except Exception as exc:
        raise TableNotFoundError(f"could not load table '{identifier}': {exc}") from exc

    metrics = collect(table, identifier)
    report = CostReport(
        identifier=identifier,
        live_bytes=metrics.total_data_bytes,
        reachable_bytes=metrics.reachable_bytes,
        orphan_bytes_estimate=metrics.orphan_bytes_estimate,
        dollars_per_gb_month=dollars_per_gb_month,
    )

    if metrics.reachable_bytes is not None:
        # reachable spans data AND delete files of every snapshot, so the current
        # snapshot's delete files must be subtracted too, not just its data files
        current_bytes = metrics.total_data_bytes + metrics.total_delete_bytes
        report.stale_bytes = max(metrics.reachable_bytes - current_bytes, 0)
    else:
        report.notes.append(
            "stale bytes unknown: this PyIceberg version exposes no all-files inspect "
            "endpoint; only current-snapshot data was measured"
        )

    if metrics.orphan_bytes_estimate is None:
        report.notes.append(
            "orphan estimate unavailable for this storage scheme (local warehouses only)"
        )

    waste = (report.stale_bytes or 0) + (report.orphan_bytes_estimate or 0)
    report.monthly_waste_dollars = round(waste / _GB * dollars_per_gb_month, 2)
    return report
