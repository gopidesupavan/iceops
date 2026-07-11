"""Fleet-wide health scan (read-only).

THE FLOW
    1. walk every namespace in the catalog, glob-match table names against --pattern
    2. run `doctor` on each table; a per-table failure becomes a TableError entry
       instead of aborting the fleet (one broken table must not hide the other 500)
    3. assemble FleetReport with per-status counts
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..catalog import list_table_identifiers
from ..models import FleetReport, TableError
from .doctor import doctor

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog


def scan(catalog: "Catalog", catalog_name: str, pattern: str = "*") -> FleetReport:
    report = FleetReport(catalog=catalog_name, pattern=pattern)
    for identifier in list_table_identifiers(catalog, pattern):
        try:
            report.reports.append(doctor(catalog, identifier))
        except Exception as exc:
            report.errors.append(TableError(identifier=identifier, error=str(exc)))
    return report
