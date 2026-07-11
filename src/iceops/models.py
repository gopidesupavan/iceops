"""Typed results shared by all frontends (CLI, API, library).

Operators return these models and never print; renderers decide presentation.
"""

from __future__ import annotations

import datetime as dt
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, computed_field

HISTOGRAM_BUCKETS: list[tuple[str, int]] = [
    ("<1MB", 1 * 1024 * 1024),
    ("1-8MB", 8 * 1024 * 1024),
    ("8-32MB", 32 * 1024 * 1024),
    ("32-128MB", 128 * 1024 * 1024),
    ("128-512MB", 512 * 1024 * 1024),
    (">512MB", 2**63),
]

SMALL_FILE_BYTES = 32 * 1024 * 1024


class Severity(str, Enum):
    INFO = "info"
    WARN = "warn"
    CRITICAL = "critical"


class Status(str, Enum):
    """Worst finding severity, surfaced directly: no letter-grade mapping to decode."""

    HEALTHY = "healthy"
    WARN = "warn"
    CRITICAL = "critical"


class TableMetrics(BaseModel):
    identifier: str
    location: Optional[str] = None
    format_version: Optional[int] = None
    properties: dict[str, str] = Field(default_factory=dict)

    data_file_count: int = 0
    delete_file_count: int = 0
    total_data_bytes: int = 0
    total_delete_bytes: int = 0
    avg_file_bytes: int = 0
    small_file_count: int = 0
    small_file_ratio: float = 0.0
    file_size_histogram: dict[str, int] = Field(default_factory=dict)

    snapshot_count: int = 0
    oldest_snapshot_age_days: Optional[float] = None
    newest_snapshot_age_days: Optional[float] = None
    snapshots_per_day: Optional[float] = None
    last_snapshot_summary: dict[str, str] = Field(default_factory=dict)

    manifest_count: int = 0
    avg_manifest_bytes: int = 0

    delete_ratio: float = 0.0

    partition_count: int = 0
    partition_file_skew: Optional[float] = None

    reachable_bytes: Optional[int] = None
    filesystem_bytes: Optional[int] = None
    orphan_bytes_estimate: Optional[int] = None


class Finding(BaseModel):
    check_id: str
    severity: Severity
    message: str
    recommendation: str
    data: dict[str, Any] = Field(default_factory=dict)


class HealthReport(BaseModel):
    identifier: str
    findings: list[Finding] = Field(default_factory=list)
    metrics: TableMetrics
    managed_by: Optional[str] = None
    streaming_writer: bool = False
    generated_at: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))

    @computed_field  # included in --json output
    @property
    def status(self) -> Status:
        """info findings are advice, not problems — they never change the status."""
        if any(f.severity == Severity.CRITICAL for f in self.findings):
            return Status.CRITICAL
        if any(f.severity == Severity.WARN for f in self.findings):
            return Status.WARN
        return Status.HEALTHY


class TableError(BaseModel):
    identifier: str
    error: str


class FleetReport(BaseModel):
    catalog: str
    pattern: str = "*"
    reports: list[HealthReport] = Field(default_factory=list)
    errors: list[TableError] = Field(default_factory=list)
    generated_at: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))

    @property
    def status_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for report in self.reports:
            counts[report.status.value] = counts.get(report.status.value, 0) + 1
        return counts


class CostReport(BaseModel):
    identifier: str
    live_bytes: int = 0
    reachable_bytes: Optional[int] = None
    stale_bytes: Optional[int] = None
    orphan_bytes_estimate: Optional[int] = None
    dollars_per_gb_month: float
    monthly_waste_dollars: Optional[float] = None
    notes: list[str] = Field(default_factory=list)
    generated_at: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))


class Action(BaseModel):
    op: str
    table: str
    params: dict[str, Any] = Field(default_factory=dict)
    estimated: dict[str, Any] = Field(default_factory=dict)


class Plan(BaseModel):
    table: str
    actions: list[Action] = Field(default_factory=list)
    dry_run: bool = True


class ActionResult(BaseModel):
    action: Action
    status: str
    details: dict[str, Any] = Field(default_factory=dict)


def human_bytes(n: Optional[int]) -> str:
    if n is None:
        return "unknown"
    size = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if size < 1024 or unit == "PB":
            return f"{size:.1f}{unit}" if unit != "B" else f"{int(size)}B"
        size /= 1024
    return f"{n}B"
