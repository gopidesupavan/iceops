"""Typed results shared by all frontends (CLI, API, library).

Operators return these models and never print; renderers decide presentation.
"""

from __future__ import annotations

import datetime as dt
import re
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


class PlanKind(str, Enum):
    """Whether iceops selected exact work or delegated selection to an engine."""

    EXACT = "exact"
    DELEGATED = "delegated"


class VerificationStatus(str, Enum):
    PLANNED = "planned"
    PASSED = "passed"
    SKIPPED = "skipped"
    FAILED = "failed"


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

    # mypy can't type decorators above @property (pydantic-docs-sanctioned ignore)
    @computed_field  # type: ignore[prop-decorator]  # included in --json output
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


class ExpireCandidate(BaseModel):
    snapshot_id: int
    committed_at: dt.datetime
    operation: Optional[str] = None


class ExpirePlan(BaseModel):
    identifier: str
    retain_last: int
    cutoff: dt.datetime
    candidates: list[ExpireCandidate] = Field(default_factory=list)
    snapshot_count: int = 0
    protected_ids: list[int] = Field(default_factory=list)
    unreferenced_data_bytes: Optional[int] = None
    unreferenced_manifest_bytes: Optional[int] = None
    engine: Optional[str] = None  # None = native; set = delegated to spark/trino
    action: Optional["Action"] = None
    engine_contract: Optional["EnginePlanContract"] = None
    warnings: list[str] = Field(default_factory=list)
    generated_at: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))

    @property
    def actionable(self) -> bool:
        # engine mode delegates candidate selection to the engine, so we can't enumerate
        # candidates up front — treat an engine plan as actionable if any snapshots exist
        if self.engine is not None:
            return self.snapshot_count > 0
        return bool(self.candidates)


class ExpireResult(BaseModel):
    plan: ExpirePlan
    expired_snapshot_ids: list[int] = Field(default_factory=list)
    snapshot_count_after: int = 0
    action_results: list["ActionResult"] = Field(default_factory=list)
    status: str = "expired"  # expired | nothing-to-do


class RewriteManifestsPlan(BaseModel):
    identifier: str
    manifest_count: int = 0
    manifest_bytes: int = 0
    files_per_manifest: float = 0.0
    estimated_after: int = 0
    target_manifest_size_bytes: int = 0
    engine: Optional[str] = None  # None = native; set = delegated to spark/trino
    action: Optional["Action"] = None
    engine_contract: Optional["EnginePlanContract"] = None
    warnings: list[str] = Field(default_factory=list)
    generated_at: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))

    @property
    def actionable(self) -> bool:
        # engine mode delegates file selection; actionable if there is >1 manifest to merge
        if self.engine is not None:
            return self.manifest_count > 1
        return self.manifest_count > 1 and self.estimated_after < self.manifest_count


class RewriteManifestsResult(BaseModel):
    plan: RewriteManifestsPlan
    manifests_before: int = 0
    manifests_after: int = 0
    new_snapshot_id: Optional[int] = None
    action_results: list["ActionResult"] = Field(default_factory=list)
    status: str = "rewritten"  # rewritten | nothing-to-do


class OrphanFile(BaseModel):
    path: str
    size_bytes: int = 0
    modified_at: Optional[dt.datetime] = None


class CleanOrphansPlan(BaseModel):
    identifier: str
    location: str = ""
    metadata_location_at_plan: str = ""  # execute re-verifies if the table moved past this
    candidates: list[OrphanFile] = Field(default_factory=list)
    total_bytes: int = 0
    listed_count: int = 0
    reachable_count: int = 0
    skipped: dict[str, int] = Field(default_factory=dict)  # young/excluded/metadata-json
    older_than_days: float = 3.0
    engine: Optional[str] = None  # None = native; set = delegated to spark/trino
    action: Optional["Action"] = None
    engine_contract: Optional["EnginePlanContract"] = None
    warnings: list[str] = Field(default_factory=list)
    generated_at: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))

    @property
    def actionable(self) -> bool:
        # engine mode delegates listing to the engine; there's always potentially
        # something to reclaim, so an engine plan is treated as actionable
        if self.engine is not None:
            return True
        return bool(self.candidates)


class CleanOrphansResult(BaseModel):
    plan: CleanOrphansPlan
    deleted: list[str] = Field(default_factory=list)
    freed_bytes: int = 0
    missing: list[str] = Field(default_factory=list)  # already gone when we got there
    spared: list[str] = Field(default_factory=list)  # re-check found them referenced
    action_results: list["ActionResult"] = Field(default_factory=list)
    status: str = "cleaned"  # cleaned | nothing-to-do


class EnginePlanContract(BaseModel):
    engine: str
    plan_kind: PlanKind = PlanKind.DELEGATED
    statement: str
    owns: list[str] = Field(default_factory=list)
    iceops_owns: list[str] = Field(default_factory=list)
    safety_notes: list[str] = Field(default_factory=list)
    verification_notes: list[str] = Field(default_factory=list)


class VerificationResult(BaseModel):
    check: str
    status: VerificationStatus
    before: Optional[int] = None
    after: Optional[int] = None
    note: Optional[str] = None


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


class CompactPlan(BaseModel):
    identifier: str
    engine: str = "native"
    engine_catalog: Optional[str] = None
    target_file_size_bytes: int = 512 * 1024 * 1024
    data_file_count: int = 0
    delete_file_count: int = 0
    small_file_count: int = 0
    total_data_bytes: int = 0
    current_snapshot_id: Optional[int] = None
    action: Optional[Action] = None
    engine_contract: Optional[EnginePlanContract] = None
    warnings: list[str] = Field(default_factory=list)
    generated_at: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))

    @property
    def actionable(self) -> bool:
        return self.small_file_count > 1 or self.delete_file_count > 0


class CompactResult(BaseModel):
    plan: CompactPlan
    action_results: list[ActionResult] = Field(default_factory=list)
    data_files_before: int = 0
    data_files_after: Optional[int] = None
    delete_files_before: int = 0
    delete_files_after: Optional[int] = None
    snapshot_before: Optional[int] = None
    snapshot_after: Optional[int] = None
    verifications: list[VerificationResult] = Field(default_factory=list)
    status: str = "compacted"  # compacted | nothing-to-do


class OpDecision(BaseModel):
    op: str
    will_run: bool
    reason: str  # why it runs or is skipped (e.g. "small-file-ratio 0.12 <= 0.3")


class TableApplyPlan(BaseModel):
    identifier: str
    engine: Optional[str] = None
    decisions: list[OpDecision] = Field(default_factory=list)

    @property
    def actionable(self) -> bool:
        return any(d.will_run for d in self.decisions)


class ApplyPlan(BaseModel):
    catalog: str
    tables: list[TableApplyPlan] = Field(default_factory=list)
    skipped: dict[str, str] = Field(default_factory=dict)  # identifier -> reason
    generated_at: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))

    @property
    def actionable(self) -> bool:
        return any(t.actionable for t in self.tables)


class TableApplyResult(BaseModel):
    identifier: str
    executed: list[str] = Field(default_factory=list)
    halted_at: Optional[str] = None
    error: Optional[str] = None


class ApplyResult(BaseModel):
    plan: ApplyPlan
    results: list[TableApplyResult] = Field(default_factory=list)
    status: str = "applied"  # applied | nothing-to-do


class TunePlan(BaseModel):
    """Composite of the four fix operators in maintenance order. tune holds only these
    typed sub-plans — it never plans a mutation of its own."""

    identifier: str
    engine: Optional[str] = None
    compact: Optional[CompactPlan] = None
    rewrite_manifests: Optional[RewriteManifestsPlan] = None
    expire: Optional[ExpirePlan] = None
    clean_orphans: Optional[CleanOrphansPlan] = None
    skipped: dict[str, str] = Field(default_factory=dict)  # step -> reason
    generated_at: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))

    @property
    def actionable(self) -> bool:
        return any(
            p is not None and p.actionable
            for p in (self.compact, self.rewrite_manifests, self.expire, self.clean_orphans)
        )


class TuneResult(BaseModel):
    plan: TunePlan
    compact: Optional[CompactResult] = None
    rewrite_manifests: Optional[RewriteManifestsResult] = None
    expire: Optional[ExpireResult] = None
    clean_orphans: Optional[CleanOrphansResult] = None
    executed: list[str] = Field(default_factory=list)
    halted_at: Optional[str] = None
    status: str = "tuned"  # tuned | nothing-to-do | halted


_DURATION_RE = re.compile(r"^(\d+)\s*([smhdw])$")
_DURATION_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def parse_duration(text: str) -> dt.timedelta:
    """Parse '30s', '12h', '7d', '2w' style durations (shared by expire and policy)."""
    match = _DURATION_RE.match(text.strip().lower())
    if not match:
        raise ValueError(f"invalid duration '{text}' (expected <number><unit>, units: s m h d w)")
    value, unit = match.groups()
    return dt.timedelta(seconds=int(value) * _DURATION_SECONDS[unit])


_SIZE_RE = re.compile(r"^(\d+)\s*(b|kb|mb|gb)$")
_SIZE_BYTES = {"b": 1, "kb": 1024, "mb": 1024**2, "gb": 1024**3}


def parse_size(text: str) -> int:
    """Parse '512MB', '8mb', '64KB' style sizes (powers of 1024)."""
    match = _SIZE_RE.match(text.strip().lower())
    if not match:
        raise ValueError(f"invalid size '{text}' (expected <number><unit>, units: B KB MB GB)")
    value, unit = match.groups()
    return int(value) * _SIZE_BYTES[unit]


def human_bytes(n: Optional[int]) -> str:
    if n is None:
        return "unknown"
    size = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if size < 1024 or unit == "PB":
            return f"{size:.1f}{unit}" if unit != "B" else f"{int(size)}B"
        size /= 1024
    return f"{n}B"
