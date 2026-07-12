"""Federated data-file compaction.

Compaction delegates the data rewrite to Spark or Trino. iceops owns the plan, safety
gates, and lifecycle ordering; the engine owns the actual Iceberg rewrite procedure.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..catalog.detect import managed_by
from ..engines import get_engine
from ..errors import IceopsError, TableNotFoundError
from ..models import (
    Action,
    CompactPlan,
    CompactResult,
    Plan,
    VerificationResult,
    VerificationStatus,
)
from ._engine_contract import delegated_contract

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table

DEFAULT_TARGET_FILE_SIZE = 512 * 1024 * 1024
# a file counts as "small" below 75% of target — the boundary Java's
# SizeBasedFileRewritePlanner uses, so the estimate matches what Spark users expect
SMALL_FILE_FRACTION = 0.75
SUPPORTED_FEDERATED_ENGINES = {"spark", "trino"}
DATA_FILE_CONTENT = 0


def compact(
    catalog: "Catalog",
    identifier: str,
    target_file_size: int = DEFAULT_TARGET_FILE_SIZE,
    engine: str = "native",
    engine_catalog: str | None = None,
    engine_config: dict[str, Any] | None = None,
    execute: bool = False,
    force: bool = False,
) -> CompactPlan | CompactResult:
    # validate the engine choice before any I/O — fail fast on a bad argument
    if engine == "native":
        raise IceopsError(
            "native compaction is not available yet — use --engine spark or --engine trino"
        )
    if engine not in SUPPORTED_FEDERATED_ENGINES:
        raise IceopsError(
            f"unknown compact engine '{engine}' (expected spark or trino; "
            f"native is not yet available)"
        )

    try:
        table = catalog.load_table(identifier)
    except Exception as exc:
        raise TableNotFoundError(f"could not load table '{identifier}': {exc}") from exc

    manager = managed_by({str(k): str(v) for k, v in table.properties.items()}, table.location())
    if manager and not force:
        raise IceopsError(
            f"'{identifier}' looks managed by {manager} — compacting behind another "
            f"optimizer's back causes commit conflicts. Use --force to override."
        )

    plan = _build_plan(table, identifier, target_file_size, engine, engine_catalog)
    if not execute:
        return plan
    return _execute(table, plan, engine_config or {})


def _build_plan(
    table: "Table",
    identifier: str,
    target_file_size: int,
    engine: str,
    engine_catalog: str | None,
) -> CompactPlan:
    snapshot = table.current_snapshot()
    plan = CompactPlan(
        identifier=identifier,
        engine=engine,
        engine_catalog=engine_catalog,
        target_file_size_bytes=target_file_size,
        current_snapshot_id=snapshot.snapshot_id if snapshot else None,
    )
    if snapshot is None:
        return plan

    files = table.inspect.files()
    names = files.column_names
    sizes = files.column("file_size_in_bytes").to_pylist()
    contents = files.column("content").to_pylist() if "content" in names else []

    for i, size in enumerate(sizes):
        content = int(contents[i]) if i < len(contents) and contents[i] is not None else 0
        if content == DATA_FILE_CONTENT:
            plan.data_file_count += 1
            plan.total_data_bytes += int(size)
            if int(size) < int(target_file_size * SMALL_FILE_FRACTION):
                plan.small_file_count += 1
        else:
            plan.delete_file_count += 1

    if plan.actionable:
        engine_catalog = engine_catalog or _catalog_name_from_table(table)
        plan.engine_catalog = engine_catalog
        if not engine_catalog:
            plan.warnings.append(
                "engine catalog is unknown; pass --engine-catalog so the engine can find the table"
            )
        plan.action = Action(
            op="compact",
            table=identifier,
            params={
                "table": identifier,
                "engine": engine,
                "engine_catalog": engine_catalog,
                "target_file_size_bytes": target_file_size,
            },
            estimated={
                "data_file_count": plan.data_file_count,
                "delete_file_count": plan.delete_file_count,
                "small_file_count": plan.small_file_count,
                "total_data_bytes": plan.total_data_bytes,
            },
        )
        plan.engine_contract = delegated_contract(
            engine,
            plan.action,
            owns=[
                "exact data-file selection",
                "data rewrite commit",
                "delete-file rewrite semantics",
            ],
            iceops_owns=[
                "table load and managed-table refusal",
                "small-file estimate",
                "statement construction",
                "post-run metadata refresh",
            ],
            safety_notes=[
                f"{engine} chooses the exact files to rewrite.",
                "iceops does not delete physical files during compact.",
                "old files remain until expire runs, then become clean-orphans candidates.",
            ],
            verification_notes=[
                "row-count verification runs after execution when snapshot metadata exposes total-records"
            ],
        )
    return plan


def _catalog_name_from_table(table: "Table") -> str | None:
    name = getattr(getattr(table, "catalog", None), "name", None)
    return str(name) if name else None


def verify_row_count(
    identifier: str,
    before: int | None,
    after: int | None,
    snapshot_id: int | None,
) -> VerificationResult:
    """Return the row-count verification result, raising on unsafe mismatch."""
    if before is None or after is None:
        return VerificationResult(
            check="row_count",
            status=VerificationStatus.SKIPPED,
            before=before,
            after=after,
            note="snapshot metadata did not expose total-records before and after compaction",
        )
    if before != after:
        raise IceopsError(
            f"compaction changed the row count of '{identifier}' ({before} -> {after}) "
            f"— the engine's rewrite is unsafe. The pre-compaction snapshot {snapshot_id} "
            f"is intact; roll back via table.manage_snapshots().rollback_to_snapshot()."
        )
    return VerificationResult(
        check="row_count",
        status=VerificationStatus.PASSED,
        before=before,
        after=after,
    )


def _total_records(table: "Table") -> int | None:
    """Row count from snapshot metadata (cheap — no data scan). None if unavailable."""
    snapshot = table.current_snapshot()
    summary = getattr(snapshot, "summary", None) if snapshot else None
    props = getattr(summary, "additional_properties", None) or {}
    value = props.get("total-records")
    return int(value) if value is not None else None


def _execute(table: "Table", plan: CompactPlan, engine_config: dict[str, Any]) -> CompactResult:
    if not plan.actionable or plan.action is None:
        return CompactResult(
            plan=plan,
            data_files_before=plan.data_file_count,
            data_files_after=plan.data_file_count,
            delete_files_before=plan.delete_file_count,
            delete_files_after=plan.delete_file_count,
            snapshot_before=plan.current_snapshot_id,
            snapshot_after=plan.current_snapshot_id,
            status="nothing-to-do",
        )

    if not plan.engine_catalog:
        raise IceopsError("cannot execute compact plan without engine_catalog")

    # capture the row count BEFORE handing the rewrite to the engine — compaction is
    # the only op that rewrites data, so iceops verifies the engine preserved every row
    rows_before = _total_records(table)

    results = get_engine(plan.engine, **engine_config).execute(
        Plan(table=plan.identifier, actions=[plan.action])
    )
    table.refresh()

    verification = verify_row_count(
        plan.identifier, rows_before, _total_records(table), plan.current_snapshot_id
    )

    after = _build_plan(
        table,
        plan.identifier,
        plan.target_file_size_bytes,
        plan.engine,
        plan.engine_catalog,
    )
    return CompactResult(
        plan=plan,
        action_results=results,
        data_files_before=plan.data_file_count,
        data_files_after=after.data_file_count,
        delete_files_before=plan.delete_file_count,
        delete_files_after=after.delete_file_count,
        snapshot_before=plan.current_snapshot_id,
        snapshot_after=after.current_snapshot_id,
        verifications=[verification],
        status="compacted",
    )
