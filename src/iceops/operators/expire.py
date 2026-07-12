"""Snapshot expiration.

WHY THIS EXISTS
    Every write creates a snapshot (a "save point"); Iceberg never discards them on its
    own. Old snapshots bloat metadata and keep replaced files alive on storage. Expire
    forgets chosen old versions. Your current data is untouched; what you lose is time
    travel to the expired versions only.

HOW IT WORKS — THE FLOW

    plan (read-only, `_build_plan`):
      1. list snapshots from table metadata; collect protected ids (branch/tag heads,
         which includes the current snapshot via the `main` branch ref)
      2. `select_candidates` (pure function): a snapshot is expired only if ALL hold —
         not protected, NOT among the newest --retain-last, AND older than --older-than
         (conservative intersection: both knobs must agree)
      3. unreferenced-bytes accounting (`_unreferenced_bytes`): reachable-set difference —
         union of files across ALL snapshots minus union across SURVIVORS. Same algorithm
         family as Java's ReachableFileCleanup / Spark's anti-join; we deliberately avoid
         Java's faster incremental walk (correctness edges with branches/tags)
      4. warn if the commit cadence looks like a streaming writer (lagging incremental
         readers WILL break when their snapshot vanishes)

    execute (`_execute`):
      5. hand the EXACT planned ids to native PyIceberg:
         `table.maintenance.expire_snapshots().by_ids(ids).commit()` — what the user
         reviewed is what happens; PyIceberg re-checks ref protection a second time
      6. on commit conflict: refresh, retry once, then fail loudly
      7. IMPORTANT: PyIceberg 0.11.x expiration is metadata-only — it deletes NO physical
         files (verified from source; pinned by a test that fails if an upgrade changes
         this). Freed storage comes later, from clean-orphans. We therefore report bytes
         as "become unreferenced", never "freed".
"""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Optional

from ..catalog.detect import STREAMING_SNAPSHOTS_PER_DAY, managed_by
from ..engines import get_engine, validate_engine
from ..errors import IceopsError, TableNotFoundError
from ..models import Action, ExpireCandidate, ExpirePlan, ExpireResult, Plan
from ._engine_contract import catalog_name_from_table, delegated_contract

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table

DEFAULT_RETAIN_LAST = 10
DEFAULT_OLDER_THAN = dt.timedelta(days=7)


def select_candidates(
    snapshots: list[tuple[int, int]],
    protected_ids: set[int],
    retain_last: int,
    cutoff_ms: int,
) -> list[int]:
    """Pure candidate selection over (snapshot_id, timestamp_ms) pairs.

    A snapshot is expired only if ALL hold (conservative intersection):
    not ref-protected, not among the newest `retain_last`, and older than the cutoff.
    """
    newest_first = sorted(snapshots, key=lambda s: s[1], reverse=True)
    retained = {sid for sid, _ in newest_first[: max(retain_last, 0)]}
    return [
        sid
        for sid, ts in newest_first
        if sid not in protected_ids and sid not in retained and ts < cutoff_ms
    ]


def expire(
    catalog: "Catalog",
    identifier: str,
    retain_last: int = DEFAULT_RETAIN_LAST,
    older_than: dt.timedelta = DEFAULT_OLDER_THAN,
    engine: Optional[str] = None,
    engine_catalog: Optional[str] = None,
    engine_config: Optional[dict] = None,
    execute: bool = False,
    force: bool = False,
) -> ExpirePlan | ExpireResult:
    try:
        table = catalog.load_table(identifier)
    except Exception as exc:
        raise TableNotFoundError(f"could not load table '{identifier}': {exc}") from exc

    manager = managed_by({str(k): str(v) for k, v in table.properties.items()}, table.location())
    if manager and not force:
        raise IceopsError(
            f"'{identifier}' looks managed by {manager} — expiring behind another "
            f"optimizer's back causes commit conflicts. Use --force to override."
        )

    if engine is not None:
        validate_engine(engine)
        return _expire_via_engine(
            table,
            identifier,
            retain_last,
            older_than,
            engine,
            engine_catalog,
            engine_config,
            execute,
        )

    plan = _build_plan(table, identifier, retain_last, older_than)
    if not execute:
        return plan
    return _execute(table, plan)


def _expire_via_engine(
    table: "Table",
    identifier: str,
    retain_last: int,
    older_than: dt.timedelta,
    engine: str,
    engine_catalog: Optional[str],
    engine_config: Optional[dict],
    execute: bool,
) -> ExpirePlan | ExpireResult:
    """Delegate expiry to the engine's expire_snapshots procedure. The engine selects
    which snapshots to drop (and deletes their files); iceops passes retain_last +
    older_than and reports before/after counts."""
    engine_catalog = engine_catalog or catalog_name_from_table(table)
    snapshots = table.metadata.snapshots or []
    cutoff = dt.datetime.now(dt.timezone.utc) - older_than
    action = Action(
        op="expire",
        table=identifier,
        params={
            "engine_catalog": engine_catalog,
            "table": identifier,
            "retain_last": retain_last,
            "older_than_seconds": int(older_than.total_seconds()),
        },
        estimated={"snapshot_count": len(snapshots)},
    )
    plan = ExpirePlan(
        identifier=identifier,
        retain_last=retain_last,
        cutoff=cutoff,
        snapshot_count=len(snapshots),
        engine=engine,
        action=action,
    )
    if engine_catalog:
        plan.engine_contract = delegated_contract(
            engine,
            action,
            owns=[
                "snapshot selection",
                "engine retention rules",
                "physical file cleanup semantics",
            ],
            iceops_owns=[
                "table load and managed-table refusal",
                "retain-last and older-than parameters",
                "before/after snapshot count reporting",
            ],
            safety_notes=[
                f"{engine} selects the exact snapshots to expire.",
                "engine expiry may delete physical files according to engine/catalog rules.",
                "native iceops expire remains metadata-only; this delegated path uses engine semantics.",
            ],
        )
    else:
        plan.warnings.append(
            "engine catalog is unknown; pass --engine-catalog so the engine can find the table"
        )
    if not execute:
        return plan
    if not engine_catalog:
        raise IceopsError("engine expiry needs --engine-catalog so the engine finds the table")

    results = get_engine(engine, **(engine_config or {})).execute(
        Plan(table=identifier, actions=[action])
    )
    table.refresh()
    return ExpireResult(
        plan=plan,
        snapshot_count_after=len(table.metadata.snapshots or []),
        action_results=results,
    )


def _build_plan(
    table: "Table", identifier: str, retain_last: int, older_than: dt.timedelta
) -> ExpirePlan:
    snapshots = table.metadata.snapshots or []
    protected_ids = {ref.snapshot_id for ref in table.metadata.refs.values()}
    cutoff = dt.datetime.now(dt.timezone.utc) - older_than
    cutoff_ms = int(cutoff.timestamp() * 1000)

    candidate_ids = select_candidates(
        [(s.snapshot_id, s.timestamp_ms) for s in snapshots],
        protected_ids,
        retain_last,
        cutoff_ms,
    )
    by_id = {s.snapshot_id: s for s in snapshots}
    candidates = [
        ExpireCandidate(
            snapshot_id=sid,
            committed_at=dt.datetime.fromtimestamp(by_id[sid].timestamp_ms / 1000, dt.timezone.utc),
            operation=_operation(by_id[sid]),
        )
        for sid in sorted(candidate_ids, key=lambda s: by_id[s].timestamp_ms)
    ]

    plan = ExpirePlan(
        identifier=identifier,
        retain_last=retain_last,
        cutoff=cutoff,
        candidates=candidates,
        snapshot_count=len(snapshots),
        protected_ids=sorted(protected_ids),
    )

    if candidates:
        survivor_ids = [s.snapshot_id for s in snapshots if s.snapshot_id not in candidate_ids]
        data_bytes, manifest_bytes = _unreferenced_bytes(table, set(candidate_ids), survivor_ids)
        plan.unreferenced_data_bytes = data_bytes
        plan.unreferenced_manifest_bytes = manifest_bytes
        if data_bytes is None:
            plan.warnings.append(
                "could not estimate unreferenced data bytes on this PyIceberg version"
            )

    _warn_streaming(table, snapshots, plan)
    return plan


def _operation(snapshot: object) -> Optional[str]:
    summary = getattr(snapshot, "summary", None)
    operation = getattr(summary, "operation", None)
    return str(operation.value if hasattr(operation, "value") else operation) if operation else None


def _unreferenced_bytes(
    table: "Table", candidate_ids: set[int], survivor_ids: list[int]
) -> tuple[Optional[int], Optional[int]]:
    """Reachable-set difference: bytes referenced by expired snapshots only.

    Metadata reads only — nothing is listed or deleted here.
    """
    data_bytes: Optional[int] = None
    try:
        all_files = table.inspect.all_files()
        every: dict[str, int] = {}
        paths = all_files.column("file_path").to_pylist()
        sizes = all_files.column("file_size_in_bytes").to_pylist()
        for path, size in zip(paths, sizes):
            every[str(path)] = int(size)

        surviving: set[str] = set()
        for sid in survivor_ids:
            files = table.inspect.files(snapshot_id=sid)
            surviving.update(str(p) for p in files.column("file_path").to_pylist())
        data_bytes = sum(size for path, size in every.items() if path not in surviving)
    except Exception:
        data_bytes = None

    manifest_bytes: Optional[int] = None
    try:
        manifests = table.inspect.all_manifests()
        names = manifests.column_names
        if {"path", "length", "reference_snapshot_id"} <= set(names):
            refs_by_path: dict[str, set[int]] = {}
            length_by_path: dict[str, int] = {}
            for row in manifests.to_pylist():
                path = str(row["path"])
                refs_by_path.setdefault(path, set()).add(int(row["reference_snapshot_id"]))
                length_by_path[path] = int(row["length"])
            manifest_bytes = sum(
                length_by_path[path] for path, refs in refs_by_path.items() if refs <= candidate_ids
            )
    except Exception:
        manifest_bytes = None

    return data_bytes, manifest_bytes


def _warn_streaming(table: "Table", snapshots: list, plan: ExpirePlan) -> None:
    if len(snapshots) < 2:
        return
    timestamps = sorted(s.timestamp_ms for s in snapshots)
    span_days = max((timestamps[-1] - timestamps[0]) / 86_400_000, 1 / 24)
    cadence = len(snapshots) / span_days
    current = table.current_snapshot()
    summary_keys = (
        list(getattr(current.summary, "additional_properties", {}) or {}) if current else []
    )
    if cadence > STREAMING_SNAPSHOTS_PER_DAY or any("flink" in k for k in summary_keys):
        plan.warnings.append(
            "streaming writer detected: incremental readers still consuming from expired "
            "snapshots WILL break — make sure no reader lags past the cutoff"
        )


def _execute(table: "Table", plan: ExpirePlan) -> ExpireResult:
    if not plan.candidates:
        return ExpireResult(
            plan=plan, status="nothing-to-do", snapshot_count_after=plan.snapshot_count
        )

    ids = [c.snapshot_id for c in plan.candidates]
    try:
        _commit_expire(table, ids)
    except ValueError as exc:
        # by_id validates existence/protection at commit time — table changed under us
        raise IceopsError(
            f"table changed between plan and execute ({exc}) — re-run iceops expire"
        ) from exc

    table.refresh()
    return ExpireResult(
        plan=plan,
        expired_snapshot_ids=ids,
        snapshot_count_after=len(table.metadata.snapshots or []),
    )


def _commit_expire(table: "Table", ids: list[int]) -> None:
    try:
        table.maintenance.expire_snapshots().by_ids(ids).commit()
    except ValueError:
        raise
    except Exception:
        # one retry on commit conflicts (concurrent writer), then fail loudly
        table.refresh()
        table.maintenance.expire_snapshots().by_ids(ids).commit()
