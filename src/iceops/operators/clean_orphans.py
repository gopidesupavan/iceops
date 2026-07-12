"""Orphan-file cleanup — THE ONLY code in iceops that deletes physical files.

WHY THIS EXISTS
    An orphan is a file inside the table location that NOTHING references: failed-write
    debris, plus everything expire and rewrite-manifests deliberately unreference.
    Iceberg reads are purely metadata-driven, so orphans are invisible to every query —
    their only effect is a storage bill. Deleting them is pure garbage collection.

HOW IT WORKS — THE FLOW

    plan (read-only, `_build_plan`):
      1. `_reachable(table)`: every path Iceberg metadata knows about, from SIX sources —
         all data/delete files (all snapshots) + all manifests + every manifest list +
         every metadata-log entry + current metadata.json + statistics files.
         Missing a source here would delete live data; the list is review-mandatory.
      2. `_listing(table)`: what is ACTUALLY on storage — recursive listing through
         PyIceberg's own filesystem (same credentials as the catalog), with size + mtime
      3. `normalize_path` both sides (safety-critical: 'file:///x' == '/x',
         's3://b/k' == 'b/k') so set membership can't miss on representation
      4. `filter_candidates` — THE FUNNEL, each stage only narrows:
         listed  →  not reachable  →  inside table location  →  not *.metadata.json /
         version-hint.text (NEVER deleted, regardless of anything)  →  older than
         --older-than (default 3d; unknown mtime = young = untouchable)  →  not --exclude
      5. the plan records `metadata_location_at_plan` — the exact table version the
         decision was based on

    execute (`execute_plan`), in batches of --batch-size:
      6. before EVERY batch: refresh the table; if metadata moved past the version the
         plan saw (ANY commit — even one between plan and execute), recompute the
         reachable set and spare anything newly referenced. This exact gap was a real
         bug caught by the concurrency race test before shipping.
      7. delete via `table.io.delete` (native IO); already-gone files are tolerated;
         a literal log of every deleted path is kept
      8. worst failure mode by construction: deleting too LITTLE. A crash mid-run just
         leaves fewer orphans; re-running resumes safely.
"""

from __future__ import annotations

import datetime as dt
import fnmatch
from typing import TYPE_CHECKING, Iterable, Optional
from urllib.parse import unquote, urlparse

from ..catalog.detect import managed_by
from ..errors import IceopsError, TableNotFoundError
from ..models import Action, CleanOrphansPlan, CleanOrphansResult, OrphanFile, Plan
from ._engine_contract import catalog_name_from_table, delegated_contract

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table

DEFAULT_OLDER_THAN = dt.timedelta(days=3)  # matches Spark's action convention
DEFAULT_BATCH_SIZE = 100

# never deleted, regardless of reachability: the audit/undo chain of last resort
PROTECTED_NAMES = ("version-hint.text",)
PROTECTED_SUFFIXES = (".metadata.json",)


def normalize_path(path: str) -> str:
    """Canonical scheme-less form for set membership across metadata URIs and listings.

    'file:///wh/t/f.parquet' == '/wh/t/f.parquet'; 's3://bucket/k' == 'bucket/k'.
    Unsafe normalization here means deleting live data — see the unit matrix.
    """
    parsed = urlparse(path)
    if parsed.scheme in ("", "file"):
        return unquote(parsed.path) if parsed.scheme == "file" else unquote(path)
    return parsed.netloc + unquote(parsed.path)


def filter_candidates(
    listed: Iterable[OrphanFile],
    reachable: set[str],
    location_prefix: str,
    cutoff: dt.datetime,
    exclude: tuple[str, ...],
) -> tuple[list[OrphanFile], dict[str, int]]:
    """The funnel, as a pure function. Every stage narrows; nothing ever widens."""
    candidates: list[OrphanFile] = []
    skipped = {"young": 0, "excluded": 0, "metadata-json": 0, "out-of-scope": 0}
    for f in listed:
        norm = normalize_path(f.path)
        if norm in reachable:
            continue
        if not norm.startswith(location_prefix):
            skipped["out-of-scope"] += 1
            continue
        name = norm.rsplit("/", 1)[-1]
        if name in PROTECTED_NAMES or any(name.endswith(s) for s in PROTECTED_SUFFIXES):
            skipped["metadata-json"] += 1
            continue
        # unknown mtime is treated as young: a file we can't age is a file we don't touch
        if f.modified_at is None or _as_utc(f.modified_at) >= cutoff:
            skipped["young"] += 1
            continue
        if any(fnmatch.fnmatch(name, pattern) for pattern in exclude):
            skipped["excluded"] += 1
            continue
        candidates.append(f)
    return candidates, skipped


def clean_orphans(
    catalog: "Catalog",
    identifier: str,
    older_than: dt.timedelta = DEFAULT_OLDER_THAN,
    exclude: tuple[str, ...] = (),
    batch_size: int = DEFAULT_BATCH_SIZE,
    engine: Optional[str] = None,
    engine_catalog: Optional[str] = None,
    engine_config: Optional[dict] = None,
    execute: bool = False,
    force: bool = False,
) -> CleanOrphansPlan | CleanOrphansResult:
    try:
        table = catalog.load_table(identifier)
    except Exception as exc:
        raise TableNotFoundError(f"could not load table '{identifier}': {exc}") from exc

    manager = managed_by({str(k): str(v) for k, v in table.properties.items()}, table.location())
    if manager and not force:
        raise IceopsError(
            f"'{identifier}' looks managed by {manager} — its optimizer may be writing "
            f"files that look orphaned mid-operation. Use --force to override."
        )

    if engine is not None:
        from ..engines import validate_engine

        validate_engine(engine)
        return _clean_via_engine(
            table, identifier, older_than, engine, engine_catalog, engine_config, execute
        )

    plan = _build_plan(table, identifier, older_than, exclude)
    if not execute:
        return plan
    return execute_plan(table, plan, batch_size)


def _clean_via_engine(
    table: "Table",
    identifier: str,
    older_than: dt.timedelta,
    engine: str,
    engine_catalog: Optional[str],
    engine_config: Optional[dict],
    execute: bool,
) -> CleanOrphansPlan | CleanOrphansResult:
    """Delegate orphan removal to the engine's remove_orphan_files (Spark's/Trino's are
    battle-tested for object-store listing at scale). We deliberately DO NOT run our own
    storage listing here — that expensive step is exactly what we're delegating. The
    engine applies its own retention window and reachability."""
    from ..engines import get_engine

    engine_catalog = engine_catalog or catalog_name_from_table(table)
    action = Action(
        op="clean_orphans",
        table=identifier,
        params={
            "engine_catalog": engine_catalog,
            "table": identifier,
            "older_than_seconds": int(older_than.total_seconds()),
        },
    )
    plan = CleanOrphansPlan(
        identifier=identifier,
        location=table.location(),
        older_than_days=older_than.total_seconds() / 86400,
        engine=engine,
        action=action,
    )
    if engine_catalog:
        plan.engine_contract = delegated_contract(
            engine,
            action,
            owns=[
                "storage listing",
                "reachability check",
                "retention enforcement",
                "physical file deletion",
            ],
            iceops_owns=[
                "table load and managed-table refusal",
                "older-than parameter",
                "engine statement construction",
                "engine result relay",
            ],
            safety_notes=[
                f"{engine} chooses the exact orphan candidates.",
                "iceops does not enumerate candidate files in engine mode.",
                "engine clean-orphans applies the engine's own retention and reachability semantics.",
            ],
        )
    else:
        plan.warnings.append(
            "engine catalog is unknown; pass --engine-catalog so the engine can find the table"
        )
    if not execute:
        return plan
    if not engine_catalog:
        raise IceopsError(
            "engine clean-orphans needs --engine-catalog so the engine finds the table"
        )

    results = get_engine(engine, **(engine_config or {})).execute(
        Plan(table=identifier, actions=[action])
    )
    return CleanOrphansResult(plan=plan, action_results=results)


def _build_plan(
    table: "Table",
    identifier: str,
    older_than: dt.timedelta,
    exclude: tuple[str, ...],
) -> CleanOrphansPlan:
    location_prefix = normalize_path(table.location()).rstrip("/") + "/"
    cutoff = dt.datetime.now(dt.timezone.utc) - older_than

    reachable = _reachable(table)
    listed = _listing(table)
    candidates, skipped = filter_candidates(listed, reachable, location_prefix, cutoff, exclude)

    plan = CleanOrphansPlan(
        identifier=identifier,
        location=table.location(),
        metadata_location_at_plan=table.metadata_location,
        candidates=sorted(candidates, key=lambda f: f.path),
        total_bytes=sum(f.size_bytes for f in candidates),
        listed_count=len(listed),
        reachable_count=len(reachable),
        skipped={k: v for k, v in skipped.items() if v},
        older_than_days=older_than.total_seconds() / 86400,
    )
    if _looks_streaming(table):
        plan.warnings.append(
            "streaming writer detected: in-flight files are more likely — the "
            "--older-than threshold is your safety margin, do not lower it casually"
        )
    return plan


def _reachable(table: "Table") -> set[str]:
    """Every path Iceberg metadata knows about, from PyIceberg — never our parsing.

    Missing a source here deletes live data, so additions to table metadata in future
    PyIceberg versions must be reviewed against this list (pinned by tests).
    """
    paths: set[str] = set()
    if table.current_snapshot() is not None:
        all_files = table.inspect.all_files()
        paths |= {str(p) for p in all_files.column("file_path").to_pylist()}
        all_manifests = table.inspect.all_manifests()
        paths |= {str(p) for p in all_manifests.column("path").to_pylist()}
    paths |= {s.manifest_list for s in (table.metadata.snapshots or [])}
    paths |= {e.metadata_file for e in (table.metadata.metadata_log or [])}
    paths.add(table.metadata_location)
    for stats in getattr(table.metadata, "statistics", None) or []:
        path = getattr(stats, "statistics_path", None)
        if path:
            paths.add(str(path))
    for stats in getattr(table.metadata, "partition_statistics", None) or []:
        path = getattr(stats, "statistics_path", None)
        if path:
            paths.add(str(path))
    return {normalize_path(p) for p in paths}


def _listing(table: "Table") -> list[OrphanFile]:
    from pyarrow.fs import FileSelector, FileType

    parsed = urlparse(table.location())
    scheme = parsed.scheme or "file"
    fs = table.io.fs_by_scheme(scheme, parsed.netloc or None)  # type: ignore[attr-defined]
    base = parsed.path if scheme == "file" else parsed.netloc + parsed.path

    listed: list[OrphanFile] = []
    for info in fs.get_file_info(FileSelector(base, recursive=True)):
        if info.type != FileType.File:
            continue
        uri = info.path if scheme == "file" else f"{scheme}://{info.path}"
        listed.append(OrphanFile(path=uri, size_bytes=info.size or 0, modified_at=info.mtime))
    return listed


def execute_plan(
    table: "Table", plan: CleanOrphansPlan, batch_size: int = DEFAULT_BATCH_SIZE
) -> CleanOrphansResult:
    """Delete exactly the planned candidates, re-checking the table before every batch."""
    if not plan.actionable:
        return CleanOrphansResult(plan=plan, status="nothing-to-do")

    result = CleanOrphansResult(plan=plan)
    # anchor to the metadata version the PLAN saw: any commit after planning —
    # including one before execute even started — forces a reachability re-check
    metadata_location_seen = plan.metadata_location_at_plan or table.metadata_location
    reachable_now: Optional[set[str]] = None

    remaining = list(plan.candidates)
    for start in range(0, len(remaining), max(batch_size, 1)):
        batch = remaining[start : start + max(batch_size, 1)]

        table.refresh()
        if table.metadata_location != metadata_location_seen:
            # someone committed since we last looked: files may have become referenced
            reachable_now = _reachable(table)
            metadata_location_seen = table.metadata_location
        if reachable_now is not None:
            spared = [f for f in batch if normalize_path(f.path) in reachable_now]
            result.spared.extend(f.path for f in spared)
            batch = [f for f in batch if normalize_path(f.path) not in reachable_now]

        for f in batch:
            try:
                table.io.delete(f.path)
                result.deleted.append(f.path)
                result.freed_bytes += f.size_bytes
            except FileNotFoundError:
                result.missing.append(f.path)
            except OSError as exc:
                if "not found" in str(exc).lower() or "no such" in str(exc).lower():
                    result.missing.append(f.path)
                else:
                    raise IceopsError(
                        f"delete failed for '{f.path}': {exc} — "
                        f"{len(result.deleted)} files were already deleted; re-run to resume"
                    ) from exc
    return result


def _as_utc(moment: dt.datetime) -> dt.datetime:
    return moment if moment.tzinfo else moment.replace(tzinfo=dt.timezone.utc)


def _looks_streaming(table: "Table") -> bool:
    snapshots = table.metadata.snapshots or []
    if len(snapshots) < 2:
        return False
    timestamps = sorted(s.timestamp_ms for s in snapshots)
    span_days = max((timestamps[-1] - timestamps[0]) / 86_400_000, 1 / 24)
    return len(snapshots) / span_days > 24
