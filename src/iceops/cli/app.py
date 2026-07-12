"""iceops CLI — a thin Typer skin over the operator library.

EVERY COMMAND FOLLOWS THE SAME FLOW
    1. resolve which catalog owns the table (`_resolve`): --catalog flag, or a
       'catalog.ns.table' prefix matching a profile, or the single configured profile
    2. parse human inputs (durations '7d', sizes '8MB') — fail fast with exit 2
    3. call the operator (all logic lives in iceops.operators; none here)
    4. render the returned model via render.py, or dump it with --json
    5. exit code: 0 = healthy / done / nothing to do,
                  1 = findings exist or work was planned but this was a dry run,
                  2 = error (CI scripts key on these)

Fix commands share one convention enforced by the operators themselves: dry-run unless
--yes, refuse externally-managed tables unless --force. Not-yet-built operators are
registered as stubs that print the roadmap instead of "no such command".
"""

from __future__ import annotations

from typing import Optional

import typer

from .. import __version__, operators
from ..catalog import connect
from ..config import default_catalog_name, load_engine_config, load_profiles
from ..errors import IceopsError
from ..models import (
    CleanOrphansResult,
    CompactResult,
    ExpireResult,
    RewriteManifestsResult,
    Severity,
    TuneResult,
    parse_duration,
    parse_size,
)
from . import render

EXIT_OK = 0
EXIT_FINDINGS = 1
EXIT_ERROR = 2

app = typer.Typer(
    name="iceops",
    help="Doctor, janitor, and autopilot for Apache Iceberg tables.",
    no_args_is_help=True,
    add_completion=True,
)

CatalogOpt = typer.Option(None, "--catalog", "-c", help="catalog profile name")
JsonOpt = typer.Option(False, "--json", help="emit machine-readable JSON instead of tables")
EngineOpt = typer.Option(
    None, "--engine", help="delegate execution to an engine (spark|trino); omit for native"
)
EngineCatalogOpt = typer.Option(
    None, "--engine-catalog", help="catalog name the engine uses (defaults to the profile)"
)


def _fail(message: str) -> "typer.Exit":
    render.error_console.print(f"[red]error:[/red] {message}")
    return typer.Exit(EXIT_ERROR)


def _resolve(identifier: str, catalog_name: Optional[str]) -> tuple[str, str]:
    """Work out (catalog profile, table identifier).

    Accepts --catalog plus 'ns.table', or a fully qualified 'catalog.ns.table' whose
    first segment matches a configured profile. With a single configured profile,
    --catalog can be omitted entirely.
    """
    if catalog_name:
        return catalog_name, identifier
    parts = identifier.split(".")
    if len(parts) >= 3 and parts[0] in load_profiles():
        return parts[0], ".".join(parts[1:])
    default = default_catalog_name()
    if default:
        return default, identifier
    raise _fail(
        f"cannot tell which catalog owns '{identifier}' — pass --catalog or use "
        f"'<catalog>.<namespace>.<table>'"
    )


def _resolve_catalog(catalog_name: Optional[str]) -> str:
    if catalog_name:
        return catalog_name
    default = default_catalog_name()
    if default:
        return default
    raise _fail("pass --catalog (multiple or zero profiles are configured)")


def _has_actionable_findings(*severities: Severity) -> bool:
    return any(s in (Severity.WARN, Severity.CRITICAL) for s in severities)


@app.command()
def scan(
    catalog: Optional[str] = CatalogOpt,
    pattern: str = typer.Option("*", "--pattern", "-p", help="glob over 'ns.table' names"),
    json_output: bool = JsonOpt,
) -> None:
    """Fleet-wide health report: one status row per table."""
    name = _resolve_catalog(catalog)
    try:
        report = operators.scan(connect(name), name, pattern)
    except IceopsError as exc:
        raise _fail(str(exc))
    if json_output:
        print(report.model_dump_json(indent=2))
    else:
        render.render_fleet(report)
    findings = [f.severity for r in report.reports for f in r.findings]
    if report.errors or _has_actionable_findings(*findings):
        raise typer.Exit(EXIT_FINDINGS)


@app.command()
def doctor(
    table: str = typer.Argument(..., help="table as 'ns.table' (or 'catalog.ns.table')"),
    catalog: Optional[str] = CatalogOpt,
    json_output: bool = JsonOpt,
) -> None:
    """Deep health report for one table."""
    name, identifier = _resolve(table, catalog)
    try:
        report = operators.doctor(connect(name), identifier)
    except IceopsError as exc:
        raise _fail(str(exc))
    if json_output:
        print(report.model_dump_json(indent=2))
    else:
        render.render_health(report)
    if _has_actionable_findings(*(f.severity for f in report.findings)):
        raise typer.Exit(EXIT_FINDINGS)


@app.command()
def cost(
    table: str = typer.Argument(..., help="table as 'ns.table' (or 'catalog.ns.table')"),
    catalog: Optional[str] = CatalogOpt,
    dollars_per_gb_month: float = typer.Option(
        0.023, "--dollars-per-gb-month", help="storage price used for the estimate"
    ),
    json_output: bool = JsonOpt,
) -> None:
    """Estimate wasted storage cost from stale snapshots and orphaned files."""
    name, identifier = _resolve(table, catalog)
    try:
        report = operators.cost(connect(name), identifier, dollars_per_gb_month)
    except IceopsError as exc:
        raise _fail(str(exc))
    if json_output:
        print(report.model_dump_json(indent=2))
    else:
        render.render_cost(report)


@app.command()
def expire(
    table: str = typer.Argument(..., help="table as 'ns.table' (or 'catalog.ns.table')"),
    catalog: Optional[str] = CatalogOpt,
    retain_last: int = typer.Option(10, "--retain-last", help="always keep the newest N snapshots"),
    older_than: str = typer.Option(
        "7d", "--older-than", help="only expire snapshots older than this (e.g. 12h, 7d, 2w)"
    ),
    engine: Optional[str] = EngineOpt,
    engine_catalog: Optional[str] = EngineCatalogOpt,
    yes: bool = typer.Option(False, "--yes", help="execute; without this it's a dry run"),
    force: bool = typer.Option(
        False, "--force", help="proceed even if another optimizer manages the table"
    ),
    json_output: bool = JsonOpt,
) -> None:
    """Expire old snapshots (dry-run by default).

    Native (default) keeps metadata-only semantics. With --engine spark|trino the engine's
    expire_snapshots runs instead (and deletes files too). A snapshot is expired only if it
    is BOTH beyond --retain-last AND older than --older-than; refs are never expired.
    """
    name, identifier = _resolve(table, catalog)
    try:
        cutoff = parse_duration(older_than)
    except ValueError as exc:
        raise _fail(str(exc))
    try:
        outcome = operators.expire(
            connect(name),
            identifier,
            retain_last=retain_last,
            older_than=cutoff,
            engine=engine,
            engine_catalog=engine_catalog or (name if engine else None),
            engine_config=load_engine_config(engine) if engine else None,
            execute=yes,
            force=force,
        )
    except IceopsError as exc:
        raise _fail(str(exc))

    plan = outcome.plan if isinstance(outcome, ExpireResult) else outcome
    result = outcome if isinstance(outcome, ExpireResult) else None
    did_work = bool(result and (result.expired_snapshot_ids or result.action_results))
    if json_output:
        print(outcome.model_dump_json(indent=2))
    else:
        render.render_expire_plan(plan, executed=result if did_work else None)
    if not yes and plan.actionable:
        raise typer.Exit(EXIT_FINDINGS)  # work is planned but nothing was done


@app.command(name="rewrite-manifests")
def rewrite_manifests_cmd(
    table: str = typer.Argument(..., help="table as 'ns.table' (or 'catalog.ns.table')"),
    catalog: Optional[str] = CatalogOpt,
    target_manifest_size: str = typer.Option(
        "8MB", "--target-manifest-size", help="bin-pack manifests to roughly this size"
    ),
    engine: Optional[str] = EngineOpt,
    engine_catalog: Optional[str] = EngineCatalogOpt,
    yes: bool = typer.Option(False, "--yes", help="execute; without this it's a dry run"),
    force: bool = typer.Option(
        False, "--force", help="proceed even if another optimizer manages the table"
    ),
    json_output: bool = JsonOpt,
) -> None:
    """Consolidate fragmented manifests (dry-run by default).

    Native (default) is metadata-only. With --engine spark|trino the engine's
    rewrite_manifests / optimize_manifests runs instead.
    """
    name, identifier = _resolve(table, catalog)
    try:
        target = parse_size(target_manifest_size)
    except ValueError as exc:
        raise _fail(str(exc))
    try:
        outcome = operators.rewrite_manifests(
            connect(name),
            identifier,
            target_manifest_size=target,
            engine=engine,
            engine_catalog=engine_catalog or (name if engine else None),
            engine_config=load_engine_config(engine) if engine else None,
            execute=yes,
            force=force,
        )
    except IceopsError as exc:
        raise _fail(str(exc))

    plan = outcome.plan if isinstance(outcome, RewriteManifestsResult) else outcome
    result = outcome if isinstance(outcome, RewriteManifestsResult) else None
    if json_output:
        print(outcome.model_dump_json(indent=2))
    else:
        render.render_rewrite_manifests_plan(
            plan, executed=result if result and result.status == "rewritten" else None
        )
    if not yes and plan.actionable:
        raise typer.Exit(EXIT_FINDINGS)  # work is planned but nothing was done


@app.command(name="clean-orphans")
def clean_orphans_cmd(
    table: str = typer.Argument(..., help="table as 'ns.table' (or 'catalog.ns.table')"),
    catalog: Optional[str] = CatalogOpt,
    older_than: str = typer.Option(
        "3d", "--older-than", help="never delete files younger than this (safety margin)"
    ),
    exclude: list[str] = typer.Option(
        [], "--exclude", help="filename glob to protect (repeatable), e.g. '_SUCCESS'"
    ),
    batch_size: int = typer.Option(100, "--batch-size", help="deletes per re-check batch"),
    engine: Optional[str] = EngineOpt,
    engine_catalog: Optional[str] = EngineCatalogOpt,
    yes: bool = typer.Option(False, "--yes", help="execute; without this it's a dry run"),
    force: bool = typer.Option(
        False, "--force", help="proceed even if another optimizer manages the table"
    ),
    json_output: bool = JsonOpt,
) -> None:
    """Delete files no snapshot references (dry-run by default).

    The only iceops command that deletes physical files. Native applies iceops' own safety
    funnel; with --engine spark|trino the engine's remove_orphan_files runs instead (its
    own retention + reachability — battle-tested for object stores at scale).
    """
    name, identifier = _resolve(table, catalog)
    try:
        cutoff = parse_duration(older_than)
    except ValueError as exc:
        raise _fail(str(exc))
    try:
        outcome = operators.clean_orphans(
            connect(name),
            identifier,
            older_than=cutoff,
            exclude=tuple(exclude),
            batch_size=batch_size,
            engine=engine,
            engine_catalog=engine_catalog or (name if engine else None),
            engine_config=load_engine_config(engine) if engine else None,
            execute=yes,
            force=force,
        )
    except IceopsError as exc:
        raise _fail(str(exc))

    plan = outcome.plan if isinstance(outcome, CleanOrphansResult) else outcome
    result = outcome if isinstance(outcome, CleanOrphansResult) else None
    if json_output:
        print(outcome.model_dump_json(indent=2))
    else:
        render.render_clean_orphans_plan(
            plan, executed=result if result and result.status == "cleaned" else None
        )
    if not yes and plan.actionable:
        raise typer.Exit(EXIT_FINDINGS)  # work is planned but nothing was done


@app.command()
def compact(
    table: str = typer.Argument(..., help="table as 'ns.table' (or 'catalog.ns.table')"),
    catalog: Optional[str] = CatalogOpt,
    engine: str = typer.Option(
        ..., "--engine", help="execution engine: spark or trino (native not yet available)"
    ),
    engine_catalog: Optional[str] = typer.Option(
        None,
        "--engine-catalog",
        help="catalog name visible to the engine (defaults to the iceops catalog profile)",
    ),
    target_file_size: str = typer.Option(
        "512MB", "--target-file-size", help="engine target file size for compacted files"
    ),
    yes: bool = typer.Option(False, "--yes", help="execute; without this it's a dry run"),
    force: bool = typer.Option(
        False, "--force", help="proceed even if another optimizer manages the table"
    ),
    json_output: bool = JsonOpt,
) -> None:
    """Compact small data files through Spark or Trino (dry-run by default)."""
    name, identifier = _resolve(table, catalog)
    try:
        target = parse_size(target_file_size)
    except ValueError as exc:
        raise _fail(str(exc))
    try:
        outcome = operators.compact(
            connect(name),
            identifier,
            target_file_size=target,
            engine=engine,
            engine_catalog=engine_catalog or name,
            engine_config=load_engine_config(engine),
            execute=yes,
            force=force,
        )
    except IceopsError as exc:
        raise _fail(str(exc))

    plan = outcome.plan if isinstance(outcome, CompactResult) else outcome
    result = outcome if isinstance(outcome, CompactResult) else None
    if json_output:
        print(outcome.model_dump_json(indent=2))
    else:
        render.render_compact_plan(
            plan, executed=result if result and result.action_results else None
        )
    if not yes and plan.actionable:
        raise typer.Exit(EXIT_FINDINGS)  # work is planned but nothing was done


@app.command()
def tune(
    table: str = typer.Argument(..., help="table as 'ns.table' (or 'catalog.ns.table')"),
    catalog: Optional[str] = CatalogOpt,
    engine: Optional[str] = typer.Option(
        None, "--engine", help="engine for the compact step (spark|trino); omit to skip compact"
    ),
    engine_catalog: Optional[str] = typer.Option(None, "--engine-catalog"),
    older_than: str = typer.Option(
        "7d", "--older-than", help="expire snapshots older than this (safe default 7d)"
    ),
    yes: bool = typer.Option(False, "--yes", help="execute; without this it's a dry run"),
    force: bool = typer.Option(
        False, "--force", help="proceed even if another optimizer manages the table"
    ),
    json_output: bool = JsonOpt,
) -> None:
    """Run all maintenance in the right order (dry-run by default).

    Sequence: compact → rewrite-manifests → expire → clean-orphans. compact runs only
    with --engine; the other three are native. A single run won't reclaim space it just
    orphaned — clean-orphans respects its age threshold.
    """
    name, identifier = _resolve(table, catalog)
    try:
        cutoff = parse_duration(older_than)
    except ValueError as exc:
        raise _fail(str(exc))
    try:
        outcome = operators.tune(
            connect(name),
            identifier,
            engine=engine,
            engine_catalog=engine_catalog or (name if engine else None),
            engine_config=load_engine_config(engine) if engine else None,
            older_than_expire=cutoff,
            execute=yes,
            force=force,
        )
    except IceopsError as exc:
        raise _fail(str(exc))

    plan = outcome.plan if isinstance(outcome, TuneResult) else outcome
    result = outcome if isinstance(outcome, TuneResult) else None
    if json_output:
        print(outcome.model_dump_json(indent=2))
    else:
        render.render_tune(plan, executed=result)
    if result is not None and result.status == "halted":
        raise typer.Exit(EXIT_ERROR)
    if not yes and plan.actionable:
        raise typer.Exit(EXIT_FINDINGS)


@app.command()
def catalogs() -> None:
    """List configured catalog profiles."""
    render.render_catalogs(load_profiles())


@app.command()
def version() -> None:
    """Print the iceops version."""
    print(__version__)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
