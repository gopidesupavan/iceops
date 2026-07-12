"""Rich rendering for operator results. Operators never print; this module does."""

from __future__ import annotations

from typing import Any

from rich.console import Console
from rich.table import Table as RichTable
from rich.text import Text

from ..models import (
    ApplyPlan,
    ApplyResult,
    CleanOrphansPlan,
    CleanOrphansResult,
    CompactPlan,
    CompactResult,
    CostReport,
    EnginePlanContract,
    ExpirePlan,
    ExpireResult,
    Finding,
    FleetReport,
    HealthReport,
    RewriteManifestsPlan,
    RewriteManifestsResult,
    Severity,
    Status,
    TunePlan,
    TuneResult,
    VerificationResult,
    human_bytes,
)

console = Console()
error_console = Console(stderr=True)

STATUS_STYLES = {
    Status.HEALTHY: "green",
    Status.WARN: "yellow",
    Status.CRITICAL: "bold red",
}

_STATUS_ORDER = [Status.CRITICAL, Status.WARN, Status.HEALTHY]

SEVERITY_STYLES = {
    Severity.INFO: "cyan",
    Severity.WARN: "yellow",
    Severity.CRITICAL: "bold red",
}

BAR_WIDTH = 30


def status_text(status: Status) -> Text:
    return Text(status.value, style=STATUS_STYLES[status])


def render_fleet(report: FleetReport) -> None:
    table = RichTable(title=f"catalog '{report.catalog}' — {len(report.reports)} tables")
    table.add_column("table")
    table.add_column("status", justify="center")
    table.add_column("files", justify="right")
    table.add_column("small", justify="right")
    table.add_column("snapshots", justify="right")
    table.add_column("size", justify="right")
    table.add_column("top issue")

    for health in sorted(
        report.reports, key=lambda r: (_STATUS_ORDER.index(r.status), r.identifier)
    ):
        m = health.metrics
        top = _top_finding(health.findings)
        table.add_row(
            health.identifier,
            status_text(health.status),
            str(m.data_file_count),
            f"{m.small_file_ratio:.0%}" if m.data_file_count else "-",
            str(m.snapshot_count),
            human_bytes(m.total_data_bytes),
            top,
        )
    console.print(table)

    counts = report.status_counts
    if counts:
        summary = "  ".join(
            f"{s.value}: {counts[s.value]}" for s in _STATUS_ORDER if s.value in counts
        )
        console.print(summary)
    for err in report.errors:
        error_console.print(f"[red]error[/red] {err.identifier}: {err.error}")


def _top_finding(findings: list[Finding]) -> Text:
    if not findings:
        return Text("healthy", style="green")
    worst = sorted(
        findings,
        key=lambda f: [Severity.CRITICAL, Severity.WARN, Severity.INFO].index(f.severity),
    )[0]
    return Text(f"{worst.check_id}", style=SEVERITY_STYLES[worst.severity])


def render_health(report: HealthReport) -> None:
    m = report.metrics
    header = Text()
    header.append(report.identifier, style="bold")
    header.append("  ")
    header.append_text(status_text(report.status))
    console.print(header)

    console.print(
        f"  {m.data_file_count} data files ({human_bytes(m.total_data_bytes)}, "
        f"avg {human_bytes(m.avg_file_bytes)})  ·  {m.delete_file_count} delete files  ·  "
        f"{m.snapshot_count} snapshots  ·  {m.manifest_count} manifests  ·  "
        f"{m.partition_count} partitions"
    )
    if report.managed_by:
        console.print(
            f"  [cyan]managed by {report.managed_by}[/cyan] — fix operators will skip "
            f"this table by default"
        )
    if report.streaming_writer:
        console.print("  [cyan]streaming writer detected[/cyan] (frequent commits)")

    if any(m.file_size_histogram.values()):
        console.print("\n  file sizes")
        peak = max(m.file_size_histogram.values())
        for label, count in m.file_size_histogram.items():
            bar = "█" * max(1, round(count / peak * BAR_WIDTH)) if count else ""
            console.print(f"  {label:>9}  {bar} {count if count else ''}")

    if report.findings:
        console.print("\n  findings")
        for finding in report.findings:
            style = SEVERITY_STYLES[finding.severity]
            console.print(f"  [{style}]● {finding.severity.value}[/{style}] {finding.message}")
            console.print(f"    [dim]{finding.recommendation}[/dim]")
    else:
        console.print("\n  [green]no findings — table looks healthy[/green]")


def render_cost(report: CostReport) -> None:
    table = RichTable(title=f"storage cost — {report.identifier}")
    table.add_column("category")
    table.add_column("bytes", justify="right")
    table.add_column("meaning")
    table.add_row("live", human_bytes(report.live_bytes), "referenced by current snapshot")
    table.add_row(
        "stale",
        human_bytes(report.stale_bytes),
        "only reachable via old snapshots — freed by expire",
    )
    table.add_row(
        "orphan (est.)",
        human_bytes(report.orphan_bytes_estimate),
        "referenced by nothing — freed by clean-orphans",
    )
    console.print(table)
    if report.monthly_waste_dollars is not None:
        console.print(
            f"estimated waste: [bold]${report.monthly_waste_dollars}/month[/bold] "
            f"at ${report.dollars_per_gb_month}/GB-month"
        )
    for note in report.notes:
        console.print(f"[dim]note: {note}[/dim]")


def _render_contract(contract: EnginePlanContract | None) -> None:
    if contract is None:
        return
    console.print(f"plan kind: {contract.plan_kind.value}")
    console.print("\nstatement:")
    console.print(f"  {contract.statement}")
    if contract.safety_notes:
        console.print("\nsafety:")
        for note in contract.safety_notes:
            console.print(f"  - {note}")
    if contract.verification_notes:
        console.print("\nverification:")
        for note in contract.verification_notes:
            console.print(f"  - {note}")


def _render_verifications(verifications: list[VerificationResult]) -> None:
    if not verifications:
        return
    console.print("\nverification:")
    for check in verifications:
        label = check.check.replace("_", " ")
        text = f"  {label}: {check.status.value}"
        if check.before is not None or check.after is not None:
            text += f" ({check.before if check.before is not None else '?'} -> "
            text += f"{check.after if check.after is not None else '?'})"
        if check.note:
            text += f" - {check.note}"
        console.print(text)


def _render_engine_op(label: str, plan: object, executed: object, show_footer: bool) -> None:
    """Uniform view for an engine-delegated fix op: the engine picks the work and applies
    its own safety; iceops shows the parameters and relays the outcome."""
    identifier = getattr(plan, "identifier")
    engine = getattr(plan, "engine")
    console.print(f"plan: {label} {identifier} via {engine}")
    _render_contract(getattr(plan, "engine_contract", None))
    if executed is None:
        for warning in getattr(plan, "warnings", []) or []:
            console.print(f"[yellow]warning: {warning}[/yellow]")
        if show_footer:
            console.print("[bold]DRY RUN — nothing changed. Add --yes to execute.[/bold]")
        return
    console.print(f"[green]{label} submitted to {engine}[/green]")
    for action_result in getattr(executed, "action_results", []) or []:
        for key, value in sorted(action_result.details.items()):
            if key == "statement":
                continue
            console.print(f"  {key}: {value}")


def render_expire_plan(
    plan: ExpirePlan, executed: ExpireResult | None = None, show_footer: bool = True
) -> None:
    if plan.engine is not None:
        _render_engine_op("expire", plan, executed, show_footer)
        return
    if not plan.candidates:
        console.print(
            f"{plan.identifier}: nothing to expire "
            f"({plan.snapshot_count} snapshots, retain-last {plan.retain_last}, "
            f"cutoff {plan.cutoff:%Y-%m-%d %H:%M} UTC)"
        )
        return

    first = plan.candidates[0].committed_at
    last = plan.candidates[-1].committed_at
    console.print(
        f"plan: expire {len(plan.candidates)} of {plan.snapshot_count} snapshots "
        f"({first:%Y-%m-%d %H:%M} … {last:%Y-%m-%d %H:%M} UTC)"
    )
    for candidate in plan.candidates:
        console.print(
            f"  snapshot {candidate.snapshot_id}  "
            f"{candidate.committed_at:%Y-%m-%d %H:%M:%S}  {candidate.operation or '?'}"
        )
    console.print(
        f"after expiry: {human_bytes(plan.unreferenced_manifest_bytes)} of manifests + "
        f"{human_bytes(plan.unreferenced_data_bytes)} of data files become unreferenced"
    )
    console.print(
        f"[dim]expiration removes metadata only — reclaim unreferenced files with: "
        f"iceops clean-orphans {plan.identifier}[/dim]"
    )
    for warning in plan.warnings:
        console.print(f"[yellow]warning: {warning}[/yellow]")

    if executed is None:
        if show_footer:
            console.print("[bold]DRY RUN — nothing changed. Add --yes to execute.[/bold]")
    else:
        console.print(
            f"[green]expired {len(executed.expired_snapshot_ids)} snapshots — "
            f"{executed.snapshot_count_after} remain[/green]"
        )


def render_rewrite_manifests_plan(
    plan: RewriteManifestsPlan,
    executed: RewriteManifestsResult | None = None,
    show_footer: bool = True,
) -> None:
    if plan.engine is not None:
        _render_engine_op("rewrite-manifests", plan, executed, show_footer)
        return
    if not plan.actionable:
        console.print(
            f"{plan.identifier}: nothing to rewrite ({plan.manifest_count} manifests, "
            f"already at or below the ~{human_bytes(plan.target_manifest_size_bytes)} target)"
        )
        return

    console.print(
        f"plan: consolidate {plan.manifest_count} manifests "
        f"({human_bytes(plan.manifest_bytes)}, ~{plan.files_per_manifest} data files each) "
        f"into ~{plan.estimated_after}"
    )
    console.print(
        "[dim]metadata only — no data files are read or written; one new snapshot is "
        "created and the previous one remains for rollback[/dim]"
    )
    for warning in plan.warnings:
        console.print(f"[yellow]warning: {warning}[/yellow]")

    if executed is None:
        if show_footer:
            console.print("[bold]DRY RUN — nothing changed. Add --yes to execute.[/bold]")
    else:
        console.print(
            f"[green]rewrote manifests: {executed.manifests_before} → "
            f"{executed.manifests_after} (snapshot {executed.new_snapshot_id})[/green]"
        )


def render_clean_orphans_plan(
    plan: CleanOrphansPlan,
    executed: CleanOrphansResult | None = None,
    show_footer: bool = True,
) -> None:
    if plan.engine is not None:
        _render_engine_op("clean-orphans", plan, executed, show_footer)
        return
    skipped_note = "  ".join(f"{k}: {v}" for k, v in plan.skipped.items()) if plan.skipped else ""
    if not plan.actionable:
        console.print(
            f"{plan.identifier}: nothing to clean — {plan.listed_count} files listed, "
            f"{plan.reachable_count} reachable, 0 deletable orphans"
            + (f" (skipped — {skipped_note})" if skipped_note else "")
        )
        return

    console.print(
        f"plan: delete {len(plan.candidates)} orphaned files "
        f"({human_bytes(plan.total_bytes)}) under {plan.location}"
    )
    now = _utcnow()
    for f in plan.candidates:
        rel = f.path.split(plan.location.rstrip("/") + "/")[-1] if plan.location else f.path
        age = f"{(now - f.modified_at).days}d" if f.modified_at else "?"
        console.print(f"  {rel}  ({human_bytes(f.size_bytes)}, {age} old)")
    console.print(
        f"listed {plan.listed_count} files · {plan.reachable_count} reachable"
        + (f" · skipped — {skipped_note}" if skipped_note else "")
    )
    console.print(
        "[dim]*.metadata.json files are never deleted; files younger than "
        f"{plan.older_than_days:g}d are never deleted[/dim]"
    )
    for warning in plan.warnings:
        console.print(f"[yellow]warning: {warning}[/yellow]")

    if executed is None:
        if show_footer:
            console.print("[bold]DRY RUN — nothing changed. Add --yes to execute.[/bold]")
    else:
        console.print(
            f"[green]deleted {len(executed.deleted)} files, freed "
            f"{human_bytes(executed.freed_bytes)}[/green]"
        )
        if executed.spared:
            console.print(
                f"[yellow]spared {len(executed.spared)} files that became referenced "
                f"during the run (a writer committed)[/yellow]"
            )
        if executed.missing:
            console.print(f"[dim]{len(executed.missing)} were already gone[/dim]")


def render_compact_plan(
    plan: CompactPlan, executed: CompactResult | None = None, show_footer: bool = True
) -> None:
    if not plan.actionable:
        console.print(
            f"{plan.identifier}: nothing to compact "
            f"({plan.data_file_count} data files, {plan.delete_file_count} delete files, "
            f"target {human_bytes(plan.target_file_size_bytes)})"
        )
        return

    console.print(
        f"plan: compact {plan.small_file_count} small files in {plan.identifier} "
        f"via {plan.engine} (target {human_bytes(plan.target_file_size_bytes)})"
    )
    if plan.engine_contract is not None:
        console.print(f"plan kind: {plan.engine_contract.plan_kind.value}")
    if plan.delete_file_count:
        console.print(
            f"  {plan.delete_file_count} delete files present — the engine owns "
            "delete-aware rewrite semantics"
        )
    console.print(
        f"  engine catalog: {plan.engine_catalog or '?'} · snapshot: "
        f"{plan.current_snapshot_id or '?'}"
    )
    if plan.engine_contract is not None:
        console.print("\nstatement:")
        console.print(f"  {plan.engine_contract.statement}")
    console.print("\nestimated work:")
    console.print(f"  data files: {plan.data_file_count}")
    console.print(f"  small files: {plan.small_file_count}")
    console.print(f"  delete files: {plan.delete_file_count}")
    console.print(f"  data bytes: {human_bytes(plan.total_data_bytes)}")
    if plan.engine_contract is not None and plan.engine_contract.safety_notes:
        console.print("\nsafety:")
        for note in plan.engine_contract.safety_notes:
            console.print(f"  - {note}")
    if plan.engine_contract is not None and plan.engine_contract.verification_notes:
        console.print("\nverification:")
        for note in plan.engine_contract.verification_notes:
            console.print(f"  - {note}")
    for warning in plan.warnings:
        console.print(f"[yellow]warning: {warning}[/yellow]")

    if executed is None:
        if show_footer:
            console.print("[bold]DRY RUN — nothing changed. Add --yes to execute.[/bold]")
        return

    console.print(
        f"[green]submitted compact via {plan.engine}: "
        f"{executed.data_files_before} → "
        f"{executed.data_files_after if executed.data_files_after is not None else '?'} "
        "data files[/green]"
    )
    console.print("\neffect:")
    console.print(
        f"  data files: {executed.data_files_before} -> "
        f"{executed.data_files_after if executed.data_files_after is not None else '?'}"
    )
    console.print(
        f"  delete files: {executed.delete_files_before} -> "
        f"{executed.delete_files_after if executed.delete_files_after is not None else '?'}"
    )
    console.print(
        f"  snapshot: {executed.snapshot_before or '?'} -> {executed.snapshot_after or '?'}"
    )
    for result in executed.action_results:
        console.print("\nengine result:")
        for key, value in sorted(result.details.items()):
            if key == "statement":
                continue
            console.print(f"  {key}: {value}")
    _render_verifications(executed.verifications)


def _utcnow():
    import datetime as _dt

    return _dt.datetime.now(_dt.timezone.utc)


_TUNE_STEP_LABELS = {
    "compact": "compact",
    "rewrite_manifests": "rewrite-manifests",
    "expire": "expire",
    "clean_orphans": "clean-orphans",
}


def render_tune(plan: TunePlan, executed: TuneResult | None = None) -> None:
    order = ["compact", "rewrite_manifests", "expire", "clean_orphans"]
    console.print(
        f"tune {plan.identifier} — maintenance in order: {' → '.join(_TUNE_STEP_LABELS[s] for s in order)}"
    )

    for step in order:
        label = _TUNE_STEP_LABELS[step]
        console.print(f"\n[bold]▸ {label}[/bold]")
        if step in plan.skipped:
            console.print(f"  [dim]skipped — {plan.skipped[step]}[/dim]")
            continue
        sub_plan = getattr(plan, step)
        if sub_plan is None:
            console.print("  [dim]skipped[/dim]")
            continue
        sub_result = getattr(executed, step) if executed else None
        _render_sub(step, sub_plan, sub_result)

    console.print(
        "\n[dim]note: each step is planned against the current table; earlier steps change "
        "what later ones do. clean-orphans only deletes files past its age threshold, so a "
        "single run won't reclaim what it just orphaned.[/dim]"
    )

    if executed is None:
        if plan.actionable:
            console.print("[bold]DRY RUN — nothing changed. Add --yes to execute.[/bold]")
        else:
            console.print("[green]nothing to tune.[/green]")
    elif executed.status == "halted":
        halted = _TUNE_STEP_LABELS.get(executed.halted_at or "", executed.halted_at or "?")
        console.print(f"[red]halted at {halted} — later steps did not run.[/red]")
    else:
        done = ", ".join(_TUNE_STEP_LABELS[s] for s in executed.executed) or "nothing"
        console.print(f"[green]tuned: ran {done}.[/green]")


def _render_sub(step: str, sub_plan: object, sub_result: object) -> None:
    # suppress each step's own DRY RUN footer — tune prints one combined footer
    if step == "compact":
        render_compact_plan(sub_plan, executed=sub_result, show_footer=False)  # type: ignore[arg-type]
    elif step == "rewrite_manifests":
        render_rewrite_manifests_plan(sub_plan, executed=sub_result, show_footer=False)  # type: ignore[arg-type]
    elif step == "expire":
        render_expire_plan(sub_plan, executed=sub_result, show_footer=False)  # type: ignore[arg-type]
    elif step == "clean_orphans":
        render_clean_orphans_plan(sub_plan, executed=sub_result, show_footer=False)  # type: ignore[arg-type]


def render_apply(plan: ApplyPlan, executed: ApplyResult | None = None) -> None:
    results = {r.identifier: r for r in executed.results} if executed else {}
    if not plan.tables and not plan.skipped:
        console.print(f"no tables in scope of the policy for catalog '{plan.catalog}'")
        return

    console.print(f"policy over catalog '{plan.catalog}' — {len(plan.tables)} tables in scope")
    for table in plan.tables:
        eng = f" [{table.engine}]" if table.engine else ""
        console.print(f"\n[bold]{table.identifier}[/bold]{eng}")
        tr = results.get(table.identifier)
        for d in table.decisions:
            label = _TUNE_STEP_LABELS.get(d.op, d.op)
            if d.will_run:
                done = tr and d.op in tr.executed
                mark = "[green]✓ ran[/green]" if done else "[bold]will run[/bold]"
                console.print(f"  {mark} {label}  [dim]({d.reason})[/dim]")
            else:
                console.print(f"  [dim]skip {label} ({d.reason})[/dim]")
        if tr and tr.halted_at:
            console.print(
                f"  [red]halted at {_TUNE_STEP_LABELS.get(tr.halted_at, tr.halted_at)}: "
                f"{tr.error}[/red]"
            )
    for identifier, reason in plan.skipped.items():
        console.print(f"[dim]· {identifier}: {reason}[/dim]")

    if executed is None:
        if plan.actionable:
            console.print("\n[bold]DRY RUN — nothing changed. Add --yes to execute.[/bold]")
        else:
            console.print("\n[green]nothing to apply.[/green]")
    elif isinstance(executed, ApplyResult):
        ran = sum(len(r.executed) for r in executed.results)
        console.print(
            f"\n[green]applied: {ran} operations across {len(plan.tables)} tables[/green]"
        )


def render_catalogs(profiles: dict[str, dict[str, Any]]) -> None:
    if not profiles:
        console.print(
            "no catalog profiles found — create .iceops.toml or ~/.iceops/config.toml "
            "with a [catalogs.<name>] section (PyIceberg's own config also works)"
        )
        return
    table = RichTable(title="catalog profiles")
    table.add_column("name")
    table.add_column("type")
    table.add_column("uri")
    for name, props in profiles.items():
        table.add_row(name, str(props.get("type", "?")), str(props.get("uri", "")))
    console.print(table)
