"""Rich rendering for operator results. Operators never print; this module does."""

from __future__ import annotations

from typing import Any

from rich.console import Console
from rich.table import Table as RichTable
from rich.text import Text

from ..models import (
    CostReport,
    Finding,
    FleetReport,
    HealthReport,
    Severity,
    Status,
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
