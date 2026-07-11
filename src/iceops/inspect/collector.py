"""Turn PyIceberg table metadata into a TableMetrics snapshot.

Everything here is read-only and defensive: inspect endpoints vary across PyIceberg
versions and catalog types, so each section degrades to "unknown" instead of failing the
whole diagnosis.
"""

from __future__ import annotations

import datetime as dt
import statistics
from pathlib import Path
from typing import TYPE_CHECKING, Optional
from urllib.parse import urlparse

from ..models import HISTOGRAM_BUCKETS, SMALL_FILE_BYTES, TableMetrics

if TYPE_CHECKING:
    from pyiceberg.table import Table

DATA_FILE_CONTENT = 0  # iceberg spec: 0=data, 1=position deletes, 2=equality deletes


def collect(table: "Table", identifier: str) -> TableMetrics:
    metrics = TableMetrics(
        identifier=identifier,
        location=table.location(),
        format_version=table.metadata.format_version,
        properties={str(k): str(v) for k, v in table.properties.items()},
    )
    _collect_files(table, metrics)
    _collect_snapshots(table, metrics)
    _collect_manifests(table, metrics)
    _collect_partitions(table, metrics)
    _collect_storage(table, metrics)
    return metrics


def _collect_files(table: "Table", metrics: TableMetrics) -> None:
    if table.current_snapshot() is None:
        return
    files = table.inspect.files()
    contents = files.column("content").to_pylist() if "content" in files.column_names else []
    sizes = files.column("file_size_in_bytes").to_pylist()

    data_sizes: list[int] = []
    delete_count = 0
    delete_bytes = 0
    for i, size in enumerate(sizes):
        content = contents[i] if i < len(contents) else DATA_FILE_CONTENT
        if content == DATA_FILE_CONTENT:
            data_sizes.append(int(size))
        else:
            delete_count += 1
            delete_bytes += int(size)

    metrics.data_file_count = len(data_sizes)
    metrics.delete_file_count = delete_count
    metrics.total_delete_bytes = delete_bytes
    metrics.total_data_bytes = sum(data_sizes)
    if data_sizes:
        metrics.avg_file_bytes = metrics.total_data_bytes // len(data_sizes)
        metrics.small_file_count = sum(1 for s in data_sizes if s < SMALL_FILE_BYTES)
        metrics.small_file_ratio = metrics.small_file_count / len(data_sizes)
        metrics.delete_ratio = delete_count / len(data_sizes)
    metrics.file_size_histogram = _histogram(data_sizes)


def _histogram(sizes: list[int]) -> dict[str, int]:
    histogram = {label: 0 for label, _ in HISTOGRAM_BUCKETS}
    for size in sizes:
        for label, upper in HISTOGRAM_BUCKETS:
            if size < upper:
                histogram[label] += 1
                break
    return histogram


def _collect_snapshots(table: "Table", metrics: TableMetrics) -> None:
    snapshots = table.metadata.snapshots or []
    metrics.snapshot_count = len(snapshots)
    if not snapshots:
        return

    now = dt.datetime.now(dt.timezone.utc)
    timestamps = sorted(s.timestamp_ms for s in snapshots)
    oldest = dt.datetime.fromtimestamp(timestamps[0] / 1000, dt.timezone.utc)
    newest = dt.datetime.fromtimestamp(timestamps[-1] / 1000, dt.timezone.utc)
    metrics.oldest_snapshot_age_days = (now - oldest).total_seconds() / 86400
    metrics.newest_snapshot_age_days = (now - newest).total_seconds() / 86400

    span_days = max((newest - oldest).total_seconds() / 86400, 1 / 24)
    if len(snapshots) > 1:
        metrics.snapshots_per_day = len(snapshots) / span_days

    current = table.current_snapshot()
    if current is not None and current.summary is not None:
        summary = current.summary
        pairs: dict[str, str] = {"operation": str(summary.operation)}
        raw = getattr(summary, "additional_properties", None)
        if isinstance(raw, dict):
            pairs.update({str(k): str(v) for k, v in raw.items()})
        metrics.last_snapshot_summary = pairs


def _collect_manifests(table: "Table", metrics: TableMetrics) -> None:
    if table.current_snapshot() is None:
        return
    try:
        manifests = table.inspect.manifests()
    except Exception:
        return
    lengths = manifests.column("length").to_pylist() if "length" in manifests.column_names else []
    metrics.manifest_count = manifests.num_rows
    if lengths:
        metrics.avg_manifest_bytes = int(sum(lengths) / len(lengths))


def _collect_partitions(table: "Table", metrics: TableMetrics) -> None:
    if table.current_snapshot() is None:
        return
    try:
        partitions = table.inspect.partitions()
    except Exception:
        return
    metrics.partition_count = partitions.num_rows
    if "file_count" not in partitions.column_names or partitions.num_rows < 2:
        return
    file_counts = [int(c) for c in partitions.column("file_count").to_pylist() if c]
    if len(file_counts) >= 2:
        median = statistics.median(file_counts)
        if median > 0:
            metrics.partition_file_skew = max(file_counts) / median


def _collect_storage(table: "Table", metrics: TableMetrics) -> None:
    """Reachable bytes across all snapshots, plus a filesystem-level orphan estimate.

    The orphan estimate only works where we can list the storage cheaply — local
    warehouses for now. Object stores get this in v0.2 alongside clean-orphans.
    """
    metrics.reachable_bytes = _reachable_bytes(table)
    local_root = _local_path(table.location())
    if local_root is not None and local_root.is_dir():
        metrics.filesystem_bytes = sum(
            f.stat().st_size for f in local_root.rglob("*") if f.is_file()
        )
        if metrics.reachable_bytes is not None:
            metadata_bytes = (
                sum(f.stat().st_size for f in (local_root / "metadata").rglob("*") if f.is_file())
                if (local_root / "metadata").is_dir()
                else 0
            )
            estimate = metrics.filesystem_bytes - metadata_bytes - metrics.reachable_bytes
            metrics.orphan_bytes_estimate = max(estimate, 0)


def _reachable_bytes(table: "Table") -> Optional[int]:
    """Total bytes of unique data/delete files referenced by ANY snapshot."""
    if table.current_snapshot() is None:
        return 0
    inspect = table.inspect
    for endpoint in ("all_files", "all_data_files"):
        method = getattr(inspect, endpoint, None)
        if method is None:
            continue
        try:
            all_files = method()
        except Exception:
            continue
        paths = all_files.column("file_path").to_pylist()
        sizes = all_files.column("file_size_in_bytes").to_pylist()
        unique: dict[str, int] = {}
        for path, size in zip(paths, sizes):
            unique[path] = int(size)
        return sum(unique.values())
    return None


def _local_path(location: str | None) -> Optional[Path]:
    if not location:
        return None
    parsed = urlparse(location)
    if parsed.scheme in ("", "file"):
        return Path(parsed.path if parsed.scheme == "file" else location)
    return None
