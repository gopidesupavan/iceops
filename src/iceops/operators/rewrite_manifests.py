"""Manifest consolidation — fixes manifest fragmentation (metadata-only).

WHY THIS EXISTS
    Every append adds a manifest (the table's "index booklet"). A table written 60 times
    has 60 manifests of ~1 file each; query planning must open every one. This operator
    consolidates them into few large manifests. Data files are NEVER read or written —
    only the index is reorganized. The previous snapshot survives, so the operation is
    always rollbackable.

HOW IT WORKS — THE FLOW

    plan (read-only, `_build_plan`):
      1. `table.inspect.manifests()` → count, byte sizes, partition-spec ids
      2. group manifests by partition spec (specs never merge across each other)
      3. estimate after-count: bin-pack each group to --target-manifest-size
         (`estimate_after`, pure function)
      4. not actionable when 0/1 manifests or already consolidated

    execute (`_execute` → `_commit_rewrite`) — ONE atomic transaction:
      5. remember the table's current `commit.manifest*` properties (user may have set them)
      6. inside a single transaction:
           a. set  commit.manifest-merge.enabled=true, min-count-to-merge=2, target-size=N
           b. commit an EMPTY merge-append — zero data files appended. PyIceberg's own
              `_ManifestMergeManager` sees the properties, bin-packs ALL existing
              manifests, and rewrites them through its native ManifestWriter. This is
              the whole trick: we never encode Avro ourselves; we trigger the library's
              commit-time merging with nothing to append.
           c. restore every property to the remembered value (remove if it wasn't set)
         → the commit produces ONE new APPEND snapshot: same data files, new index,
           and no property changes left behind (all-or-nothing with the merge itself)
      7. on commit conflict (concurrent writer): refresh, retry once, then fail loudly —
         a failed attempt leaves only orphaned manifest files, reclaimable by clean-orphans

    post-verify (`_verify_unchanged`) — distrust our own commit before declaring success:
      8. reload the table; assert the live data-file path set and total row count are
         byte-for-byte identical, and no merge property leaked. On any mismatch: raise
         with rollback instructions (the pre-rewrite snapshot still exists).

Discovered by probe against PyIceberg 0.11.1 source — see design/plan-v0.2-manifests.md.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Optional

from ..catalog.detect import managed_by
from ..errors import IceopsError, TableNotFoundError
from ..models import RewriteManifestsPlan, RewriteManifestsResult

if TYPE_CHECKING:
    from pyiceberg.table import Table
    from pyiceberg.catalog import Catalog

DEFAULT_TARGET_MANIFEST_SIZE = 8 * 1024 * 1024  # PyIceberg/Java default

PROP_MERGE_ENABLED = "commit.manifest-merge.enabled"
PROP_MIN_MERGE_COUNT = "commit.manifest.min-count-to-merge"
PROP_TARGET_SIZE = "commit.manifest.target-size-bytes"
MERGE_PROPS = (PROP_MERGE_ENABLED, PROP_MIN_MERGE_COUNT, PROP_TARGET_SIZE)


def rewrite_manifests(
    catalog: "Catalog",
    identifier: str,
    target_manifest_size: int = DEFAULT_TARGET_MANIFEST_SIZE,
    execute: bool = False,
    force: bool = False,
) -> RewriteManifestsPlan | RewriteManifestsResult:
    try:
        table = catalog.load_table(identifier)
    except Exception as exc:
        raise TableNotFoundError(f"could not load table '{identifier}': {exc}") from exc

    manager = managed_by({str(k): str(v) for k, v in table.properties.items()}, table.location())
    if manager and not force:
        raise IceopsError(
            f"'{identifier}' looks managed by {manager} — rewriting manifests behind "
            f"another optimizer's back causes commit conflicts. Use --force to override."
        )

    plan = _build_plan(table, identifier, target_manifest_size)
    if not execute:
        return plan
    return _execute(table, plan)


def estimate_after(groups: dict[int, list[int]], target: int) -> int:
    """Manifests remaining after bin-packing each partition-spec group to `target`."""
    total = 0
    for lengths in groups.values():
        total += max(1, math.ceil(sum(lengths) / target))
    return total


def _build_plan(table: "Table", identifier: str, target_manifest_size: int) -> RewriteManifestsPlan:
    plan = RewriteManifestsPlan(
        identifier=identifier, target_manifest_size_bytes=target_manifest_size
    )
    if table.current_snapshot() is None:
        return plan

    manifests = table.inspect.manifests()
    plan.manifest_count = manifests.num_rows
    if plan.manifest_count == 0:
        return plan

    names = manifests.column_names
    lengths = manifests.column("length").to_pylist() if "length" in names else []
    spec_ids = (
        manifests.column("partition_spec_id").to_pylist()
        if "partition_spec_id" in names
        else [0] * plan.manifest_count
    )
    plan.manifest_bytes = int(sum(lengths))

    file_count = 0
    for column in ("added_data_files_count", "existing_data_files_count"):
        if column in names:
            file_count += sum(int(c or 0) for c in manifests.column(column).to_pylist())
    if plan.manifest_count:
        plan.files_per_manifest = round(file_count / plan.manifest_count, 1)

    groups: dict[int, list[int]] = {}
    for spec_id, length in zip(spec_ids, lengths):
        groups.setdefault(int(spec_id), []).append(int(length))
    plan.estimated_after = estimate_after(groups, target_manifest_size)
    return plan


def _execute(table: "Table", plan: RewriteManifestsPlan) -> RewriteManifestsResult:
    if not plan.actionable:
        return RewriteManifestsResult(
            plan=plan,
            manifests_before=plan.manifest_count,
            manifests_after=plan.manifest_count,
            status="nothing-to-do",
        )

    before_paths, before_rows = _live_files(table)
    original = {prop: table.properties.get(prop) for prop in MERGE_PROPS}

    try:
        _commit_rewrite(table, plan.target_manifest_size_bytes, original)
    except Exception:
        # one retry on commit conflicts (concurrent writer), then fail loudly
        table.refresh()
        _commit_rewrite(table, plan.target_manifest_size_bytes, original)

    table.refresh()
    _verify_unchanged(table, plan.identifier, before_paths, before_rows, original)

    current = table.current_snapshot()
    return RewriteManifestsResult(
        plan=plan,
        manifests_before=plan.manifest_count,
        manifests_after=table.inspect.manifests().num_rows,
        new_snapshot_id=current.snapshot_id if current else None,
        status="rewritten",
    )


def _commit_rewrite(table: "Table", target_size: int, original: dict[str, Optional[str]]) -> None:
    tx = table.transaction()
    tx.set_properties(
        {
            PROP_MERGE_ENABLED: "true",
            PROP_MIN_MERGE_COUNT: "2",
            PROP_TARGET_SIZE: str(target_size),
        }
    )
    with tx.update_snapshot().merge_append():
        pass  # zero files: the commit exists only to run manifest merging
    restore = {prop: value for prop, value in original.items() if value is not None}
    remove = [prop for prop, value in original.items() if value is None]
    if restore:
        tx.set_properties(restore)
    if remove:
        tx.remove_properties(*remove)
    tx.commit_transaction()


def _live_files(table: "Table") -> tuple[set[str], int]:
    files = table.inspect.files()
    paths = {str(p) for p in files.column("file_path").to_pylist()}
    rows = sum(int(c or 0) for c in files.column("record_count").to_pylist())
    return paths, rows


def _verify_unchanged(
    table: "Table",
    identifier: str,
    before_paths: set[str],
    before_rows: int,
    original: dict[str, Optional[str]],
) -> None:
    after_paths, after_rows = _live_files(table)
    if after_paths != before_paths or after_rows != before_rows:
        raise IceopsError(
            f"post-rewrite verification FAILED for '{identifier}': live file set or row "
            f"count changed ({before_rows} -> {after_rows} rows). The previous snapshot "
            f"is intact — roll back via table.manage_snapshots().rollback_to_snapshot()."
        )
    for prop in MERGE_PROPS:
        if table.properties.get(prop) != original[prop]:
            raise IceopsError(
                f"post-rewrite verification FAILED for '{identifier}': table property "
                f"'{prop}' was not restored to its original value."
            )
