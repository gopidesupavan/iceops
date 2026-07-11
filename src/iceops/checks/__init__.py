"""Health-check registry."""

from __future__ import annotations

from .base import Check, check
from .deletes import delete_files
from .manifests import manifest_fragmentation
from .metadata_config import metadata_cleanup_disabled
from .orphans import orphan_files
from .partitions import partition_skew
from .small_files import small_files
from .snapshots import snapshot_bloat

__all__ = ["Check", "check", "all_checks"]

_REGISTRY: list[Check] = [
    small_files,
    snapshot_bloat,
    manifest_fragmentation,
    delete_files,
    partition_skew,
    orphan_files,
    metadata_cleanup_disabled,
]


def all_checks() -> list[Check]:
    return list(_REGISTRY)
