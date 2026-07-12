"""iceops.yaml schema — per-table maintenance as code.

A checked-in policy that `iceops apply` runs. Policy is PER TABLE: `defaults` apply to
every table, and `tables` entries (glob → overrides) tune individual tables — hot tables
aggressive, cold tables gentle, some tables disabled entirely.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field


class _Base(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class CompactPolicy(_Base):
    target_file_size: str = Field(default="512MB", alias="target-file-size")
    when: Optional[str] = None  # e.g. "small-file-ratio > 0.3"


class RewriteManifestsPolicy(_Base):
    target_manifest_size: str = Field(default="8MB", alias="target-manifest-size")
    when: Optional[str] = None


class ExpireSnapshotsPolicy(_Base):
    retain_last: int = Field(default=10, alias="retain-last")
    older_than: str = Field(default="7d", alias="older-than")
    when: Optional[str] = None


class CleanOrphansPolicy(_Base):
    older_than: str = Field(default="3d", alias="older-than")
    when: Optional[str] = None


class PolicySpec(_Base):
    """The four op policies. An absent op never runs for the table."""

    compact: Optional[CompactPolicy] = None
    rewrite_manifests: Optional[RewriteManifestsPolicy] = Field(
        default=None, alias="rewrite-manifests"
    )
    expire_snapshots: Optional[ExpireSnapshotsPolicy] = Field(
        default=None, alias="expire-snapshots"
    )
    clean_orphans: Optional[CleanOrphansPolicy] = Field(default=None, alias="clean-orphans")


class TablePolicy(PolicySpec):
    """A per-table override block: the op policies plus table-level controls."""

    disabled: bool = False
    engine: Optional[str] = None


class PolicyDoc(_Base):
    catalog: Optional[str] = None
    engine: Optional[str] = None  # global engine; a table's engine overrides it
    defaults: PolicySpec = Field(default_factory=PolicySpec)
    tables: dict[str, TablePolicy] = Field(default_factory=dict)


def load_policy(path: str | Path) -> PolicyDoc:
    return PolicyDoc.model_validate(yaml.safe_load(Path(path).read_text()))
