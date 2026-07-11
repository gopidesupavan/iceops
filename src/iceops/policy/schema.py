"""iceops.yaml schema — maintenance as code.

The planner that turns a policy into per-table Plans ships in v0.3; the schema exists now
so the file format is stable and examples validate.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field


class CompactPolicy(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    target_file_size: str = Field(default="512MB", alias="target-file-size")
    when: Optional[str] = None  # e.g. "small-file-ratio > 0.3"


class ExpireSnapshotsPolicy(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    retain_last: int = Field(default=10, alias="retain-last")
    older_than: str = Field(default="7d", alias="older-than")


class CleanOrphansPolicy(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    older_than: str = Field(default="3d", alias="older-than")


class PolicySpec(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    compact: Optional[CompactPolicy] = None
    expire_snapshots: Optional[ExpireSnapshotsPolicy] = Field(
        default=None, alias="expire-snapshots"
    )
    clean_orphans: Optional[CleanOrphansPolicy] = Field(default=None, alias="clean-orphans")


class PolicyDoc(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    catalog: Optional[str] = None
    tables: list[str] = Field(default_factory=lambda: ["*"])
    policy: PolicySpec = Field(default_factory=PolicySpec)


def load_policy(path: str | Path) -> PolicyDoc:
    return PolicyDoc.model_validate(yaml.safe_load(Path(path).read_text()))
