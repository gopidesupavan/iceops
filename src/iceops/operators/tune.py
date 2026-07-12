"""Sequenced maintenance — the one command that runs the right ops in the right order.

tune adds ZERO mutation code: it composes the four fix operators in the Iceberg-standard
order (compact -> rewrite-manifests -> expire -> clean-orphans) so a user can't corrupt a
table by sequencing maintenance wrong.

Two honest properties:
  1. The dry-run is an APPROXIMATION — each step is planned against the current table, but
     each real step changes what the next sees (compact adds the snapshot expire later
     drops). On --yes, each operator re-plans and executes atomically at its turn, so
     plans-never-re-decide still holds per operator.
  2. A single run does NOT fully reclaim space: clean-orphans respects its age threshold,
     so files this run just orphaned aren't deleted until a later run when they age past
     it. That is the safe behaviour, not a bug.
"""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Any, Optional

from ..catalog.detect import managed_by
from ..errors import IceopsError, TableNotFoundError
from ..models import TunePlan, TuneResult
from .clean_orphans import DEFAULT_OLDER_THAN as ORPHANS_DEFAULT_OLDER_THAN
from .clean_orphans import clean_orphans
from .compact import DEFAULT_TARGET_FILE_SIZE, compact
from .expire import DEFAULT_OLDER_THAN as EXPIRE_DEFAULT_OLDER_THAN
from .expire import DEFAULT_RETAIN_LAST, expire
from .rewrite_manifests import DEFAULT_TARGET_MANIFEST_SIZE, rewrite_manifests

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog

# maintenance order — see module docstring for why compact precedes rewrite-manifests
STEP_ORDER = ("compact", "rewrite_manifests", "expire", "clean_orphans")


def tune(
    catalog: "Catalog",
    identifier: str,
    engine: Optional[str] = None,
    engine_catalog: Optional[str] = None,
    engine_config: Optional[dict[str, Any]] = None,
    retain_last: int = DEFAULT_RETAIN_LAST,
    older_than_expire: dt.timedelta = EXPIRE_DEFAULT_OLDER_THAN,
    older_than_orphans: dt.timedelta = ORPHANS_DEFAULT_OLDER_THAN,
    exclude: tuple[str, ...] = (),
    target_file_size: int = DEFAULT_TARGET_FILE_SIZE,
    target_manifest_size: int = DEFAULT_TARGET_MANIFEST_SIZE,
    execute: bool = False,
    force: bool = False,
) -> TunePlan | TuneResult:
    try:
        table = catalog.load_table(identifier)
    except Exception as exc:
        raise TableNotFoundError(f"could not load table '{identifier}': {exc}") from exc

    manager = managed_by({str(k): str(v) for k, v in table.properties.items()}, table.location())
    if manager and not force:
        raise IceopsError(
            f"'{identifier}' looks managed by {manager} — tuning behind another optimizer's "
            f"back causes commit conflicts. Use --force to override."
        )

    # each step is (name, callable building its plan/result for the given execute flag)
    def run_compact(exec_: bool):
        if engine is None:
            return None
        return compact(
            catalog,
            identifier,
            target_file_size=target_file_size,
            engine=engine,
            engine_catalog=engine_catalog,
            engine_config=engine_config,
            execute=exec_,
            force=force,
        )

    def run_rewrite(exec_: bool):
        return rewrite_manifests(
            catalog,
            identifier,
            target_manifest_size=target_manifest_size,
            engine=engine,
            engine_catalog=engine_catalog,
            engine_config=engine_config,
            execute=exec_,
            force=force,
        )

    def run_expire(exec_: bool):
        return expire(
            catalog,
            identifier,
            retain_last=retain_last,
            older_than=older_than_expire,
            engine=engine,
            engine_catalog=engine_catalog,
            engine_config=engine_config,
            execute=exec_,
            force=force,
        )

    def run_clean(exec_: bool):
        return clean_orphans(
            catalog,
            identifier,
            older_than=older_than_orphans,
            exclude=exclude,
            engine=engine,
            engine_catalog=engine_catalog,
            engine_config=engine_config,
            execute=exec_,
            force=force,
        )

    steps = {
        "compact": run_compact,
        "rewrite_manifests": run_rewrite,
        "expire": run_expire,
        "clean_orphans": run_clean,
    }

    plan = TunePlan(identifier=identifier, engine=engine)
    if engine is None:
        plan.skipped["compact"] = "no --engine (compaction needs spark or trino)"
    for name in STEP_ORDER:
        sub = steps[name](False)
        if sub is not None:
            setattr(plan, name, sub)

    if not execute:
        return plan

    return _execute(plan, steps)


def _execute(plan: TunePlan, steps: dict) -> TuneResult:
    result = TuneResult(plan=plan)
    for name in STEP_ORDER:
        # skip only what's genuinely unavailable (compact with no engine). We do NOT gate
        # on the dry-run sub-plan's actionability: an earlier step can make a later one
        # actionable (compact adds the snapshot expire then drops), so each step re-plans
        # fresh at its turn and no-ops if there's truly nothing to do.
        if name in plan.skipped or getattr(plan, name) is None:
            continue
        try:
            sub_result = steps[name](True)
        except IceopsError:
            result.halted_at = name
            result.status = "halted"
            return result
        if sub_result is None:
            continue
        setattr(result, name, sub_result)
        if getattr(sub_result, "status", "") != "nothing-to-do":
            result.executed.append(name)

    if not result.executed and result.halted_at is None:
        result.status = "nothing-to-do"
    return result
