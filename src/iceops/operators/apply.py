"""`iceops apply` — run a per-table policy across a catalog.

Composition only, ZERO new mutation code: for each table, resolve its policy, collect
metrics, decide which of the four operators run (op present in policy AND its `when:`
passes), then execute them in the canonical maintenance order — reusing tune's ordering
and the existing operators. Dry-run by default; per-table halt on error.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from ..catalog import list_table_identifiers
from ..errors import IceopsError
from ..inspect import collect
from ..models import (
    ApplyPlan,
    ApplyResult,
    OpDecision,
    TableApplyPlan,
    TableApplyResult,
    parse_duration,
    parse_size,
)
from ..policy import ResolvedPolicy, parse_when, resolve
from ..policy.schema import PolicyDoc
from .clean_orphans import clean_orphans
from .compact import compact
from .expire import expire
from .rewrite_manifests import rewrite_manifests
from .tune import STEP_ORDER  # canonical order: compact → rewrite → expire → clean

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog


def apply(
    catalog: "Catalog",
    doc: PolicyDoc,
    catalog_name: str,
    engine_config: Optional[dict[str, Any]] = None,
    execute: bool = False,
    force: bool = False,
) -> ApplyPlan | ApplyResult:
    plan = ApplyPlan(catalog=catalog_name)
    resolved: dict[str, ResolvedPolicy] = {}

    for identifier in list_table_identifiers(catalog):
        rp = resolve(doc, identifier)
        if rp is None:
            # only note tables explicitly disabled; unmatched tables are silently out of scope
            if identifier in doc.tables and doc.tables[identifier].disabled:
                plan.skipped[identifier] = "disabled by policy"
            continue
        try:
            table = catalog.load_table(identifier)
            metrics = collect(table, identifier)
        except Exception as exc:
            plan.skipped[identifier] = f"could not read: {exc}"
            continue
        resolved[identifier] = rp
        plan.tables.append(_table_plan(rp, metrics))

    if not execute:
        return plan
    return _execute(catalog, plan, resolved, engine_config, force)


def _table_plan(rp: ResolvedPolicy, metrics) -> TableApplyPlan:
    tp = TableApplyPlan(identifier=rp.identifier, engine=rp.engine)
    for op in STEP_ORDER:
        policy = _op_policy(rp, op)
        if policy is None:
            continue  # op not in this table's policy → never runs (not even listed)
        when = getattr(policy, "when", None)
        if when is None:
            tp.decisions.append(OpDecision(op=op, will_run=True, reason="no condition"))
            continue
        cond = parse_when(when)
        tp.decisions.append(
            OpDecision(op=op, will_run=cond.evaluate(metrics), reason=cond.describe(metrics))
        )
    return tp


def _execute(
    catalog: "Catalog",
    plan: ApplyPlan,
    resolved: dict[str, ResolvedPolicy],
    engine_config: Optional[dict[str, Any]],
    force: bool,
) -> ApplyResult:
    result = ApplyResult(plan=plan)
    ran_any = False
    for table_plan in plan.tables:
        rp = resolved[table_plan.identifier]
        tr = TableApplyResult(identifier=table_plan.identifier)
        for decision in table_plan.decisions:
            if not decision.will_run:
                continue
            try:
                _run_op(catalog, rp, decision.op, engine_config, force)
            except IceopsError as exc:
                tr.halted_at = decision.op
                tr.error = str(exc)
                break
            tr.executed.append(decision.op)
            ran_any = True
        result.results.append(tr)

    if not ran_any and not any(r.halted_at for r in result.results):
        result.status = "nothing-to-do"
    return result


def _run_op(
    catalog: "Catalog",
    rp: ResolvedPolicy,
    op: str,
    engine_config: Optional[dict[str, Any]],
    force: bool,
) -> None:
    engine = rp.engine
    engine_catalog = rp.identifier.split(".", 1)[0] if engine else None
    ident = rp.identifier

    if op == "compact":
        if engine is None:
            raise IceopsError("compact policy requires an engine (set engine: spark|trino)")
        compact_p = rp.spec.compact
        assert compact_p is not None
        compact(
            catalog,
            ident,
            target_file_size=parse_size(compact_p.target_file_size),
            engine=engine,
            engine_catalog=engine_catalog,
            engine_config=engine_config,
            execute=True,
            force=force,
        )
    elif op == "rewrite_manifests":
        rewrite_p = rp.spec.rewrite_manifests
        assert rewrite_p is not None
        rewrite_manifests(
            catalog,
            ident,
            target_manifest_size=parse_size(rewrite_p.target_manifest_size),
            engine=engine,
            engine_catalog=engine_catalog,
            engine_config=engine_config,
            execute=True,
            force=force,
        )
    elif op == "expire":
        expire_p = rp.spec.expire_snapshots
        assert expire_p is not None
        expire(
            catalog,
            ident,
            retain_last=expire_p.retain_last,
            older_than=parse_duration(expire_p.older_than),
            engine=engine,
            engine_catalog=engine_catalog,
            engine_config=engine_config,
            execute=True,
            force=force,
        )
    elif op == "clean_orphans":
        clean_p = rp.spec.clean_orphans
        assert clean_p is not None
        clean_orphans(
            catalog,
            ident,
            older_than=parse_duration(clean_p.older_than),
            engine=engine,
            engine_catalog=engine_catalog,
            engine_config=engine_config,
            execute=True,
            force=force,
        )


def _op_policy(rp: ResolvedPolicy, op: str):
    return {
        "compact": rp.spec.compact,
        "rewrite_manifests": rp.spec.rewrite_manifests,
        "expire": rp.spec.expire_snapshots,
        "clean_orphans": rp.spec.clean_orphans,
    }[op]
