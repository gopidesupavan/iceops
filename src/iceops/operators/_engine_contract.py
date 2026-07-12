from __future__ import annotations

from ..engines import build_statement
from ..models import Action, EnginePlanContract, PlanKind


def catalog_name_from_table(table: object) -> str | None:
    name = getattr(getattr(table, "catalog", None), "name", None)
    return str(name) if name else None


def delegated_contract(
    engine: str,
    action: Action,
    *,
    owns: list[str],
    iceops_owns: list[str],
    safety_notes: list[str],
    verification_notes: list[str] | None = None,
) -> EnginePlanContract:
    return EnginePlanContract(
        engine=engine,
        plan_kind=PlanKind.DELEGATED,
        statement=build_statement(engine, action),
        owns=owns,
        iceops_owns=iceops_owns,
        safety_notes=safety_notes,
        verification_notes=verification_notes or [],
    )
