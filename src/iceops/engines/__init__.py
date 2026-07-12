from __future__ import annotations

from typing import Any

from ..errors import IceopsError
from ..models import Action, ActionResult, Plan
from .base import Engine
from .native import NativeEngine

__all__ = [
    "Engine",
    "build_statement",
    "get_engine",
    "submit",
    "validate_engine",
    "SUPPORTED_ENGINES",
]

SUPPORTED_ENGINES = ("spark", "trino")


def validate_engine(engine: str) -> None:
    """Reject an unknown engine name up front (in dry-run too), so a typo fails fast
    instead of only surfacing at execute time."""
    if engine not in SUPPORTED_ENGINES:
        raise IceopsError(f"unknown engine '{engine}' (expected spark or trino)")


def get_engine(name: str = "native", **config: Any) -> Engine:
    if name == "native":
        return NativeEngine()
    if name == "spark":
        from .spark import SparkEngine

        return SparkEngine(**config)
    if name == "trino":
        from .trino import TrinoEngine

        return TrinoEngine(**config)
    raise IceopsError(f"unknown engine '{name}' (expected native, spark, or trino)")


def build_statement(engine: str, action: Action) -> str:
    """Render the exact SQL/procedure statement for an engine action."""
    validate_engine(engine)
    if engine == "spark":
        from .spark import SPARK_SQL_BUILDERS

        builder = SPARK_SQL_BUILDERS.get(action.op)
    else:
        from .trino import TRINO_SQL_BUILDERS

        builder = TRINO_SQL_BUILDERS.get(action.op)
    if builder is None:
        raise IceopsError(f"{engine} engine cannot execute '{action.op}'")
    return builder(action)


def submit(
    engine: str,
    op: str,
    table: str,
    params: dict[str, Any],
    engine_config: dict[str, Any] | None = None,
) -> list[ActionResult]:
    """Build a single-action Plan and hand it to the engine — the one delegation seam
    every fix operator uses for its engine-backed path."""
    action = Action(op=op, table=table, params=params)
    return get_engine(engine, **(engine_config or {})).execute(Plan(table=table, actions=[action]))
