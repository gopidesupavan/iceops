from __future__ import annotations

from typing import Any

from ..errors import IceopsError
from ..models import Action, ActionResult, Plan
from .base import Engine
from .native import NativeEngine

__all__ = ["Engine", "get_engine", "submit"]


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
