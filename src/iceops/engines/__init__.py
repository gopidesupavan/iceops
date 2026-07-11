from __future__ import annotations

from ..errors import IceopsError
from .base import Engine
from .native import NativeEngine

__all__ = ["Engine", "get_engine"]


def get_engine(name: str = "native") -> Engine:
    if name == "native":
        return NativeEngine()
    if name == "spark":
        from .spark import SparkEngine

        return SparkEngine()
    if name == "trino":
        from .trino import TrinoEngine

        return TrinoEngine()
    raise IceopsError(f"unknown engine '{name}' (expected native, spark, or trino)")
