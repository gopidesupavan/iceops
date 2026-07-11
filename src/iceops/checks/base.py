"""Check protocol: a check inspects TableMetrics and returns at most one Finding.

HOW A CHECK WORKS
    1. write a pure function: TableMetrics in → Finding | None out (None = "all good").
       No I/O, no table access — thresholds live as constants next to the function.
    2. decorate it with @check("machine-name"). The decorator wraps the function in a
       FunctionCheck object carrying `.id` + `.run()` — the contract the Check Protocol
       below defines. After decoration the module name IS that object, not a function.
    3. register it in the explicit _REGISTRY list in checks/__init__.py (no magic
       auto-discovery — adding a check is two reviewable lines).
    4. doctor calls `run(metrics)` on every registered check and keeps the non-None
       results; the worst severity among them becomes the table's status.
"""

from __future__ import annotations

from typing import Callable, Optional, Protocol

from ..models import Finding, Severity, TableMetrics

__all__ = ["Check", "Severity", "Finding", "check"]


class Check(Protocol):
    id: str

    def run(self, metrics: TableMetrics) -> Optional[Finding]: ...


class FunctionCheck:
    def __init__(self, check_id: str, fn: Callable[[TableMetrics], Optional[Finding]]) -> None:
        self.id = check_id
        self._fn = fn

    def run(self, metrics: TableMetrics) -> Optional[Finding]:
        return self._fn(metrics)


def check(check_id: str) -> Callable[[Callable[[TableMetrics], Optional[Finding]]], FunctionCheck]:
    def wrap(fn: Callable[[TableMetrics], Optional[Finding]]) -> FunctionCheck:
        return FunctionCheck(check_id, fn)

    return wrap
