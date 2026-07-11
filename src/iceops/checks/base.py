"""Check protocol: a check inspects TableMetrics and returns at most one Finding."""

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
