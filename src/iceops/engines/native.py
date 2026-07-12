"""In-process engine (Arrow/DuckDB). The default: no cluster, no JVM."""

from __future__ import annotations

from ..errors import NotYetImplemented
from ..models import ActionResult, Plan


class NativeEngine:
    name = "native"

    def execute(self, plan: Plan) -> list[ActionResult]:
        if plan.actions:
            raise NotYetImplemented(f"native execution of '{plan.actions[0].op}'")
        return []
