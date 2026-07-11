"""In-process engine (Arrow/DuckDB). The default: no cluster, no JVM.

v0.1 carries the structure only; action executors arrive with the fix operators in v0.2.
"""

from __future__ import annotations

from ..errors import NotYetImplemented
from ..models import ActionResult, Plan


class NativeEngine:
    name = "native"

    def execute(self, plan: Plan) -> list[ActionResult]:
        if plan.actions:
            raise NotYetImplemented(f"native execution of '{plan.actions[0].op}'", "v0.2")
        return []
