"""Engine protocol: operators produce Plans, engines execute them.

Diagnose operators never need an engine. Fix operators plan via the catalog and hand
heavy file work to whichever engine is configured.
"""

from __future__ import annotations

from typing import Protocol

from ..models import ActionResult, Plan


class Engine(Protocol):
    name: str

    def execute(self, plan: Plan) -> list[ActionResult]: ...
