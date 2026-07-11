"""Trino engine: submits ALTER TABLE ... EXECUTE optimize / expire_snapshots over the
Trino Python client. Ships in v0.2.

Requires: pip install iceops[trino]
"""

from __future__ import annotations

from ..errors import NotYetImplemented
from ..models import ActionResult, Plan


class TrinoEngine:
    name = "trino"

    def __init__(self, *args: object, **kwargs: object) -> None:
        raise NotYetImplemented("trino engine", "v0.2")

    def execute(self, plan: Plan) -> list[ActionResult]:
        raise NotYetImplemented("trino engine", "v0.2")
