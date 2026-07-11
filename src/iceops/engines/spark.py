"""Spark Connect engine: submits CALL system.rewrite_data_files(...) and friends to the
user's existing Spark — iceops stays the brain, Spark is rented muscle. Ships in v0.2.

Requires: pip install iceops[spark]
"""

from __future__ import annotations

from ..errors import NotYetImplemented
from ..models import ActionResult, Plan


class SparkEngine:
    name = "spark"

    def __init__(self, *args: object, **kwargs: object) -> None:
        raise NotYetImplemented("spark engine", "v0.2")

    def execute(self, plan: Plan) -> list[ActionResult]:
        raise NotYetImplemented("spark engine", "v0.2")
