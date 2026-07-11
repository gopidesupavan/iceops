from __future__ import annotations

from types import SimpleNamespace

from iceops.errors import IceopsError
from iceops.models import CompactPlan, RewriteManifestsPlan, TunePlan
from iceops.operators.tune import STEP_ORDER, _execute


def _did(status: str = "done"):
    """A plain result-shaped stand-in (status is all _execute inspects)."""
    return SimpleNamespace(status=status)


def test_step_order_is_the_maintenance_sequence():
    assert STEP_ORDER == ("compact", "rewrite_manifests", "expire", "clean_orphans")


class TestTunePlanActionable:
    def test_empty_plan_not_actionable(self):
        assert not TunePlan(identifier="db.t").actionable

    def test_any_actionable_subplan_makes_it_actionable(self):
        plan = TunePlan(
            identifier="db.t",
            compact=CompactPlan(identifier="db.t", small_file_count=3),  # actionable
        )
        assert plan.actionable


class TestExecuteOrchestration:
    """_execute drives the steps; pin the halt + skip behaviour deterministically."""

    def _plan(self):
        # two actionable steps: rewrite-manifests then (later) expire via clean_orphans slot
        return TunePlan(
            identifier="db.t",
            rewrite_manifests=RewriteManifestsPlan(
                identifier="db.t", manifest_count=5, estimated_after=1
            ),
            compact=CompactPlan(identifier="db.t", small_file_count=3),
        )

    def test_halt_stops_and_records_where(self):
        calls: list[str] = []

        def compact_step(exec_: bool):
            calls.append("compact")
            raise IceopsError("engine boom")

        def rewrite_step(exec_: bool):
            calls.append("rewrite_manifests")
            return _did()

        steps = {
            "compact": compact_step,
            "rewrite_manifests": rewrite_step,
            "expire": lambda e: _did(),
            "clean_orphans": lambda e: _did(),
        }
        result = _execute(self._plan(), steps)
        assert result.status == "halted"
        assert result.halted_at == "compact"  # compact is first in STEP_ORDER
        assert calls == ["compact"]  # never reached rewrite-manifests
        assert result.executed == []

    def test_only_available_steps_run_in_order(self):
        ran: list[str] = []

        def step(name: str):
            def run(exec_: bool):
                ran.append(name)
                return _did()

            return run

        steps = {name: step(name) for name in STEP_ORDER}
        # compact/expire/clean absent (None) → unavailable; only rewrite present
        plan = TunePlan(
            identifier="db.t",
            rewrite_manifests=RewriteManifestsPlan(
                identifier="db.t", manifest_count=5, estimated_after=1
            ),
        )
        result = _execute(plan, steps)
        assert ran == ["rewrite_manifests"]
        assert result.executed == ["rewrite_manifests"]
        assert result.status == "tuned"

    def test_nothing_to_do_result_is_not_counted_as_executed(self):
        plan = TunePlan(
            identifier="db.t",
            rewrite_manifests=RewriteManifestsPlan(
                identifier="db.t", manifest_count=5, estimated_after=1
            ),
        )
        steps = {name: (lambda e: _did("nothing-to-do")) for name in STEP_ORDER}
        result = _execute(plan, steps)
        assert result.executed == []
        assert result.status == "nothing-to-do"
