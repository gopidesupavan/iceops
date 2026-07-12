"""apply against a real seeded catalog (native path).

Verifies per-table policy resolution drives the right operators: fragmented table
converges, a disabled table is untouched, per-table overrides honored.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from iceops.models import ApplyPlan, ApplyResult
from iceops.operators import apply
from iceops.policy.schema import PolicyDoc


def _fragmented(catalog, name: str, appends: int = 8):
    try:
        catalog.drop_table(name)
    except Exception:
        pass
    batch = pa.table({"id": pa.array(range(50), type=pa.int64())})
    t = catalog.create_table(name, schema=batch.schema)
    for _ in range(appends):
        t.append(batch)
    return catalog.load_table(name)


def _ops():
    return {
        "rewrite-manifests": {"when": "manifest-count > 2"},
        "expire-snapshots": {"retain-last": 2, "older-than": "0s"},
        "clean-orphans": {"older-than": "0s"},
    }


@pytest.fixture()
def policy_doc():
    # scoped to our own tables via `tables:` entries (NOT fleet-wide defaults) so the
    # shared seeded catalog's other tables stay untouched. db.applykeep is disabled.
    return PolicyDoc.model_validate(
        {"tables": {"db.applyme": _ops(), "db.applykeep": {"disabled": True}}}
    )


def test_dry_run_selects_ops_per_table(seeded_catalog, policy_doc):
    _fragmented(seeded_catalog, "db.applyme")
    _fragmented(seeded_catalog, "db.applykeep", appends=4)

    plan = apply(seeded_catalog, policy_doc, "test")
    assert isinstance(plan, ApplyPlan)
    by_id = {t.identifier: t for t in plan.tables}

    # applyme: rewrite-manifests should be selected (8 manifests > 2)
    assert "db.applyme" in by_id
    rewrite = next(d for d in by_id["db.applyme"].decisions if d.op == "rewrite_manifests")
    assert rewrite.will_run
    # applykeep is disabled → not planned, and recorded as skipped
    assert "db.applykeep" not in by_id
    assert plan.skipped.get("db.applykeep") == "disabled by policy"


def test_execute_converges_and_respects_disabled(seeded_catalog, policy_doc):
    table = _fragmented(seeded_catalog, "db.applyme")
    rows_before = table.scan().to_arrow().num_rows
    keep = _fragmented(seeded_catalog, "db.applykeep", appends=4)
    keep_manifests_before = keep.inspect.manifests().num_rows

    result = apply(seeded_catalog, policy_doc, "test", execute=True)
    assert isinstance(result, ApplyResult)
    assert result.status == "applied"

    # applyme converged: manifests consolidated, rows intact
    table = seeded_catalog.load_table("db.applyme")
    assert table.inspect.manifests().num_rows == 1
    assert table.scan().to_arrow().num_rows == rows_before

    # applykeep (disabled) is completely untouched
    keep = seeded_catalog.load_table("db.applykeep")
    assert keep.inspect.manifests().num_rows == keep_manifests_before


def test_per_table_retain_last_override_is_honored(seeded_catalog):
    _fragmented(seeded_catalog, "db.applyhot", appends=8)
    doc = PolicyDoc.model_validate(
        {"tables": {"db.applyhot": {"expire-snapshots": {"retain-last": 5, "older-than": "0s"}}}}
    )
    apply(seeded_catalog, doc, "test", execute=True)
    # retain-last 5 (table override) beats the default 2
    assert len(seeded_catalog.load_table("db.applyhot").metadata.snapshots) >= 5


def test_managed_table_skipped_via_force_default(seeded_catalog):
    table = _fragmented(seeded_catalog, "db.applymanaged")
    with table.transaction() as tx:
        tx.set_properties({"self-optimizing.enabled": "true"})
    doc = PolicyDoc.model_validate(
        {"tables": {"db.applymanaged": {"rewrite-manifests": {"when": "manifest-count > 2"}}}}
    )
    result = apply(seeded_catalog, doc, "test", execute=True)
    assert isinstance(result, ApplyResult)
    tr = next(r for r in result.results if r.identifier == "db.applymanaged")
    assert tr.halted_at == "rewrite_manifests"  # managed guard halted it
    assert "managed by amoro" in (tr.error or "")
