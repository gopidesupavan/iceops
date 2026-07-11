from __future__ import annotations

import pyarrow as pa
import pytest

from iceops.errors import IceopsError
from iceops.models import RewriteManifestsPlan, RewriteManifestsResult
from iceops.operators.rewrite_manifests import MERGE_PROPS, rewrite_manifests


def make_fragmented(catalog, name: str, appends: int = 8):
    try:
        catalog.drop_table(name)
    except Exception:
        pass
    batch = pa.table({"id": pa.array(range(50), type=pa.int64())})
    table = catalog.create_table(name, schema=batch.schema)
    for _ in range(appends):
        table.append(batch)
    return catalog.load_table(name)


class TestRewriteManifestsIntegration:
    def test_dry_run_changes_nothing(self, seeded_catalog):
        make_fragmented(seeded_catalog, "db.frag1")
        plan = rewrite_manifests(seeded_catalog, "db.frag1")
        assert isinstance(plan, RewriteManifestsPlan)
        assert plan.manifest_count == 8
        assert plan.estimated_after == 1
        assert plan.actionable
        table = seeded_catalog.load_table("db.frag1")
        assert table.inspect.manifests().num_rows == 8  # untouched
        assert len(table.metadata.snapshots) == 8

    def test_execute_consolidates_and_preserves_data(self, seeded_catalog):
        table = make_fragmented(seeded_catalog, "db.frag2")
        rows_before = table.scan().to_arrow().num_rows
        paths_before = {str(p) for p in table.inspect.files().column("file_path").to_pylist()}
        snapshots_before = len(table.metadata.snapshots)

        result = rewrite_manifests(seeded_catalog, "db.frag2", execute=True)
        assert isinstance(result, RewriteManifestsResult)
        assert result.status == "rewritten"
        assert result.manifests_before == 8
        assert result.manifests_after == 1

        table = seeded_catalog.load_table("db.frag2")
        assert table.inspect.manifests().num_rows == 1
        assert table.scan().to_arrow().num_rows == rows_before
        paths_after = {str(p) for p in table.inspect.files().column("file_path").to_pylist()}
        assert paths_after == paths_before  # same live data files, new index
        # exactly one new snapshot; the previous one survives for rollback
        assert len(table.metadata.snapshots) == snapshots_before + 1
        assert result.new_snapshot_id == table.current_snapshot().snapshot_id

    def test_no_merge_properties_leak(self, seeded_catalog):
        make_fragmented(seeded_catalog, "db.frag3")
        rewrite_manifests(seeded_catalog, "db.frag3", execute=True)
        table = seeded_catalog.load_table("db.frag3")
        assert not any(prop in table.properties for prop in MERGE_PROPS)

    def test_user_set_merge_properties_are_restored(self, seeded_catalog):
        table = make_fragmented(seeded_catalog, "db.frag4")
        with table.transaction() as tx:
            tx.set_properties(
                {
                    "commit.manifest-merge.enabled": "false",
                    "commit.manifest.target-size-bytes": "1048576",
                }
            )
        rewrite_manifests(seeded_catalog, "db.frag4", execute=True)
        table = seeded_catalog.load_table("db.frag4")
        assert table.properties["commit.manifest-merge.enabled"] == "false"
        assert table.properties["commit.manifest.target-size-bytes"] == "1048576"
        assert "commit.manifest.min-count-to-merge" not in table.properties
        assert table.inspect.manifests().num_rows == 1  # and it still worked

    def test_nothing_to_do_paths(self, seeded_catalog):
        # single manifest -> not actionable, execute is a safe no-op
        make_fragmented(seeded_catalog, "db.frag5", appends=1)
        plan = rewrite_manifests(seeded_catalog, "db.frag5")
        assert isinstance(plan, RewriteManifestsPlan)
        assert not plan.actionable
        result = rewrite_manifests(seeded_catalog, "db.frag5", execute=True)
        assert isinstance(result, RewriteManifestsResult)
        assert result.status == "nothing-to-do"

    def test_managed_table_refused_without_force(self, seeded_catalog):
        table = make_fragmented(seeded_catalog, "db.frag6")
        with table.transaction() as tx:
            tx.set_properties({"self-optimizing.enabled": "true"})
        with pytest.raises(IceopsError, match="managed by amoro"):
            rewrite_manifests(seeded_catalog, "db.frag6")
        plan = rewrite_manifests(seeded_catalog, "db.frag6", force=True)
        assert isinstance(plan, RewriteManifestsPlan)
