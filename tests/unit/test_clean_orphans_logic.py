from __future__ import annotations

import datetime as dt

from iceops.models import OrphanFile
from iceops.operators.clean_orphans import filter_candidates, normalize_path

OLD = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30)
NOW = dt.datetime.now(dt.timezone.utc)


class TestNormalizePath:
    def test_file_uri_equals_bare_path(self):
        assert normalize_path("file:///wh/t/data/f.parquet") == "/wh/t/data/f.parquet"
        assert normalize_path("/wh/t/data/f.parquet") == "/wh/t/data/f.parquet"

    def test_s3_uri_equals_bucket_key(self):
        assert normalize_path("s3://bucket/wh/t/f.parquet") == "bucket/wh/t/f.parquet"

    def test_url_encoding(self):
        assert normalize_path("file:///wh/my%20table/f.parquet") == "/wh/my table/f.parquet"

    def test_gs_and_abfs_schemes(self):
        assert normalize_path("gs://b/k") == "b/k"
        assert normalize_path("abfss://container@acct.dfs.core.windows.net/k").endswith("/k")


def orphan(path: str, mtime: dt.datetime = OLD, size: int = 100) -> OrphanFile:
    return OrphanFile(path=path, size_bytes=size, modified_at=mtime)


class TestFunnel:
    LOC = "/wh/t/"
    CUTOFF = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=3)

    def run(self, files, reachable=frozenset(), exclude=()):
        return filter_candidates(files, set(reachable), self.LOC, self.CUTOFF, exclude)

    def test_reachable_files_never_candidates(self):
        candidates, _ = self.run(
            [orphan("/wh/t/data/live.parquet")], reachable={"/wh/t/data/live.parquet"}
        )
        assert candidates == []

    def test_young_files_skipped(self):
        candidates, skipped = self.run([orphan("/wh/t/data/f.parquet", mtime=NOW)])
        assert candidates == [] and skipped["young"] == 1

    def test_unknown_mtime_treated_as_young(self):
        candidates, skipped = self.run([OrphanFile(path="/wh/t/data/f.parquet")])
        assert candidates == [] and skipped["young"] == 1

    def test_metadata_json_hard_protected_even_when_old_and_unreachable(self):
        files = [
            orphan("/wh/t/metadata/00001-abc.metadata.json"),
            orphan("/wh/t/metadata/version-hint.text"),
        ]
        candidates, skipped = self.run(files)
        assert candidates == [] and skipped["metadata-json"] == 2

    def test_exclude_globs(self):
        candidates, skipped = self.run(
            [orphan("/wh/t/data/_SUCCESS"), orphan("/wh/t/data/f.parquet")],
            exclude=("_SUCCESS",),
        )
        assert [c.path for c in candidates] == ["/wh/t/data/f.parquet"]
        assert skipped["excluded"] == 1

    def test_out_of_scope_skipped(self):
        candidates, skipped = self.run([orphan("/other/place/f.parquet")])
        assert candidates == [] and skipped["out-of-scope"] == 1

    def test_true_orphan_passes_all_stages(self):
        candidates, _ = self.run([orphan("/wh/t/data/dead.parquet")])
        assert [c.path for c in candidates] == ["/wh/t/data/dead.parquet"]
