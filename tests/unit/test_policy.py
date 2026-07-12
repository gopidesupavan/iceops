from __future__ import annotations

import pytest

from iceops.errors import IceopsError
from iceops.models import TableMetrics
from iceops.policy import parse_when, resolve, validate_policy
from iceops.policy.schema import PolicyDoc


def m(**kw) -> TableMetrics:
    return TableMetrics(identifier="db.t", **kw)


class TestParseWhen:
    def test_operators(self):
        assert parse_when("small-file-ratio > 0.3").evaluate(m(small_file_ratio=0.5))
        assert not parse_when("small-file-ratio > 0.3").evaluate(m(small_file_ratio=0.2))
        assert parse_when("manifest-count >= 50").evaluate(m(manifest_count=50))
        assert parse_when("snapshot-count < 10").evaluate(m(snapshot_count=9))
        assert parse_when("delete-ratio == 0").evaluate(m(delete_ratio=0.0))

    def test_unknown_metric_rejected_at_parse(self):
        with pytest.raises(IceopsError, match="unknown metric 'bogus-metric'"):
            parse_when("bogus-metric > 1")

    def test_bad_syntax_rejected(self):
        for bad in ("small-file-ratio", "ratio ~ 3", "> 3", "small-file-ratio > "):
            with pytest.raises(IceopsError, match="invalid when|unknown metric"):
                parse_when(bad)

    def test_none_valued_metric_is_false(self):
        # partition_file_skew is Optional and None by default → never act on unknown
        assert not parse_when("partition-file-skew > 5").evaluate(m())


class TestResolvePerTable:
    def _doc(self):
        return PolicyDoc.model_validate(
            {
                "defaults": {
                    "expire-snapshots": {"retain-last": 10, "older-than": "7d"},
                    "clean-orphans": {"older-than": "3d"},
                },
                "tables": {
                    "db.events": {"expire-snapshots": {"retain-last": 50}},
                    "db.hot_*": {"engine": "spark", "clean-orphans": {"older-than": "1d"}},
                    "db.audit": {"disabled": True},
                },
            }
        )

    def test_defaults_apply_to_unlisted_table(self):
        r = resolve(self._doc(), "db.random")
        assert r is not None
        assert r.spec.expire_snapshots.retain_last == 10

    def test_per_table_override_merges_field_by_field(self):
        r = resolve(self._doc(), "db.events")
        assert r.spec.expire_snapshots.retain_last == 50  # overridden
        assert r.spec.expire_snapshots.older_than == "7d"  # kept from defaults

    def test_glob_match_and_engine_resolution(self):
        r = resolve(self._doc(), "db.hot_orders")
        assert r.engine == "spark"
        assert r.spec.clean_orphans.older_than == "1d"

    def test_disabled_table_returns_none(self):
        assert resolve(self._doc(), "db.audit") is None

    def test_no_defaults_and_no_match_is_out_of_scope(self):
        doc = PolicyDoc.model_validate({"tables": {"db.events": {"clean-orphans": {}}}})
        assert resolve(doc, "db.other") is None  # not in scope
        assert resolve(doc, "db.events") is not None

    def test_exact_beats_glob_specificity(self):
        doc = PolicyDoc.model_validate(
            {
                "tables": {
                    "db.*": {"expire-snapshots": {"retain-last": 5}},
                    "db.events": {"expire-snapshots": {"retain-last": 99}},
                }
            }
        )
        assert resolve(doc, "db.events").spec.expire_snapshots.retain_last == 99


class TestValidatePolicy:
    def test_unknown_metric_in_when_fails_at_load(self):
        doc = PolicyDoc.model_validate({"defaults": {"compact": {"when": "not-a-metric > 1"}}})
        with pytest.raises(IceopsError, match="unknown metric"):
            validate_policy(doc)

    def test_valid_policy_passes(self):
        doc = PolicyDoc.model_validate(
            {"defaults": {"compact": {"when": "small-file-ratio > 0.3"}}}
        )
        validate_policy(doc)  # no raise
