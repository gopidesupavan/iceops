from __future__ import annotations

import pytest

from iceops.models import parse_size
from iceops.operators.rewrite_manifests import estimate_after


class TestParseSize:
    def test_units(self):
        assert parse_size("512b") == 512
        assert parse_size("64KB") == 64 * 1024
        assert parse_size("8MB") == 8 * 1024**2
        assert parse_size("2gb") == 2 * 1024**3

    def test_rejects_garbage(self):
        for bad in ("8", "MB8", "8 megabytes", "", "-8MB", "8TB"):
            with pytest.raises(ValueError):
                parse_size(bad)


class TestEstimateAfter:
    def test_single_group_bin_packs(self):
        assert estimate_after({0: [1000] * 10}, target=100_000) == 1
        assert estimate_after({0: [60_000] * 10}, target=100_000) == 6

    def test_groups_never_merge_across_specs(self):
        assert estimate_after({0: [1000], 1: [1000]}, target=100_000) == 2
