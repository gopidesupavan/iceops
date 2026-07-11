from __future__ import annotations

import datetime as dt

import pytest

from iceops.models import parse_duration
from iceops.operators.expire import select_candidates

NOW_MS = 1_800_000_000_000
HOUR_MS = 3_600_000


def snap(i: int, hours_ago: int) -> tuple[int, int]:
    return (i, NOW_MS - hours_ago * HOUR_MS)


class TestSelectCandidates:
    def test_protected_never_expired(self):
        snapshots = [snap(1, 100), snap(2, 50), snap(3, 0)]
        out = select_candidates(snapshots, protected_ids={1}, retain_last=1, cutoff_ms=NOW_MS)
        assert 1 not in out and out == [2]

    def test_retain_last_keeps_newest_regardless_of_age(self):
        snapshots = [snap(i, 100 - i) for i in range(1, 6)]  # 5 old snapshots
        out = select_candidates(snapshots, protected_ids=set(), retain_last=3, cutoff_ms=NOW_MS)
        assert sorted(out) == [1, 2]  # newest three (3,4,5) survive on count alone

    def test_cutoff_keeps_young_regardless_of_count(self):
        snapshots = [snap(1, 100), snap(2, 1), snap(3, 0)]
        cutoff = NOW_MS - 48 * HOUR_MS  # older than 2 days only
        out = select_candidates(snapshots, protected_ids=set(), retain_last=0, cutoff_ms=cutoff)
        assert out == [1]

    def test_both_conditions_required(self):
        # beyond retain-last but younger than cutoff -> kept
        snapshots = [snap(1, 3), snap(2, 2), snap(3, 1)]
        cutoff = NOW_MS - 10 * HOUR_MS
        assert select_candidates(snapshots, set(), retain_last=1, cutoff_ms=cutoff) == []


class TestParseDuration:
    def test_units(self):
        assert parse_duration("30s") == dt.timedelta(seconds=30)
        assert parse_duration("12h") == dt.timedelta(hours=12)
        assert parse_duration("7d") == dt.timedelta(days=7)
        assert parse_duration("2w") == dt.timedelta(weeks=2)

    def test_rejects_garbage(self):
        for bad in ("7", "d7", "7 days", "", "-3d"):
            with pytest.raises(ValueError):
                parse_duration(bad)
