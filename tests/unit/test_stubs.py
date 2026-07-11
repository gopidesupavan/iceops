from __future__ import annotations

import pytest

from iceops.errors import NotYetImplemented
from iceops.operators import compact, tune


def test_fix_operators_are_explicit_stubs():
    for op in (compact, tune):
        with pytest.raises(NotYetImplemented):
            op()
