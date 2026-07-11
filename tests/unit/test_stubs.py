from __future__ import annotations

import pytest

from iceops.errors import NotYetImplemented
from iceops.operators import tune


def test_fix_operators_are_explicit_stubs():
    with pytest.raises(NotYetImplemented):
        tune()
