from __future__ import annotations

from ..errors import NotYetImplemented


def tune(*args: object, **kwargs: object) -> None:
    """v0.2: run the right fixes in the right order — compact, then expire, then
    clean-orphans — so users can't corrupt tables by sequencing maintenance wrong."""
    raise NotYetImplemented("tune", "v0.2")
