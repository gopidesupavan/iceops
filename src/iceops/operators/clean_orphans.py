from __future__ import annotations

from ..errors import NotYetImplemented


def clean_orphans(*args: object, **kwargs: object) -> None:
    """v0.2: orphan-file cleanup with age threshold, exclusion patterns, and a manifest
    re-check before every delete batch."""
    raise NotYetImplemented("clean-orphans", "v0.2")
