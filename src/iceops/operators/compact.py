from __future__ import annotations

from ..errors import NotYetImplemented


def compact(*args: object, **kwargs: object) -> None:
    """v0.2: native bin-pack compaction for append-only tables (Arrow, in-process);
    tables with delete files route to the Spark/Trino escape hatch."""
    raise NotYetImplemented("compact", "v0.2")
