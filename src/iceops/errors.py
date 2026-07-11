"""iceops exceptions."""

from __future__ import annotations


class IceopsError(Exception):
    """Base class for all iceops errors."""


class CatalogProfileError(IceopsError):
    """A catalog profile is missing or misconfigured."""


class TableNotFoundError(IceopsError):
    """The requested table does not exist in the catalog."""


class NotYetImplemented(IceopsError):
    """The operation has a home in iceops but its body ships in a later version."""

    def __init__(self, op: str, version: str) -> None:
        self.op = op
        self.version = version
        super().__init__(
            f"'{op}' is on the iceops roadmap for {version}. "
            f"v0.1 is the read-only diagnose slice (scan / doctor / cost) — "
            f"see https://github.com/gopidesupavan/iceops#what-works-today"
        )
