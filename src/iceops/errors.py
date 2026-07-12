"""iceops exceptions."""

from __future__ import annotations


class IceopsError(Exception):
    """Base class for all iceops errors."""


class CatalogProfileError(IceopsError):
    """A catalog profile is missing or misconfigured."""


class TableNotFoundError(IceopsError):
    """The requested table does not exist in the catalog."""


class NotYetImplemented(IceopsError):
    """The operation has a home in iceops but is not implemented yet."""

    def __init__(self, op: str) -> None:
        self.op = op
        super().__init__(
            f"'{op}' is not implemented yet — "
            f"see https://github.com/gopidesupavan/iceops#what-works-today"
        )
