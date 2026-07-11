"""Catalog connectivity: iceops profiles on top of PyIceberg catalogs."""

from __future__ import annotations

import fnmatch
from typing import TYPE_CHECKING

from pyiceberg.catalog import load_catalog

from ..config import get_profile
from ..errors import CatalogProfileError

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog


def connect(name: str) -> "Catalog":
    """Load a catalog by iceops profile name, falling back to PyIceberg's own config."""
    props = get_profile(name)
    try:
        if props is not None:
            return load_catalog(name, **{k: str(v) for k, v in props.items()})
        return load_catalog(name)
    except Exception as exc:
        raise CatalogProfileError(
            f"could not connect to catalog '{name}': {exc}. "
            f"Define it under [catalogs.{name}] in .iceops.toml or ~/.iceops/config.toml"
        ) from exc


def list_table_identifiers(catalog: "Catalog", pattern: str = "*") -> list[str]:
    """All 'ns.table' identifiers in the catalog matching a glob pattern."""
    identifiers: list[str] = []
    for namespace in _all_namespaces(catalog):
        for ident in catalog.list_tables(namespace):
            dotted = ".".join(ident)
            if fnmatch.fnmatch(dotted, pattern):
                identifiers.append(dotted)
    return sorted(identifiers)


def _all_namespaces(catalog: "Catalog", parent: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
    namespaces: list[tuple[str, ...]] = []
    for child in catalog.list_namespaces(parent) if parent else catalog.list_namespaces():
        child_tuple = tuple(child)
        namespaces.append(child_tuple)
        try:
            namespaces.extend(_all_namespaces(catalog, child_tuple))
        except Exception:
            # Some catalogs reject nested namespace listing; the flat level is enough.
            pass
    return namespaces
