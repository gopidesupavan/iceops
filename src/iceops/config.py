"""Catalog profile loading.

Search order for profiles:
1. ``$ICEOPS_CONFIG`` (explicit path)
2. ``./.iceops.toml`` (project-local)
3. ``~/.iceops/config.toml`` (user)

A profile is a ``[catalogs.<name>]`` TOML table whose keys are passed straight to
``pyiceberg.catalog.load_catalog``. Names not found in any iceops config fall through to
PyIceberg's own configuration (~/.pyiceberg.yaml, env vars), so anything PyIceberg can
reach, iceops can reach.
"""

from __future__ import annotations

import os
import sys

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib
from pathlib import Path
from typing import Any

from .errors import CatalogProfileError

ENV_CONFIG = "ICEOPS_CONFIG"
PROJECT_CONFIG = ".iceops.toml"
USER_CONFIG = Path.home() / ".iceops" / "config.toml"


def config_paths() -> list[Path]:
    paths: list[Path] = []
    env = os.environ.get(ENV_CONFIG)
    if env:
        paths.append(Path(env))
    paths.append(Path.cwd() / PROJECT_CONFIG)
    paths.append(USER_CONFIG)
    return paths


def load_profiles() -> dict[str, dict[str, Any]]:
    """Merge profiles from all config files; earlier paths win on name clashes."""
    profiles: dict[str, dict[str, Any]] = {}
    for path in reversed(config_paths()):
        if not path.is_file():
            continue
        try:
            doc = tomllib.loads(path.read_text())
        except tomllib.TOMLDecodeError as exc:
            raise CatalogProfileError(f"invalid TOML in {path}: {exc}") from exc
        for name, props in doc.get("catalogs", {}).items():
            if not isinstance(props, dict):
                raise CatalogProfileError(f"[catalogs.{name}] in {path} must be a table")
            profiles[name] = props
    return profiles


def get_profile(name: str) -> dict[str, Any] | None:
    return load_profiles().get(name)


def default_catalog_name() -> str | None:
    """If exactly one profile is configured, use it without requiring --catalog."""
    profiles = load_profiles()
    if len(profiles) == 1:
        return next(iter(profiles))
    return None
