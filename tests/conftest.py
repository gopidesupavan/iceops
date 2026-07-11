"""Shared fixtures: a REAL SQLite-backed Iceberg catalog with seeded tables.

House rule — no mocks in integration tests, ever. Hard scenarios are constructed
physically (real files planted on disk, real commits racing the operator), because a
mock encodes the author's assumption while reality is what finds the bug (the
clean-orphans race test proved this pre-ship). tests/unit needs no mocks by
construction: pure functions over plain data. monkeypatch is acceptable only for
environment setup (env vars, cwd), never for faking behavior.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pyarrow as pa
import pytest
from pyiceberg.catalog import Catalog, load_catalog

MESSY_APPENDS = 25
HEALTHY_APPENDS = 2


def _batch(rows: int, start: int = 0) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(range(start, start + rows), type=pa.int64()),
            "value": pa.array([float(i) for i in range(rows)], type=pa.float64()),
            "label": pa.array([f"row-{i}" for i in range(rows)]),
        }
    )


@pytest.fixture(scope="session")
def warehouse(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("warehouse")


@pytest.fixture(scope="session")
def seeded_catalog(warehouse: Path) -> Catalog:
    """A local SQLite catalog with one messy table and one healthy table."""
    catalog = load_catalog(
        "test",
        type="sql",
        uri=f"sqlite:///{warehouse}/catalog.db",
        warehouse=f"file://{warehouse}",
    )
    catalog.create_namespace("db")

    messy = catalog.create_table("db.messy", schema=_batch(1).schema)
    for i in range(MESSY_APPENDS):
        messy.append(_batch(100, i * 100))

    healthy = catalog.create_table("db.healthy", schema=_batch(1).schema)
    for i in range(HEALTHY_APPENDS):
        healthy.append(_batch(1000, i * 1000))

    # plant an orphan next to messy's data files
    data_dirs = [p for p in warehouse.rglob("data") if p.is_dir() and "messy" in str(p)]
    if data_dirs:
        parquet = sorted(data_dirs[0].glob("*.parquet"))
        if parquet:
            orphan = data_dirs[0] / "00000-0-orphan.parquet"
            shutil.copy(parquet[0], orphan)
            with orphan.open("ab") as fh:
                fh.write(b"\0" * 2 * 1024 * 1024)

    return catalog


@pytest.fixture(scope="session")
def iceops_config(warehouse: Path, tmp_path_factory: pytest.TempPathFactory) -> Path:
    """An iceops config file pointing at the seeded catalog, for CLI tests."""
    config = tmp_path_factory.mktemp("config") / "iceops.toml"
    config.write_text(
        "[catalogs.test]\n"
        'type = "sql"\n'
        f'uri = "sqlite:///{warehouse}/catalog.db"\n'
        f'warehouse = "file://{warehouse}"\n'
    )
    return config
