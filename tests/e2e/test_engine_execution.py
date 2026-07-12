"""Production-like e2e: the REAL iceops binary running maintenance THROUGH a real engine.

Not dry-run. This spawns `iceops <op> --engine spark --yes` as a subprocess with the
engine configured in .iceops.toml — exercising the entire production path (arg parsing,
load_engine_config, SparkEngine session build, procedure submission) and asserting the
actual mutation happened. Gated by ICEOPS_RUN_SPARK (needs Java + PySpark).
"""

from __future__ import annotations

import datetime as dt
import os
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pytest
from pyiceberg.catalog import load_catalog

pytestmark = pytest.mark.skipif(
    os.environ.get("ICEOPS_RUN_SPARK") != "1",
    reason="set ICEOPS_RUN_SPARK=1 to run the real-engine CLI execution e2e",
)

ICEOPS_BIN = shutil.which("iceops")
SQLITE_JDBC = "org.xerial:sqlite-jdbc:3.53.2.0"


def _iceberg_runtime() -> str:
    import pyspark

    mm = ".".join(pyspark.__version__.split(".")[:2])
    return f"org.apache.iceberg:iceberg-spark-runtime-{mm}_2.13:1.11.0"


def iceops(*args: str, cwd: Path) -> subprocess.CompletedProcess:
    cmd = [ICEOPS_BIN, *args] if ICEOPS_BIN else [sys.executable, "-m", "iceops.cli.app", *args]
    env = {k: v for k, v in os.environ.items() if k != "ICEOPS_CONFIG"}
    # Spark session build + jar resolution is slow; allow generous time
    result = subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True, timeout=300)
    print(f"\n$ iceops {' '.join(args)}   [exit {result.returncode}]")
    if result.stdout.strip():
        print(result.stdout.rstrip())
    if result.stderr.strip():
        print(f"[stderr] {result.stderr.rstrip()}")
    return result


def test_iceops_binary_runs_full_maintenance_through_spark(tmp_path: Path):
    warehouse = tmp_path / "warehouse"
    catalog_db = tmp_path / "catalog.db"
    packages = f"{_iceberg_runtime()},{SQLITE_JDBC}"
    cat = "lab"

    # setup via PyIceberg (the writer); Spark will be the maintainer through the CLI
    catalog = load_catalog(
        cat, type="sql", uri=f"sqlite:///{catalog_db}", warehouse=warehouse.as_uri()
    )
    catalog.create_namespace("db")
    b = pa.table({"id": pa.array(range(50), type=pa.int64())})
    table = catalog.create_table("db.t", schema=b.schema)
    for _ in range(6):  # 6 snapshots, 6 manifests
        table.append(b)

    # .iceops.toml wires BOTH the catalog profile and the spark engine, dotted keys quoted
    engine_lines = "\n".join(
        f'"{k}" = "{v}"'
        for k, v in {
            "spark.jars.packages": packages,
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            f"spark.sql.catalog.{cat}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{cat}.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
            f"spark.sql.catalog.{cat}.uri": f"jdbc:sqlite:{catalog_db}",
            f"spark.sql.catalog.{cat}.warehouse": warehouse.as_uri(),
            f"spark.sql.catalog.{cat}.jdbc.schema-version": "V1",
        }.items()
    )
    (tmp_path / ".iceops.toml").write_text(
        f"[catalogs.{cat}]\n"
        'type = "sql"\n'
        f'uri = "sqlite:///{catalog_db}"\n'
        f'warehouse = "{warehouse.as_uri()}"\n\n'
        "[engines.spark]\n"
        'master = "local[2]"\n'
        f"{engine_lines}\n"
    )

    base = ["--catalog", cat, "--engine", "spark", "--engine-catalog", cat, "--yes"]

    # 1. rewrite-manifests THROUGH the binary THROUGH real Spark
    r = iceops("rewrite-manifests", "db.t", *base, cwd=tmp_path)
    assert r.returncode == 0, r.stdout + r.stderr
    assert catalog.load_table("db.t").inspect.manifests().num_rows < 6  # really consolidated

    # 2. expire through the binary through real Spark
    before = len((catalog.load_table("db.t").metadata.snapshots) or [])
    r = iceops("expire", "db.t", "--retain-last", "1", "--older-than", "0s", *base, cwd=tmp_path)
    assert r.returncode == 0, r.stdout + r.stderr
    assert len(catalog.load_table("db.t").metadata.snapshots or []) < before  # really expired

    # 3. clean-orphans through the binary through real Spark — plant a real, aged orphan
    data_dir = Path(urlparse(catalog.load_table("db.t").location()).path) / "data"
    live = sorted(data_dir.glob("*.parquet"))[0]
    orphan = data_dir / "00000-0-e2e-orphan.parquet"
    shutil.copy(live, orphan)
    two_days = (dt.datetime.now() - dt.timedelta(days=2)).timestamp()
    os.utime(orphan, (two_days, two_days))

    r = iceops("clean-orphans", "db.t", "--older-than", "25h", *base, cwd=tmp_path)
    assert r.returncode == 0, r.stdout + r.stderr
    assert not orphan.exists(), "clean-orphans --engine spark --yes did not delete the orphan"

    # every mutation happened through the real CLI + real Spark, and data survived
    assert catalog.load_table("db.t").scan().to_arrow().num_rows == 300
