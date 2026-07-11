"""Build a local Spark Connect + Iceberg compaction lab for iceops.

This verifies the PySpark Connect client path:
- Spark Connect local mode writes an Iceberg table through Iceberg's JDBC catalog.
- PyIceberg reads the same SQLite-backed catalog.
- `iceops compact --engine spark` connects with `remote_uri = "local[2]"`.

Run:
  uv sync --extra spark
  uv run python examples/spark_connect_lab.py

Then follow the printed commands.
"""

from __future__ import annotations

import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LAB_ROOT = ROOT / "spark_connect_lab_warehouse"
CONFIG = ROOT / ".iceops.spark-connect-lab.toml"
CATALOG = "sparkconnectlab"
TABLE = "db.events"
ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0"
SQLITE_JDBC = "org.xerial:sqlite-jdbc:3.53.2.0"


def main() -> None:
    if LAB_ROOT.exists():
        shutil.rmtree(LAB_ROOT)
    LAB_ROOT.mkdir(parents=True)

    warehouse = LAB_ROOT / "warehouse"
    catalog_db = LAB_ROOT / "catalog.db"

    spark = _spark_connect(warehouse, catalog_db)
    try:
        from pyspark.sql import is_remote

        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.db").collect()
        spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{TABLE}").collect()
        spark.sql(
            f"CREATE TABLE {CATALOG}.{TABLE} "
            "(event_id BIGINT, user_id BIGINT, event_type STRING) USING iceberg"
        ).collect()
        for i in range(8):
            start = i * 250
            (
                spark.range(start, start + 250)
                .selectExpr(
                    "id as event_id",
                    "id % 37 as user_id",
                    "case when id % 3 = 0 then 'click' "
                    "when id % 3 = 1 then 'view' else 'purchase' end as event_type",
                )
                .repartition(1)
                .writeTo(f"{CATALOG}.{TABLE}")
                .append()
            )
        count = spark.sql(f"SELECT count(*) AS c FROM {CATALOG}.{TABLE}").collect()[0]["c"]
        files = spark.sql(f"SELECT count(*) AS c FROM {CATALOG}.{TABLE}.files").collect()[0]["c"]
        remote = is_remote()
    finally:
        spark.stop()

    _write_config(warehouse, catalog_db)

    print(f"Spark Connect lab ready: {LAB_ROOT}")
    print(f"Seeded {CATALOG}.{TABLE}: {count} rows, {files} data files")
    print(f"Spark session was remote: {remote}")
    print(f"Config written: {CONFIG}")
    print("")
    print("Try:")
    print(f"  ICEOPS_CONFIG={CONFIG} uv run iceops doctor {TABLE} --catalog {CATALOG}")
    print(
        f"  ICEOPS_CONFIG={CONFIG} uv run iceops compact {TABLE} --catalog {CATALOG} --engine spark"
    )
    print(
        f"  ICEOPS_CONFIG={CONFIG} uv run iceops compact {TABLE} "
        f"--catalog {CATALOG} --engine spark --yes"
    )
    print(f"  ICEOPS_CONFIG={CONFIG} uv run iceops doctor {TABLE} --catalog {CATALOG}")


def _spark_connect(warehouse: Path, catalog_db: Path):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName("iceops-spark-connect-lab")
    for key, value in _spark_conf(warehouse, catalog_db).items():
        builder = builder.config(key, value)
    return builder.remote("local[2]").getOrCreate()


def _spark_conf(warehouse: Path, catalog_db: Path) -> dict[str, str]:
    packages = f"{ICEBERG_RUNTIME},{SQLITE_JDBC}"
    return {
        "spark.jars.packages": packages,
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        f"spark.sql.catalog.{CATALOG}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{CATALOG}.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
        f"spark.sql.catalog.{CATALOG}.uri": f"jdbc:sqlite:{catalog_db}",
        f"spark.sql.catalog.{CATALOG}.warehouse": warehouse.as_uri(),
        f"spark.sql.catalog.{CATALOG}.jdbc.schema-version": "V1",
    }


def _write_config(warehouse: Path, catalog_db: Path) -> None:
    conf = _spark_conf(warehouse, catalog_db)
    lines = [
        f"[catalogs.{CATALOG}]",
        'type = "sql"',
        f'uri = "sqlite:///{catalog_db}"',
        f'warehouse = "{warehouse.as_uri()}"',
        "",
        "[engines.spark]",
        'remote_uri = "local[2]"',
    ]
    for key, value in conf.items():
        lines.append(f'"{key}" = "{value}"')
    CONFIG.write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
