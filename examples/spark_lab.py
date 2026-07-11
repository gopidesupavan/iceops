"""Build a local Spark + Iceberg compaction lab for iceops.

This is a real Spark/Iceberg setup:
- Spark writes an Iceberg table through Iceberg's JDBC catalog.
- PyIceberg reads the same SQLite-backed catalog.
- `iceops compact --engine spark` submits Spark's `rewrite_data_files` procedure.

Run:
  uv sync --extra spark
  uv run python examples/spark_lab.py

Then follow the printed commands.
"""

from __future__ import annotations

import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LAB_ROOT = ROOT / "spark_lab_warehouse"
CONFIG = ROOT / ".iceops.spark-lab.toml"
CATALOG = "sparklab"
TABLE = "db.events"
ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0"
SQLITE_JDBC = "org.xerial:sqlite-jdbc:3.53.2.0"


def main() -> None:
    if LAB_ROOT.exists():
        shutil.rmtree(LAB_ROOT)
    LAB_ROOT.mkdir(parents=True)

    warehouse = LAB_ROOT / "warehouse"
    catalog_db = LAB_ROOT / "catalog.db"

    spark = _spark(warehouse, catalog_db)
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.db")
        spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{TABLE}")
        spark.sql(
            f"CREATE TABLE {CATALOG}.{TABLE} "
            "(event_id BIGINT, user_id BIGINT, event_type STRING) USING iceberg"
        )
        for i in range(12):
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
    finally:
        spark.stop()

    _write_config(warehouse, catalog_db)

    print(f"Spark lab ready: {LAB_ROOT}")
    print(f"Seeded {CATALOG}.{TABLE}: {count} rows, {files} data files")
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


def _spark(warehouse: Path, catalog_db: Path):
    from pyspark.sql import SparkSession

    packages = f"{ICEBERG_RUNTIME},{SQLITE_JDBC}"
    return (
        SparkSession.builder.appName("iceops-spark-lab")
        .master("local[2]")
        .config("spark.jars.packages", packages)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.uri", f"jdbc:sqlite:{catalog_db}")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse.as_uri())
        .config(f"spark.sql.catalog.{CATALOG}.jdbc.schema-version", "V1")
        .getOrCreate()
    )


def _write_config(warehouse: Path, catalog_db: Path) -> None:
    packages = f"{ICEBERG_RUNTIME},{SQLITE_JDBC}"
    CONFIG.write_text(
        f"""[catalogs.{CATALOG}]
type = "sql"
uri = "sqlite:///{catalog_db}"
warehouse = "{warehouse.as_uri()}"

[engines.spark]
master = "local[2]"
"spark.jars.packages" = "{packages}"
"spark.sql.extensions" = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
"spark.sql.catalog.{CATALOG}" = "org.apache.iceberg.spark.SparkCatalog"
"spark.sql.catalog.{CATALOG}.catalog-impl" = "org.apache.iceberg.jdbc.JdbcCatalog"
"spark.sql.catalog.{CATALOG}.uri" = "jdbc:sqlite:{catalog_db}"
"spark.sql.catalog.{CATALOG}.warehouse" = "{warehouse.as_uri()}"
"spark.sql.catalog.{CATALOG}.jdbc.schema-version" = "V1"
"""
    )


if __name__ == "__main__":
    main()
