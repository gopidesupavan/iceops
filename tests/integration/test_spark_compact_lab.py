from __future__ import annotations

import os
from pathlib import Path

import pytest
from pyiceberg.catalog import load_catalog

from iceops.operators import compact

ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0"
SQLITE_JDBC = "org.xerial:sqlite-jdbc:3.53.2.0"


pytestmark = pytest.mark.skipif(
    os.environ.get("ICEOPS_RUN_SPARK") != "1",
    reason="set ICEOPS_RUN_SPARK=1 to run the local Spark/Iceberg compaction lab",
)


def test_spark_compact_rewrites_real_iceberg_table(tmp_path: Path):
    from pyspark.sql import SparkSession

    catalog_name = "sparklabtest"
    warehouse = tmp_path / "warehouse"
    catalog_db = tmp_path / "catalog.db"
    packages = f"{ICEBERG_RUNTIME},{SQLITE_JDBC}"

    spark = (
        SparkSession.builder.appName("iceops-spark-compact-test")
        .master("local[2]")
        .config("spark.jars.packages", packages)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            f"spark.sql.catalog.{catalog_name}.catalog-impl",
            "org.apache.iceberg.jdbc.JdbcCatalog",
        )
        .config(f"spark.sql.catalog.{catalog_name}.uri", f"jdbc:sqlite:{catalog_db}")
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse.as_uri())
        .config(f"spark.sql.catalog.{catalog_name}.jdbc.schema-version", "V1")
        .getOrCreate()
    )
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.db")
        spark.sql(f"CREATE TABLE {catalog_name}.db.events (id BIGINT, label STRING) USING iceberg")
        for i in range(6):
            (
                spark.range(i * 100, (i + 1) * 100)
                .selectExpr("id", "concat('row-', id) as label")
                .repartition(1)
                .writeTo(f"{catalog_name}.db.events")
                .append()
            )
    finally:
        spark.stop()

    catalog = load_catalog(
        catalog_name,
        type="sql",
        uri=f"sqlite:///{catalog_db}",
        warehouse=warehouse.as_uri(),
    )
    table = catalog.load_table("db.events")
    assert table.scan().to_arrow().num_rows == 600
    assert table.inspect.files().num_rows == 6

    result = compact(
        catalog,
        "db.events",
        engine="spark",
        engine_catalog=catalog_name,
        execute=True,
        engine_config={
            "master": "local[2]",
            "spark.jars.packages": packages,
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{catalog_name}.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
            f"spark.sql.catalog.{catalog_name}.uri": f"jdbc:sqlite:{catalog_db}",
            f"spark.sql.catalog.{catalog_name}.warehouse": warehouse.as_uri(),
            f"spark.sql.catalog.{catalog_name}.jdbc.schema-version": "V1",
        },
    )

    table.refresh()
    assert result.data_files_before == 6
    assert result.data_files_after == 1
    assert result.action_results[0].details["row_0"]["rewritten_data_files_count"] == 6
    assert result.action_results[0].details["row_0"]["added_data_files_count"] == 1
    assert table.inspect.files().num_rows == 1
    assert table.scan().to_arrow().num_rows == 600
