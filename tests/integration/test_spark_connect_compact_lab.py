from __future__ import annotations

import os
from pathlib import Path

import pytest
from pyiceberg.catalog import load_catalog

from iceops.operators import compact

ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0"
SQLITE_JDBC = "org.xerial:sqlite-jdbc:3.53.2.0"


pytestmark = pytest.mark.skipif(
    os.environ.get("ICEOPS_RUN_SPARK_CONNECT") != "1",
    reason="set ICEOPS_RUN_SPARK_CONNECT=1 to run the Spark Connect compaction lab",
)


def test_spark_connect_compact_rewrites_real_iceberg_table(tmp_path: Path):
    from pyspark.sql import SparkSession, is_remote

    catalog_name = "sparkconnecttest"
    warehouse = tmp_path / "warehouse"
    catalog_db = tmp_path / "catalog.db"
    packages = f"{ICEBERG_RUNTIME},{SQLITE_JDBC}"
    conf = {
        "spark.jars.packages": packages,
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{catalog_name}.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
        f"spark.sql.catalog.{catalog_name}.uri": f"jdbc:sqlite:{catalog_db}",
        f"spark.sql.catalog.{catalog_name}.warehouse": warehouse.as_uri(),
        f"spark.sql.catalog.{catalog_name}.jdbc.schema-version": "V1",
    }

    builder = SparkSession.builder.appName("iceops-spark-connect-compact-test")
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.remote("local[2]").getOrCreate()
    try:
        assert is_remote()
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.db").collect()
        spark.sql(
            f"CREATE TABLE {catalog_name}.db.events (id BIGINT, label STRING) USING iceberg"
        ).collect()
        for i in range(4):
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
    assert table.scan().to_arrow().num_rows == 400
    assert table.inspect.files().num_rows == 4

    result = compact(
        catalog,
        "db.events",
        engine="spark",
        engine_catalog=catalog_name,
        execute=True,
        engine_config={"remote_uri": "local[2]", **conf},
    )

    table.refresh()
    assert result.data_files_before == 4
    assert result.data_files_after == 1
    assert result.action_results[0].details["row_0"]["rewritten_data_files_count"] == 4
    assert result.action_results[0].details["row_0"]["added_data_files_count"] == 1
    assert table.inspect.files().num_rows == 1
    assert table.scan().to_arrow().num_rows == 400
