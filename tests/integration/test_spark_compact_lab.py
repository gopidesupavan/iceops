from __future__ import annotations

import os
from pathlib import Path

import pytest
from pyiceberg.catalog import load_catalog

from iceops.operators import compact

SQLITE_JDBC = "org.xerial:sqlite-jdbc:3.53.2.0"


def _iceberg_runtime() -> str:
    """Match the Iceberg Spark runtime to the installed PySpark's major.minor — a
    mismatch (e.g. Spark 4.1 with the 4.0 runtime) throws NoSuchMethodError deep inside
    the metadata-table procedures (rewrite_manifests/expire/remove_orphan_files)."""
    import pyspark

    major_minor = ".".join(pyspark.__version__.split(".")[:2])
    return f"org.apache.iceberg:iceberg-spark-runtime-{major_minor}_2.13:1.11.0"


pytestmark = pytest.mark.skipif(
    os.environ.get("ICEOPS_RUN_SPARK") != "1",
    reason="set ICEOPS_RUN_SPARK=1 to run the local Spark/Iceberg compaction lab",
)


def test_spark_compact_rewrites_real_iceberg_table(tmp_path: Path):
    from pyspark.sql import SparkSession

    catalog_name = "sparklabtest"
    warehouse = tmp_path / "warehouse"
    catalog_db = tmp_path / "catalog.db"
    packages = f"{_iceberg_runtime()},{SQLITE_JDBC}"

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


def test_spark_backs_expire_rewrite_and_clean_orphans(tmp_path: Path):
    """Every non-compact fix operator, delegated to a real Spark, in one session."""
    import datetime as dt

    from pyspark.sql import SparkSession

    from iceops.operators import clean_orphans, expire, rewrite_manifests

    catalog_name = "sparkmaint"
    warehouse = tmp_path / "warehouse"
    catalog_db = tmp_path / "catalog.db"
    packages = f"{_iceberg_runtime()},{SQLITE_JDBC}"

    spark = (
        SparkSession.builder.appName("iceops-spark-maint-test")
        .master("local[2]")
        .config("spark.jars.packages", packages)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog"
        )
        .config(f"spark.sql.catalog.{catalog_name}.uri", f"jdbc:sqlite:{catalog_db}")
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse.as_uri())
        .config(f"spark.sql.catalog.{catalog_name}.jdbc.schema-version", "V1")
        .getOrCreate()
    )
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.db")
        spark.sql(f"CREATE TABLE {catalog_name}.db.t (id BIGINT) USING iceberg")
        for i in range(6):  # 6 snapshots, 6 manifests
            spark.range(i * 50, (i + 1) * 50).writeTo(f"{catalog_name}.db.t").append()

        catalog = load_catalog(
            catalog_name, type="sql", uri=f"sqlite:///{catalog_db}", warehouse=warehouse.as_uri()
        )
        cfg = {"session": spark}
        assert catalog.load_table("db.t").inspect.manifests().num_rows == 6
        assert catalog.load_table("db.t").scan().to_arrow().num_rows == 300

        # rewrite-manifests via Spark
        rr = rewrite_manifests(
            catalog,
            "db.t",
            engine="spark",
            engine_catalog=catalog_name,
            engine_config=cfg,
            execute=True,
        )
        assert rr.manifests_after < rr.manifests_before

        # expire via Spark (retain 1, older_than 0)
        er = expire(
            catalog,
            "db.t",
            retain_last=1,
            older_than=dt.timedelta(0),
            engine="spark",
            engine_catalog=catalog_name,
            engine_config=cfg,
            execute=True,
        )
        assert er.snapshot_count_after < er.plan.snapshot_count

        # clean-orphans via Spark — plant a REAL orphan and prove Spark deletes it.
        # Spark hardcodes a 24h minimum for remove_orphan_files (no SQL override, unlike
        # Trino's configurable min-retention), so the orphan is backdated past that window.
        import os
        import shutil
        from urllib.parse import urlparse

        data_dir = Path(urlparse(catalog.load_table("db.t").location()).path) / "data"
        live = sorted(data_dir.glob("*.parquet"))[0]
        orphan = data_dir / "00000-0-planted-orphan.parquet"
        shutil.copy(live, orphan)
        two_days_ago = (dt.datetime.now() - dt.timedelta(days=2)).timestamp()
        os.utime(orphan, (two_days_ago, two_days_ago))
        assert orphan.exists()

        cr = clean_orphans(
            catalog,
            "db.t",
            older_than=dt.timedelta(hours=25),
            engine="spark",
            engine_catalog=catalog_name,
            engine_config=cfg,
            execute=True,
        )
        assert cr.status == "cleaned"
        # Spark actually removed the planted orphan…
        assert not orphan.exists(), "Spark remove_orphan_files did not delete the planted orphan"
        # …and left every referenced live file + all rows intact
        assert live.exists()
        assert catalog.load_table("db.t").scan().to_arrow().num_rows == 300
    finally:
        spark.stop()
