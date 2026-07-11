"""Manual Trino compaction lab against a live REST catalog + MinIO.

Mirrors examples/spark_lab.py for the engine that needs containers. Bring the stack up
yourself, then run this to watch `iceops compact --engine trino` work end to end:

    docker compose -f tests/integration/trino_stack/docker-compose.yml up -d
    uv run python examples/trino_lab.py
    docker compose -f tests/integration/trino_stack/docker-compose.yml down -v

Requires: uv sync --extra trino
"""

from __future__ import annotations

import pyarrow as pa
from pyiceberg.catalog import load_catalog

from iceops.operators import compact

REST_URI = "http://localhost:8181"
MINIO_ENDPOINT = "http://localhost:9000"


def main() -> None:
    catalog = load_catalog(
        "rest",
        type="rest",
        uri=REST_URI,
        warehouse="s3://warehouse/",
        **{
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
            "s3.access-key-id": "iceops",
            "s3.secret-access-key": "iceops-secret",
        },
    )
    for step in (lambda: catalog.drop_table("db.events"), lambda: catalog.create_namespace("db")):
        try:
            step()
        except Exception:
            pass

    batch = pa.table({"id": pa.array(range(100), type=pa.int64())})
    table = catalog.create_table("db.events", schema=batch.schema)
    for _ in range(8):
        table.append(batch)
    table = catalog.load_table("db.events")
    print(
        f"seeded: {table.inspect.files().num_rows} files, {table.scan().to_arrow().num_rows} rows"
    )

    result = compact(
        catalog,
        "db.events",
        engine="trino",
        engine_catalog="iceberg",
        execute=True,
        engine_config={"host": "localhost", "port": 8080, "user": "iceops"},
    )

    table = catalog.load_table("db.events")
    print(
        f"compacted via Trino: {result.data_files_before} -> {result.data_files_after} files, "
        f"{table.scan().to_arrow().num_rows} rows (row count preserved)"
    )


if __name__ == "__main__":
    main()
