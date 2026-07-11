"""Real Trino compaction against a live REST catalog + MinIO — no mocks.

Gated by ICEOPS_RUN_TRINO=1 (heavy: pulls/starts three containers). Verifies that
`iceops compact --engine trino` compacts a table PyIceberg wrote to object storage, that
row count is preserved (the safety-critical verify_row_count path on a second engine),
and that both engines read the same rows. Doubles as the project's first REST-catalog and
S3/MinIO exercise.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
import urllib.request
from pathlib import Path

import pyarrow as pa
import pytest
from pyiceberg.catalog import load_catalog

from iceops.operators import compact

STACK = Path(__file__).parent / "trino_stack"
REST_URI = "http://localhost:8181"
TRINO_HOST, TRINO_PORT = "localhost", 8080
MINIO_ENDPOINT = "http://localhost:9000"
CREDS = {"s3.access-key-id": "iceops", "s3.secret-access-key": "iceops-secret"}

pytestmark = pytest.mark.skipif(
    os.environ.get("ICEOPS_RUN_TRINO") != "1",
    reason="set ICEOPS_RUN_TRINO=1 to run the live Trino/REST/MinIO compaction lab",
)


def _compose(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", "-f", str(STACK / "docker-compose.yml"), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def _wait_ready(url: str, timeout: float = 120.0) -> None:
    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as resp:
                if resp.status == 200:
                    return
        except Exception as exc:  # not up yet
            last = str(exc)
        time.sleep(3)
    raise RuntimeError(f"service {url} not ready within {timeout}s (last: {last})")


def _wait_trino_ready(timeout: float = 180.0) -> None:
    """Trino returns 200 on /v1/info while still initializing — it reports readiness via
    the `starting` flag. Polling only for HTTP 200 races the SERVER_STARTING_UP error
    (found by a real run). Wait until starting == false."""
    url = f"http://{TRINO_HOST}:{TRINO_PORT}/v1/info"
    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as resp:
                info = json.loads(resp.read())
                if resp.status == 200 and not info.get("starting", True):
                    return
                last = f"starting={info.get('starting')}"
        except Exception as exc:
            last = str(exc)
        time.sleep(3)
    raise RuntimeError(f"trino not accepting queries within {timeout}s (last: {last})")


@pytest.fixture(scope="module")
def trino_stack():
    if shutil.which("docker") is None:
        pytest.skip("docker not available")
    up = _compose("up", "-d")
    if up.returncode != 0:
        pytest.fail(f"docker compose up failed:\n{up.stdout}\n{up.stderr}")
    try:
        _wait_ready(f"{REST_URI}/v1/config")
        _wait_trino_ready()
        yield
    finally:
        _compose("down", "-v")


def _rest_catalog():
    return load_catalog(
        "rest",
        type="rest",
        uri=REST_URI,
        warehouse="s3://warehouse/",
        **{
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
            **CREDS,
        },
    )


def test_trino_compacts_a_pyiceberg_written_table(trino_stack):
    catalog = _rest_catalog()
    try:
        catalog.drop_table("db.events")
    except Exception:
        pass
    try:
        catalog.create_namespace("db")
    except Exception:
        pass

    batch = pa.table({"id": pa.array(range(100), type=pa.int64())})
    table = catalog.create_table("db.events", schema=batch.schema)
    for _ in range(8):  # eight small files in MinIO
        table.append(batch)
    table = catalog.load_table("db.events")
    assert table.inspect.files().num_rows == 8
    assert table.scan().to_arrow().num_rows == 800

    result = compact(
        catalog,
        "db.events",
        engine="trino",
        engine_catalog="iceberg",  # the Trino catalog name from iceberg.properties
        execute=True,
        engine_config={"host": TRINO_HOST, "port": TRINO_PORT, "user": "iceops"},
    )

    table = catalog.load_table("db.events")
    assert result.status == "compacted"
    assert result.data_files_before == 8
    assert result.data_files_after is not None and result.data_files_after < 8
    # row count preserved — verify_row_count did not raise on a real Trino rewrite
    assert table.scan().to_arrow().num_rows == 800
    assert table.inspect.files().num_rows == result.data_files_after

    # cross-engine confirmation: Trino sees the same rows PyIceberg does
    import trino

    conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="iceops", catalog="iceberg")
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM iceberg.db.events")
    assert cur.fetchone()[0] == 800
