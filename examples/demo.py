"""Build a deliberately unhealthy local lakehouse to demo iceops against.

Creates ./demo_warehouse (SQLite catalog + parquet files) and a project-local
.iceops.toml with a 'demo' profile, then prints the commands to try.

Run:  uv run python examples/demo.py
"""

from __future__ import annotations

import random
import shutil
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog import load_catalog

ROOT = Path(__file__).resolve().parent.parent
WAREHOUSE = ROOT / "demo_warehouse"

EVENT_TYPES = ["click", "view", "purchase", "signup"]


def make_batch(rows: int, start_id: int) -> pa.Table:
    rng = random.Random(start_id)
    return pa.table(
        {
            "event_id": pa.array(range(start_id, start_id + rows), type=pa.int64()),
            "user_id": pa.array([rng.randint(1, 5000) for _ in range(rows)], type=pa.int64()),
            "event_type": pa.array([rng.choice(EVENT_TYPES) for _ in range(rows)]),
            "amount": pa.array([rng.random() * 100 for _ in range(rows)], type=pa.float64()),
        }
    )


def main() -> None:
    if WAREHOUSE.exists():
        shutil.rmtree(WAREHOUSE)
    WAREHOUSE.mkdir()

    catalog = load_catalog(
        "demo",
        type="sql",
        uri=f"sqlite:///{WAREHOUSE}/catalog.db",
        warehouse=f"file://{WAREHOUSE}",
    )
    catalog.create_namespace("db")

    # db.events — a streaming-ingestion victim: 60 tiny commits, 60 tiny files
    print("creating db.events (60 tiny commits — the streaming-ingestion victim) ...")
    events = catalog.create_table("db.events", schema=make_batch(1, 0).schema)
    for i in range(60):
        events.append(make_batch(300, i * 300))

    # sprinkle an orphan file into its data dir, like a failed write would
    data_dir = WAREHOUSE / "db.db" / "events" / "data"
    if not data_dir.exists():
        candidates = list(WAREHOUSE.rglob("data"))
        data_dir = candidates[0] if candidates else None
    if data_dir:
        parquet_files = sorted(data_dir.glob("*.parquet"))
        if parquet_files:
            orphan = data_dir / "00000-0-orphaned-by-failed-write.parquet"
            shutil.copy(parquet_files[0], orphan)
            with orphan.open("ab") as fh:  # pad it so the estimate is visible
                fh.write(b"\0" * 2 * 1024 * 1024)

    # db.orders — a healthy batch table: 3 commits
    print("creating db.orders (3 batch commits — the healthy neighbor) ...")
    orders = catalog.create_table("db.orders", schema=make_batch(1, 0).schema)
    for i in range(3):
        orders.append(make_batch(50_000, i * 50_000))

    config = ROOT / ".iceops.toml"
    config.write_text(
        "[catalogs.demo]\n"
        'type = "sql"\n'
        f'uri = "sqlite:///{WAREHOUSE}/catalog.db"\n'
        f'warehouse = "file://{WAREHOUSE}"\n'
    )

    print(f"\ndemo lakehouse ready at {WAREHOUSE}")
    print(f"profile 'demo' written to {config}\n")
    print("try:")
    print("  uv run iceops scan --catalog demo")
    print("  uv run iceops doctor db.events --catalog demo")
    print("  uv run iceops cost db.events --catalog demo")
    print("  uv run iceops doctor db.events --catalog demo --json")


if __name__ == "__main__":
    main()
