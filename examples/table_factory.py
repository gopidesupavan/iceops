"""Factory for Iceberg table variations — the iceops compatibility lab.

Builds one table per Iceberg feature dimension so operators can be validated against
real shapes, not just plain unpartitioned appends: identity/temporal/bucket/truncate
partition transforms, partition-spec evolution, schema evolution, and copy-on-write
overwrites (which create genuinely stale files — the case expire actually frees bytes).

Used by tests/test_variations.py (the compatibility matrix) and examples/variations.py
(builds the same tables in the demo warehouse to try iceops against by hand). If a
variation is unsupported by the installed PyIceberg, the factory records WHY instead of
crashing — unsupported shapes are findings, not failures.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Callable

import pyarrow as pa
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    IdentityTransform,
    MonthTransform,
    TruncateTransform,
)
from pyiceberg.types import DoubleType, LongType, NestedField, StringType, TimestampType

BASE_SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "category", StringType(), required=False),
    NestedField(3, "ts", TimestampType(), required=False),
    NestedField(4, "amount", DoubleType(), required=False),
)

CATEGORIES = ["alpha", "beta", "gamma", "delta"]
BASE_DAY = dt.datetime(2026, 6, 1)

ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("category", pa.string()),
        pa.field("ts", pa.timestamp("us")),
        pa.field("amount", pa.float64()),
    ]
)


def batch(start: int = 0, rows: int = 200, day_spread: int = 4, month_spread: int = 1) -> pa.Table:
    """Rows spread across categories, days, and months so every transform yields >1 partition."""
    ids = list(range(start, start + rows))
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "category": pa.array([CATEGORIES[i % len(CATEGORIES)] for i in ids]),
            "ts": pa.array(
                [
                    BASE_DAY + dt.timedelta(days=(i % day_spread) + 31 * (i % max(month_spread, 1)))
                    for i in ids
                ],
                type=pa.timestamp("us"),
            ),
            "amount": pa.array([float(i) * 1.5 for i in ids], type=pa.float64()),
        },
        schema=ARROW_SCHEMA,
    )


def _spec(source_id: int, transform: object, name: str) -> PartitionSpec:
    return PartitionSpec(
        PartitionField(source_id=source_id, field_id=1000, transform=transform, name=name)
    )


@dataclass
class Variation:
    name: str
    describe: str
    build: Callable  # (catalog, identifier) -> None


def _partitioned(spec: PartitionSpec, appends: int = 4):
    def build(catalog, identifier: str) -> None:
        table = catalog.create_table(identifier, schema=BASE_SCHEMA, partition_spec=spec)
        for i in range(appends):
            table.append(batch(start=i * 200, month_spread=2))

    return build


def _plain(catalog, identifier: str) -> None:
    table = catalog.create_table(identifier, schema=BASE_SCHEMA)
    for i in range(4):
        table.append(batch(start=i * 200))


def _evolved_spec(catalog, identifier: str) -> None:
    """Starts unpartitioned, gains an identity partition mid-life: two specs, one table."""
    table = catalog.create_table(identifier, schema=BASE_SCHEMA)
    for i in range(3):
        table.append(batch(start=i * 200))
    with table.update_spec() as update:
        update.add_field("category", IdentityTransform(), "category")
    table = catalog.load_table(identifier)
    for i in range(3, 6):
        table.append(batch(start=i * 200))


def _evolved_schema(catalog, identifier: str) -> None:
    """Gains a column mid-life: old files lack it, new files have it."""
    table = catalog.create_table(identifier, schema=BASE_SCHEMA)
    for i in range(2):
        table.append(batch(start=i * 200))
    with table.update_schema() as update:
        update.add_column("extra", LongType())
    table = catalog.load_table(identifier)
    wide = batch(start=400).append_column("extra", pa.array(range(200), type=pa.int64()))
    for _ in range(2):
        table.append(wide)


def _overwritten(catalog, identifier: str) -> None:
    """Copy-on-write overwrite: previous files become STALE (referenced only by old
    snapshots) — the one shape where expire genuinely frees data bytes."""
    table = catalog.create_table(identifier, schema=BASE_SCHEMA)
    for i in range(4):
        table.append(batch(start=i * 200))
    table.overwrite(batch(start=10_000, rows=300))


VARIATIONS: list[Variation] = [
    Variation("plain", "unpartitioned appends (the baseline)", _plain),
    Variation(
        "part_identity",
        "identity(category) — classic string partition",
        _partitioned(_spec(2, IdentityTransform(), "category")),
    ),
    Variation(
        "part_day",
        "day(ts) — temporal partitioning",
        _partitioned(_spec(3, DayTransform(), "ts_day")),
    ),
    Variation(
        "part_month",
        "month(ts) — coarser temporal partitioning",
        _partitioned(_spec(3, MonthTransform(), "ts_month")),
    ),
    Variation(
        "part_bucket",
        "bucket(4, id) — hash partitioning",
        _partitioned(_spec(1, BucketTransform(4), "id_bucket")),
    ),
    Variation(
        "part_truncate",
        "truncate(2, category) — prefix partitioning",
        _partitioned(_spec(2, TruncateTransform(2), "cat_trunc")),
    ),
    Variation("evolved_spec", "partition spec evolved mid-life (two specs)", _evolved_spec),
    Variation("evolved_schema", "column added mid-life", _evolved_schema),
    Variation("overwritten", "copy-on-write overwrite (stale files exist)", _overwritten),
]


def build_all(catalog, namespace: str = "lab") -> dict[str, str]:
    """Build every variation; returns {identifier or name: 'ok' | 'unsupported: why'}."""
    try:
        catalog.create_namespace(namespace)
    except Exception:
        pass
    results: dict[str, str] = {}
    for variation in VARIATIONS:
        identifier = f"{namespace}.{variation.name}"
        try:
            catalog.drop_table(identifier)
        except Exception:
            pass
        try:
            variation.build(catalog, identifier)
            results[identifier] = "ok"
        except Exception as exc:  # unsupported shapes are findings, not failures
            results[identifier] = f"unsupported: {type(exc).__name__}: {exc}"
    return results
