# Engines — native vs. Spark / Trino

Every fix operator runs **natively by default** (no cluster) and can instead **delegate to
an engine** with `--engine spark|trino`. iceops is always the brain (what to do, in what
order, with what safety); the engine is optional muscle for scale.

## What runs where

| Command | Native (default) | With `--engine spark\|trino` |
| --- | --- | --- |
| `expire` | PyIceberg, metadata-only | engine's `expire_snapshots` (also deletes files) |
| `rewrite-manifests` | PyIceberg, metadata-only | engine's `rewrite_manifests` / `optimize_manifests` |
| `clean-orphans` | iceops' own safety funnel | engine's `remove_orphan_files` |
| `compact` | *not available yet* | **required**: engine's `rewrite_data_files` / `optimize` |

Use **native** for the no-cluster path (fast, metadata-only, works from a laptop or CI).
Use an **engine** when you already run one, want a single governed execution path, or need
scale. The standout is **clean-orphans via an engine** — Spark/Trino's `remove_orphan_files`
is battle-tested for object-store listing at scale.

`compact` currently requires an engine; native (in-process) compaction is planned.

## How engine mode behaves

In engine mode iceops submits the engine's own maintenance procedure and relays the result.
The engine selects the exact work and applies **its own** safety semantics, which differ:

- **Spark** hardcodes a 24-hour minimum for `remove_orphan_files` (no override).
- **Trino** makes it configurable via `iceberg.remove-orphan-files.min-retention`
  (default 7 days).

iceops surfaces the engine's safety rather than fighting it. Because compaction rewrites
data, iceops additionally verifies the engine preserved every row (via snapshot
`total-records`) and refuses the result on a mismatch — the pre-compaction snapshot stays
intact for rollback.

## Configuring engines

Add an `[engines.*]` block to your `.iceops.toml`:

```toml
[engines.spark]
master = "local[*]"
# or Spark Connect:  remote_uri = "sc://spark-connect-host:15002"
# pass through Spark catalog config as quoted dotted keys:
# "spark.jars.packages" = "org.apache.iceberg:iceberg-spark-runtime-…"
# "spark.sql.catalog.prod" = "org.apache.iceberg.spark.SparkCatalog"

[engines.trino]
host = "trino.example.com"
port = 8080
user = "iceops"
catalog = "iceberg"
```

Install the extra for the engine you use: `pip install "iceops[spark]"` or
`"iceops[trino]"`.

## Usage

```console
# native (default)
$ iceops clean-orphans db.events --catalog prod

# delegate to an engine (dry-run shows the engine-mode plan)
$ iceops expire db.events --catalog prod --engine spark

# whole maintenance loop via one engine
$ iceops tune db.events --catalog prod --engine spark --yes
```

`--engine-catalog <name>` sets the catalog name the engine uses (defaults to the profile
name). In a policy, `engine` can be global or set per table.

## Managed tables

Tables already managed by another optimizer — Amoro, S3 Tables, Snowflake/Databricks
managed — are detected and **skipped** by fix operators (to avoid fighting their
optimizer). Override with `--force` only if you know what you're doing.
