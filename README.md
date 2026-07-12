<p align="center">
  <img src="docs/assets/iceops-logo.svg" alt="iceops logo" width="180" height="180">
</p>

# iceops

**Doctor, janitor, and autopilot for your Apache Iceberg lakehouse — in one `pip install`.**

No JVM. No Spark cluster. No platform to deploy. Point iceops at any Iceberg catalog and get
a fleet health report in five minutes.

```console
$ pip install iceops
$ iceops scan --catalog prod
$ iceops doctor db.events
$ iceops cost db.events
```

## Why

Operating Iceberg in production means fighting small files, unbounded snapshot growth,
manifest fragmentation, and orphaned data silently burning object-storage money. Today your
options are hand-rolled Spark maintenance jobs per table, or deploying and operating a
management platform. iceops is the third option: a CLI-first tool that diagnoses, fixes, and
continuously maintains Iceberg tables from your laptop, your CI, or your existing scheduler.

**Documentation:** [Quickstart](docs/quickstart.md) · [Concepts](docs/concepts.md) ·
[Command reference](docs/commands.md) · [Policy (`iceops.yaml`)](docs/policy.md) ·
[Engines](docs/engines.md). See [VISION.md](VISION.md) for goals and (importantly)
non-goals.

## The mental model

iceops is one loop: **make the state of every Iceberg table observable, and every change
to it reviewable.**

```
scan ──▶ plan ──▶ review ──▶ apply ──▶ verify ──▶ (back to scan)
```

| Stage | What it means | In iceops |
| --- | --- | --- |
| **scan** | observe reality, grade it, price it | `iceops scan` / `doctor` / `cost` |
| **plan** | a literal listing of what *would* change | every fix command's default is a dry run |
| **review** | a human decides — this stage is non-removable | you read the plan; in policy mode, your team reviews the `iceops.yaml` PR |
| **apply** | execute exactly the reviewed plan, atomically | `--yes` (per command) / `iceops apply` (v0.3) |
| **verify** | confirm the loop converged | re-run `scan` — status flips, exit codes make it CI-checkable |

Tables keep getting written to, so this is a cycle, not a pipeline — the same
`plan → review → apply` discipline as terraform, pointed at table health.

## What works today

| Command | What it does |
| --- | --- |
| `iceops scan` | Fleet-wide health report: healthy / warn / critical per table |
| `iceops doctor <table>` | Deep single-table report: file-size histogram, snapshot bloat, manifest fragmentation, delete-file ratio, partition skew |
| `iceops cost <table>` | Estimated wasted storage $ from unexpired snapshots and orphaned files |
| `iceops expire <table>` | Expire old snapshots — dry-run by default, `--yes` to execute |
| `iceops rewrite-manifests <table>` | Consolidate fragmented manifests (metadata only) — dry-run by default |
| `iceops clean-orphans <table>` | Delete files no snapshot references — dry-run by default, age-guarded |
| `iceops tune <table>` | Run all maintenance in the right order (compact → rewrite-manifests → expire → clean-orphans) |
| `iceops apply` | Run a per-table `iceops.yaml` policy across a catalog — maintenance as code |
| `iceops compact <table> --engine spark` | Plan/submit data-file compaction through Spark — dry-run by default |
| `iceops catalogs` | List configured catalog profiles |

Every command supports `--json` for machine consumption, and exit codes are CI-friendly
(0 = healthy/done, 1 = findings or planned-but-dry-run, 2 = error). Full options and
example output for every command: **[docs/commands.md](docs/commands.md)**.

`expire` never deletes files: it unreferences old snapshots atomically via PyIceberg
(branch/tag heads and the current snapshot are always protected) and reports exactly
which snapshots go and how many bytes become unreferenced. A snapshot is only expired if
it is BOTH beyond `--retain-last` AND older than `--older-than`.

`clean-orphans` is the only iceops command that deletes physical files, and it is built
paranoid: it deletes only files referenced by no snapshot (failed-write debris and what
`expire`/`rewrite-manifests` unreference), never touches `*.metadata.json`, never touches
files younger than `--older-than` (default 3d — an in-flight write can look orphaned),
supports `--exclude` globs, and re-verifies table metadata before every delete batch in
case a writer committed mid-run.

`compact` is federated first: iceops plans and renders the action, while Spark/Trino own
the data-file rewrite. Dry-run output shows the delegated plan kind, the exact engine
statement, and safety/verification notes. Storage reclaim still flows through `expire`
then `clean-orphans`; compact never deletes physical files directly. Native Arrow
compaction remains later; `tune` and declarative policy (`iceops.yaml` + `iceops apply`)
are available in the current development tree; a stateless HTTP API (`iceops serve`)
comes later.

## Quickstart with a local demo lakehouse

```console
$ uv sync
$ uv run python examples/demo.py      # builds a deliberately unhealthy local warehouse
$ uv run iceops scan --catalog demo
$ uv run iceops doctor db.events --catalog demo
$ uv run iceops cost db.events --catalog demo
```

To verify Spark-backed compaction locally:

```console
$ uv sync --extra spark
$ uv run python examples/spark_lab.py
$ ICEOPS_CONFIG=.iceops.spark-lab.toml uv run iceops compact db.events --catalog sparklab --engine spark --yes
```

To verify the Spark Connect client path:

```console
$ uv run python examples/spark_connect_lab.py
$ ICEOPS_CONFIG=.iceops.spark-connect-lab.toml uv run iceops compact db.events --catalog sparkconnectlab --engine spark --yes
```

## Connecting to your catalog

iceops reads profiles from `.iceops.toml` (project) or `~/.iceops/config.toml` (user),
and falls back to your existing [PyIceberg configuration](https://py.iceberg.apache.org/configuration/)
— if `pyiceberg` can reach your catalog, so can iceops.

```toml
[catalogs.prod]
type = "rest"
uri = "https://polaris.example.com/api/catalog"
credential = "…"

[catalogs.demo]
type = "sql"
uri = "sqlite:///demo_warehouse/catalog.db"
warehouse = "file://demo_warehouse"

[engines.spark]
master = "local[*]"
# or: remote_uri = "sc://spark-connect-host:15002"
# Spark catalog settings can be passed through as quoted TOML keys, for example:
# "spark.sql.catalog.demo" = "org.apache.iceberg.spark.SparkCatalog"
# "spark.sql.catalog.demo.type" = "hadoop"
# "spark.sql.catalog.demo.warehouse" = "file:///path/to/warehouse"
```

Catalog connectivity is PyIceberg's, so any catalog it supports works: any REST-spec catalog
(Polaris, Nessie, Gravitino, Lakekeeper), plus SQL and Hive. AWS Glue is available via
`pip install "iceops[glue]"` — that extra is a pass-through to `pyiceberg[glue]` (boto3);
Glue works through PyIceberg but isn't yet exercised by the iceops test suite.

## Design in one paragraph

Thin frontends, fat library: the CLI (and later the HTTP API) are skins over operator
functions that return typed results. Every operation splits into a *plan* (metadata-only,
via the catalog) and an *execute* (heavy file work, via a pluggable engine — in-process
Arrow by default, your existing Spark/Trino for very large tables). Destructive actions are
dry-run by default and gated behind `--yes`. Tables managed by another optimizer (Amoro,
S3 Tables, Snowflake/Databricks managed) are detected and skipped by fix operators.

## License

Apache-2.0
