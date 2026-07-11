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

See [VISION.md](VISION.md) for goals and (importantly) non-goals.

## What works today (v0.1 — read-only)

| Command | What it does |
| --- | --- |
| `iceops scan` | Fleet-wide health report: healthy / warn / critical per table |
| `iceops doctor <table>` | Deep single-table report: file-size histogram, snapshot bloat, manifest fragmentation, delete-file ratio, partition skew |
| `iceops cost <table>` | Estimated wasted storage $ from unexpired snapshots and orphaned files |
| `iceops catalogs` | List configured catalog profiles |

Every command supports `--json` for machine consumption, and exit codes are CI-friendly
(0 = healthy, 1 = findings, 2 = error).

v0.1 never writes to your tables. Fix operators (`compact`, `expire`, `clean-orphans`,
`tune`) land in v0.2, dry-run by default; declarative policy (`iceops.yaml` + `iceops apply`)
in v0.3; a stateless HTTP API (`iceops serve`) in v0.4.

## Quickstart with a local demo lakehouse

```console
$ uv sync
$ uv run python examples/demo.py      # builds a deliberately unhealthy local warehouse
$ uv run iceops scan --catalog demo
$ uv run iceops doctor db.events --catalog demo
$ uv run iceops cost db.events --catalog demo
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
```

Any Iceberg REST-spec catalog works: Polaris, Nessie, Gravitino, Lakekeeper. AWS Glue via
`pip install iceops[glue]`.

## Design in one paragraph

Thin frontends, fat library: the CLI (and later the HTTP API) are skins over operator
functions that return typed results. Every operation splits into a *plan* (metadata-only,
via the catalog) and an *execute* (heavy file work, via a pluggable engine — in-process
Arrow by default, your existing Spark/Trino for very large tables). Destructive actions are
dry-run by default and gated behind `--yes`. Tables managed by another optimizer (Amoro,
S3 Tables, Snowflake/Databricks managed) are detected and skipped by fix operators.

## License

Apache-2.0
