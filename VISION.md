# iceops vision

**Every Iceberg table healthy, without anyone running a platform to make it so.**

Operating Iceberg today forces a choice: hand-roll Spark maintenance jobs per table, or
deploy and operate a management platform (services, databases, optimizer clusters). iceops
removes that choice. Table maintenance should be as easy as `terraform plan` /
`terraform apply` — a tool you install in seconds, point at a catalog, and trust.

## Mission

A single `pip install` that lets any data engineer **diagnose, fix, and continuously
maintain** Iceberg tables from their laptop, their CI, or their existing scheduler — no JVM,
no cluster, no service to babysit.

## Goals

1. **Zero-to-insight in five minutes.** `pip install iceops && iceops scan` against any
   catalog must produce a fleet health report with no other setup. Every release is measured
   against this bar.
2. **Make table health legible.** Health scores, file-size histograms, snapshot/manifest
   bloat, and wasted-storage cost in plain language and dollars — the `df -h` for a
   lakehouse.
3. **Encode the tribal knowledge.** Correct operation ordering (compact → expire →
   clean-orphans), safe defaults, dry-run everywhere. Users shouldn't need to have read the
   Iceberg spec to run maintenance safely.
4. **Maintenance as code.** A declarative `iceops.yaml` policy, checked into git, applied by
   any scheduler. Reviewable, diffable, reproducible.
5. **Scale by delegation, not by infrastructure.** In-process (Arrow/DuckDB) by default;
   delegate to the user's *existing* Spark or Trino for huge tables. iceops is always the
   brain, never the cluster.
6. **Work everywhere Iceberg lives.** Any REST-spec catalog (Polaris, Nessie, Gravitino,
   Lakekeeper) plus Glue and Hive — one tool across heterogeneous estates.
7. **Automation-native.** `--json` on every command, meaningful exit codes, Prometheus
   metrics, a stateless HTTP API — built to be embedded in Airflow, CI, and dashboards, not
   to replace them.

## Non-goals

1. **Not a platform.** No long-running managed service, no database, no built-in scheduler,
   no user management, no web console. If a feature requires iceops to *stay running* to
   deliver value, it's out of scope.
2. **Not a catalog.** iceops connects to catalogs; it never stores or federates metadata
   itself.
3. **Not a query or ingestion engine.** No SQL querying of table data, no writing user data,
   no CDC/streaming ingestion. iceops touches data files only to reorganize them.
4. **Not a UI product.** CLI and API first; a TUI at most. Web UIs can be built *on* the API
   by others.
5. **Not multi-format.** Iceberg only — no Delta, no Hudi. Depth over breadth. (Other
   formats may be considered later only via their Iceberg-compatible interfaces.)
6. **Not a Spark/Flink replacement.** For very large tables iceops orchestrates the user's
   engines rather than reimplementing distributed compute in Python.
7. **Not autonomous by default.** iceops never mutates a table it wasn't explicitly told (by
   flag or committed policy) to touch. Predictability beats cleverness in tools that delete
   files.

## Design principles

- **Read-only until told otherwise** — destructive actions are opt-in, dry-run is the
  default, `--yes` is always explicit.
- **Plan, then execute** — every fix shows what it *would* do before it does it.
- **Thin frontends, fat library** — CLI, API, and `import iceops` are all skins over the
  same operators.
- **Lean install** — the base package stays small; Glue, Spark, Trino support are extras.
- **Boring is a feature** — no surprises is the product.
