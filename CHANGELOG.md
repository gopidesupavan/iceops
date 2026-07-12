# Changelog

All notable changes to iceops are documented here. This project follows
[Semantic Versioning](https://semver.org).

## [0.1.0] — 2026-07-12

First public release. A CLI-first tool to diagnose, fix, and continuously maintain
Apache Iceberg tables — no JVM, no cluster, no platform to deploy.

### Diagnose (read-only)
- `iceops scan` — fleet-wide health report (healthy / warn / critical per table)
- `iceops doctor <table>` — deep report: file-size histogram, snapshot bloat, manifest
  fragmentation, delete-file ratio, partition skew, orphan estimate, metadata-cleanup
- `iceops cost <table>` — wasted-storage estimate (live / stale / orphan bytes, $/month)

### Fix operators (dry-run by default, `--yes` to execute)
- `iceops expire` — drop old snapshots (retain-last AND older-than); metadata-only
- `iceops rewrite-manifests` — consolidate fragmented manifests; metadata-only
- `iceops clean-orphans` — delete files no snapshot references; the single deletion path,
  with a metadata-protection + age-threshold + re-check safety funnel
- `iceops compact --engine spark|trino` — data-file compaction, delegated to an engine
- `iceops tune` — run all of the above in the correct maintenance order

### Engine backends
- Native (default, no cluster) for expire / rewrite-manifests / clean-orphans
- `--engine spark|trino` for every fix operator (Spark system procedures / Trino
  `ALTER TABLE … EXECUTE`), verified against real Spark and real Trino
- Managed-table detection (Amoro, S3 Tables, Snowflake/Databricks) — skipped unless
  `--force`

### Maintenance as code
- `iceops apply` — run a per-table `iceops.yaml` policy across a catalog: `defaults` +
  per-table overrides, `when:` conditions on metrics, `disabled`, per-table `engine`

### Everywhere
- `--json` on every command; CI-friendly exit codes (0 done, 1 planned/findings, 2 error)
- Catalog-agnostic via PyIceberg: REST (Polaris/Nessie/Gravitino/Lakekeeper), Glue, SQL

### Known limitations
- Native (no-cluster) compaction is not available yet — compaction requires
  `--engine spark|trino`. Native partition-level compaction is planned.
- Native `clean-orphans` on object stores (S3/GCS/ADLS) runs through PyIceberg FileIO but
  has been exercised primarily via the engine path; treat native object-store orphan
  cleanup as early. `--older-than` (default 3d) is the safety guard.
- REST and Glue catalogs are supported via PyIceberg; the test suite exercises SQL and
  REST (through the Trino stack) catalogs directly.

[0.1.0]: https://github.com/gopidesupavan/iceops/releases/tag/v0.1.0
