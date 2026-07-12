# iceops command reference

Minimal reference for everything shipped today. Every command supports `--json` for
machine-readable output. Exit codes everywhere: `0` healthy / done / nothing to do,
`1` findings exist or work was planned but not executed (dry run), `2` error.

Fix commands (`expire`, `rewrite-manifests`, `clean-orphans`, `compact`) are
**dry-run by default** — they print exactly what would happen and change nothing until
`--yes`. They refuse tables managed by another optimizer (Amoro, S3 Tables, …) unless
`--force`.

## Setup

Point iceops at a catalog via `.iceops.toml` (project) or `~/.iceops/config.toml`:

```toml
[catalogs.prod]
type = "rest"
uri = "https://polaris.example.com/api/catalog"

[catalogs.demo]
type = "sql"
uri = "sqlite:///demo_warehouse/catalog.db"
warehouse = "file://demo_warehouse"

[engines.spark]
master = "local[*]"
# or: remote_uri = "sc://spark-connect-host:15002"
```

Unknown names fall through to [PyIceberg's own config](https://py.iceberg.apache.org/configuration/).
With a single profile configured, `--catalog` can be omitted. Tables are addressed as
`namespace.table` or fully qualified `catalog.namespace.table`.

Try everything against a local demo lakehouse: `uv run python examples/demo.py`.
For a real Spark-backed compaction lab: `uv sync --extra spark`, then
`uv run python examples/spark_lab.py`. For Spark Connect local mode:
`uv run python examples/spark_connect_lab.py`. Gated verification tests:
`ICEOPS_RUN_SPARK=1 uv run pytest tests/integration/test_spark_compact_lab.py` and
`ICEOPS_RUN_SPARK_CONNECT=1 uv run pytest tests/integration/test_spark_connect_compact_lab.py`.

---

## iceops scan — fleet health report

```console
$ iceops scan --catalog demo
┃ table     ┃ status  ┃ files ┃ small ┃ snapshots ┃    size ┃ top issue   ┃
│ db.events │  warn   │    60 │  100% │        60 │ 359.3KB │ small-files │
│ db.orders │ healthy │     3 │  100% │         3 │   2.1MB │ healthy     │
warn: 1  healthy: 1
```

Options: `--pattern 'db.ev*'` (glob over table names), `--json`.
Status is the worst finding severity: `healthy` / `warn` / `critical`.

## iceops doctor — single-table deep report

```console
$ iceops doctor db.events --catalog demo
db.events  warn
  60 data files (359.3KB, avg 6.0KB) · 0 delete files · 60 snapshots · 60 manifests …
  file sizes
       <1MB  ██████████████████████████████ 60
  findings
  ● warn 60 of 60 data files (100%) are under 32MB …
```

Checks: small-files, snapshot-bloat, manifest-fragmentation, delete-files,
partition-skew, orphan-files, metadata-cleanup-disabled. Also detects externally
managed tables and streaming writers.

## iceops cost — wasted-storage estimate

```console
$ iceops cost db.events --catalog demo
│ live          │ 359.3KB │ referenced by current snapshot                     │
│ stale         │      0B │ only reachable via old snapshots — freed by expire │
│ orphan (est.) │   2.0MB │ referenced by nothing — freed by clean-orphans     │
estimated waste: $0.02/month at $0.023/GB-month
```

Options: `--dollars-per-gb-month 0.023`. The default (`0.023`) is S3 Standard's first-50TB
us-east-1 rate — a sane default only. Set your own for the whole environment with the
`ICEOPS_DOLLARS_PER_GB_MONTH` env var (e.g. `0.021` for higher S3 tiers, your GCS/Azure
rate, or `0` for self-hosted MinIO); the `--dollars-per-gb-month` flag overrides the env
var. The rate used is printed in the output, so the assumption is always visible. Stale is
an upper bound; unknowns are reported as notes, never silently zeroed.

## iceops expire — drop old snapshots (metadata only)

```console
$ iceops expire db.events --catalog demo --retain-last 10 --older-than 7d
plan: expire 50 of 60 snapshots (2026-07-04 … 2026-07-11 UTC)
  snapshot 168789917308195932  2026-07-11 09:15:11  append
  …
after expiry: 258.4KB of manifests + 0B of data files become unreferenced
DRY RUN — nothing changed. Add --yes to execute.
```

A snapshot is expired only if it is BOTH beyond `--retain-last` (default 10) AND older
than `--older-than` (default 7d). Branch/tag heads and the current snapshot are never
expired. Expiration deletes **no files** — it unreferences them; reclaim with
`clean-orphans`. You lose time travel only to the expired versions.

## iceops rewrite-manifests — consolidate the table's index (metadata only)

```console
$ iceops rewrite-manifests db.events --catalog demo --yes
rewrote manifests: 60 → 1 (snapshot 7402711359425541986)
```

Fixes slow query planning caused by many tiny manifests. No data files touched; one new
snapshot is created and the previous one remains for rollback.
Options: `--target-manifest-size 8MB`.

## iceops clean-orphans — delete files no snapshot references

```console
$ iceops clean-orphans db.events --catalog demo
plan: delete 1 orphaned files (2.0MB) under file:///…/db/events
  data/00000-0-orphaned-by-failed-write.parquet  (2.0MB, 5d old)
listed 242 files · 241 reachable
*.metadata.json files are never deleted; files younger than 3d are never deleted
DRY RUN — nothing changed. Add --yes to execute.
```

The only iceops command that deletes physical files. Guards: `*.metadata.json` and
`version-hint.text` are never deleted; files younger than `--older-than` (default 3d)
are never deleted (in-flight writes can look orphaned); `--exclude '_SUCCESS'`
(repeatable) protects extra patterns; the table is re-checked before every delete batch
in case a writer committed mid-run.

## iceops compact — federated data-file compaction

```console
$ iceops compact db.events --catalog demo --engine spark --target-file-size 512MB
plan: compact 60 small files in db.events via spark (target 512.0MB)
plan kind: delegated
  engine catalog: demo · snapshot: 7402711359425541986

statement:
  CALL `demo`.system.rewrite_data_files(table => 'demo.db.events', options => map(...))

estimated work:
  data files: 60
  small files: 60
  delete files: 0

safety:
  - spark chooses the exact files to rewrite.
  - iceops does not delete physical files during compact.
  - old files remain until expire runs, then become clean-orphans candidates.

verification:
  - row-count verification runs after execution when snapshot metadata exposes total-records

DRY RUN — nothing changed. Add --yes to execute.
```

`compact` is engine-backed in this slice, and `--engine` is **required** (native Arrow
compaction is not yet available). iceops plans and submits one engine action; Spark runs
Iceberg `rewrite_data_files`, Trino runs `ALTER TABLE … EXECUTE optimize`. Because
compaction rewrites data, iceops verifies the engine preserved every row (via snapshot
`total-records`) when that metadata is available, reports `passed` or `skipped`, and
refuses any mismatch — the pre-compaction snapshot stays intact for rollback.

Both engines are verified against the real thing (no mocks): Spark via a local JVM, Trino
via a REST catalog + MinIO + Trino container stack. Run the gated labs with
`ICEOPS_RUN_SPARK=1` / `ICEOPS_RUN_TRINO=1`, or by hand: `examples/spark_lab.py` and
`examples/trino_lab.py` (the latter needs
`docker compose -f tests/integration/trino_stack/docker-compose.yml up -d`).

Options: `--engine spark|trino` (required), `--engine-catalog <name>`,
`--target-file-size 512MB`, `--yes`, `--force`, `--json`.

Compaction rewrites data into a new snapshot but does not reclaim old physical files by
itself. Reclaim remains the normal safe lifecycle: compact, then `expire`, then
`clean-orphans`.

## Native vs. engine execution

Every fix command runs **natively by default** (no cluster) and can instead **delegate to
an engine** with `--engine spark|trino`:

| Command | Native (default) | With `--engine` |
| --- | --- | --- |
| `expire` | PyIceberg, metadata-only | engine's `expire_snapshots` (also deletes files) |
| `rewrite-manifests` | PyIceberg | engine's `rewrite_manifests` / `optimize_manifests` |
| `clean-orphans` | iceops' own safety funnel | engine's `remove_orphan_files` |
| `compact` | not available yet | required: engine's `rewrite_data_files` / `optimize` |

Use native for the no-cluster path; use an engine when you already run one, want a single
governed execution path, or need scale. The standout is **clean-orphans via engine** —
Spark/Trino's `remove_orphan_files` is battle-tested for object-store listing at scale.
In engine mode the engine selects the exact work and applies **its own** retention and
reachability (e.g. Spark hardcodes a 24h minimum for orphan removal; Trino's is
configurable). Configure connections in `.iceops.toml`:

```toml
[engines.spark]
master = "local[*]"          # or remote_uri = "sc://spark-connect-host:15002"
[engines.trino]
host = "trino.example.com"
port = 8080
user = "iceops"
```

## iceops tune — run all maintenance in the right order

```console
$ iceops tune db.events --catalog demo
tune db.events — maintenance in order: compact → rewrite-manifests → expire → clean-orphans

▸ compact
  skipped — no --engine (compaction needs spark or trino)
▸ rewrite-manifests
plan: consolidate 60 manifests … into ~1
▸ expire
db.events: nothing to expire (…)
▸ clean-orphans
db.events: nothing to clean (…)
note: each step is planned against the current table; earlier steps change what later
ones do. clean-orphans only deletes files past its age threshold …
DRY RUN — nothing changed. Add --yes to execute.
```

One command for the whole loop, in the order that can't corrupt a table:
**compact → rewrite-manifests → expire → clean-orphans**. tune adds no new behaviour — it
composes the four operators. Two things to know:

- **compact runs only with `--engine spark|trino`**; without it, tune runs the native
  three and shows compact as skipped. No-cluster users still get most of the value.
- **A single run won't reclaim everything immediately.** clean-orphans respects its age
  threshold, so files this run just orphaned aren't deleted until a later run when they
  age past it — the safe behaviour. Run tune on a schedule; each pass converges further.

If a step fails, tune stops there and never runs later steps on an unexpected state
(exit 2, `halted at <step>`).

Options: `--engine spark|trino`, `--engine-catalog`, `--older-than 7d` (expire window),
`--yes`, `--force`, `--json`.

## iceops apply — per-table maintenance as code

```console
$ iceops apply --policy iceops.yaml
policy over catalog 'prod' — 2 tables in scope

db.events [spark]
  will run rewrite-manifests  (manifest-count 60 > 50)
  skip compact (small-file-ratio 0.12 <= 0.3)
  will run expire  (no condition)
· db.audit: disabled by policy

DRY RUN — nothing changed. Add --yes to execute.
```

Runs a checked-in `iceops.yaml` across a catalog — the "maintenance as code" path. Policy
is **per table**: `defaults` apply to every table, and `tables` entries (glob → overrides)
tune individual tables, merging field-by-field so a table that sets only `retain-last`
keeps the default `older-than`. `disabled: true` skips a table entirely; `engine` can be
set globally or per table. Each op runs only if its section is present AND its `when:`
condition passes — the dry-run shows the reason for every decision.

apply composes the four fix operators in the safe order (compact → rewrite-manifests →
expire → clean-orphans); it never adds new behavior. Dry-run lists **every table in
scope** so you see the full blast radius before `--yes`. Check the policy into git, review
it as a PR, run `iceops apply --yes` from cron / Airflow / a GitHub Action.

See [examples/iceops.yaml](../examples/iceops.yaml) for a full annotated policy and the
list of metrics usable in `when:`. Options: `--policy iceops.yaml`, `--catalog`, `--yes`,
`--force`, `--json`.

## iceops catalogs / iceops version

List configured profiles / print the version.

---

## Typical maintenance session

```console
$ iceops scan --catalog prod                       # who needs help?
$ iceops doctor db.events                           # what exactly is wrong?
$ iceops tune db.events --engine spark --yes        # do everything, in the right order
$ iceops scan --catalog prod                        # verify it converged
```

`tune` replaces running the four fix commands by hand. Run the individual commands when
you want fine control over one operation; run `tune` for routine maintenance.

Coming next: engine backend for expire/clean/rewrite (not just compact), native compact,
`iceops.yaml` policies + `apply`.
