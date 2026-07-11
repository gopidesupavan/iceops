# iceops command reference

Minimal reference for everything shipped today. Every command supports `--json` for
machine-readable output. Exit codes everywhere: `0` healthy / done / nothing to do,
`1` findings exist or work was planned but not executed (dry run), `2` error.

Fix commands (`expire`, `rewrite-manifests`, `clean-orphans`) are **dry-run by
default** — they print exactly what would happen and change nothing until `--yes`.
They refuse tables managed by another optimizer (Amoro, S3 Tables, …) unless `--force`.

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
```

Unknown names fall through to [PyIceberg's own config](https://py.iceberg.apache.org/configuration/).
With a single profile configured, `--catalog` can be omitted. Tables are addressed as
`namespace.table` or fully qualified `catalog.namespace.table`.

Try everything against a local demo lakehouse: `uv run python examples/demo.py`

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

Options: `--dollars-per-gb-month 0.023`. Stale is an upper bound; unknowns are
reported as notes, never silently zeroed.

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

## iceops catalogs / iceops version

List configured profiles / print the version.

---

## Typical maintenance session

```console
$ iceops scan --catalog prod                      # who needs help?
$ iceops doctor db.events                          # what exactly is wrong?
$ iceops rewrite-manifests db.events --yes         # fix the index
$ iceops expire db.events --yes                    # drop old versions (safe defaults)
$ iceops clean-orphans db.events --yes             # reclaim the bytes (3d age guard)
$ iceops scan --catalog prod                       # verify it converged
```

Coming next: `compact` (merge small data files), `tune` (all of the above in the right
order), `iceops.yaml` policies + `apply`.
