# Quickstart

Five minutes from install to a fleet health report.

## Install

```bash
pip install iceops
# or, without installing:  uvx iceops --help
```

Engine backends are optional extras (only if you'll delegate to Spark/Trino):

```bash
pip install "iceops[spark]"    # compaction and maintenance via Spark
pip install "iceops[trino]"    # via Trino
pip install "iceops[glue]"     # AWS Glue catalog (via pyiceberg[glue]/boto3)
```

## Point iceops at a catalog

iceops reads catalog profiles from `.iceops.toml` (project) or `~/.iceops/config.toml`
(user). Anything [PyIceberg](https://py.iceberg.apache.org/configuration/) can reach,
iceops can reach.

```toml
[catalogs.prod]
type = "rest"
uri = "https://polaris.example.com/api/catalog"
credential = "…"
```

Catalog support comes from PyIceberg, so any catalog PyIceberg supports works: any
REST-spec catalog (Polaris, Nessie, Gravitino, Lakekeeper), plus SQL and Hive. AWS Glue is
available via `iceops[glue]` (a pass-through to `pyiceberg[glue]`/boto3) — supported through
PyIceberg but not yet exercised by the iceops test suite.

## Try it on a throwaway local lakehouse

No catalog handy? Build a deliberately-unhealthy demo warehouse:

```bash
git clone https://github.com/gopidesupavan/iceops && cd iceops
uv sync
uv run python examples/demo.py
```

This creates `db.events` (60 tiny commits — a streaming-ingestion victim) and a healthy
`db.orders`, plus a `demo` catalog profile.

## 1. Scan the fleet

```console
$ iceops scan --catalog demo
                         catalog 'demo' — 2 tables
┏━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━┳━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━┓
┃ table     ┃ status  ┃ files ┃ small ┃ snapshots ┃    size ┃ top issue   ┃
┡━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━╇━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━┩
│ db.events │  warn   │    60 │  100% │        60 │ 359.3KB │ small-files │
│ db.orders │ healthy │     3 │  100% │         3 │   2.1MB │ healthy     │
└───────────┴─────────┴───────┴───────┴───────────┴─────────┴─────────────┘
warn: 1  healthy: 1
```

## 2. Diagnose the problem table

```console
$ iceops doctor db.events --catalog demo
db.events  warn
  60 data files (359.3KB, avg 6.0KB)  ·  0 delete files  ·  60 snapshots  ·  60 manifests  ·  1 partitions
  streaming writer detected (frequent commits)

  file sizes
       <1MB  ██████████████████████████████ 60

  findings
  ● warn 60 of 60 data files (100%) are under 32MB (avg 6.0KB)
  ● warn 60 snapshots retained
  ● warn 60 manifest files averaging 1 data files each — query planning reads every one
  ● warn ~2.0MB in the table location is not referenced by any snapshot
  ● info write.metadata.delete-after-commit.enabled is not enabled
```

## 3. Price the waste

```console
$ iceops cost db.events --catalog demo
                            storage cost — db.events
┏━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ category      ┃   bytes ┃ meaning                                            ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ live          │ 359.3KB │ referenced by current snapshot                     │
│ stale         │      0B │ only reachable via old snapshots — freed by expire │
│ orphan (est.) │   2.0MB │ referenced by nothing — freed by clean-orphans     │
└───────────────┴─────────┴────────────────────────────────────────────────────┘
estimated waste: $0.0/month at $0.023/GB-month
```

## 4. Fix it — dry-run first, always

Every fix command prints exactly what it *would* do and changes nothing until `--yes`:

```console
$ iceops rewrite-manifests db.events --catalog demo
plan: consolidate 60 manifests (262.8KB, ~1.0 data files each) into ~1
metadata only — no data files are read or written; one new snapshot is created …
DRY RUN — nothing changed. Add --yes to execute.

$ iceops rewrite-manifests db.events --catalog demo --yes
rewrote manifests: 60 → 1 (snapshot 7402711359425541986)
```

Or run the whole maintenance sequence at once with `tune`:

```console
$ iceops tune db.events --catalog demo --yes
```

## 5. Verify the loop converged

```console
$ iceops scan --catalog demo      # db.events status should improve
```

## Next steps

- **[Command reference](commands.md)** — every command and option
- **[Policy](policy.md)** — encode this as `iceops.yaml` and run it from cron/CI
- **[Concepts](concepts.md)** — why small files, snapshots, and orphans cost you

Every command supports `--json` for machines and returns CI-friendly exit codes
(`0` done, `1` findings or planned-but-dry-run, `2` error).
