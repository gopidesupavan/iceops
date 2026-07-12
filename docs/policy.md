# Policy — maintenance as code (`iceops.yaml` + `iceops apply`)

Instead of running commands by hand, describe your desired maintenance in a checked-in
`iceops.yaml` and let `iceops apply` run it. Policy is **per table**: hot tables get
aggressive maintenance, cold tables gentle, some tables none.

## A complete policy

```yaml
catalog: prod                 # optional; --catalog overrides
# engine: spark               # optional global engine (native by default)

# defaults apply to EVERY table, then each `tables` entry overrides field-by-field.
defaults:
  compact:                    # compact requires an engine
    target-file-size: 512MB
    when: "small-file-ratio > 0.3"
  rewrite-manifests:
    when: "manifest-count > 50"
  expire-snapshots:
    retain-last: 10
    older-than: 7d
  clean-orphans:
    older-than: 3d

# per-table control
tables:
  "db.events":
    engine: spark             # this table maintained by Spark
    expire-snapshots:
      retain-last: 50         # hot table keeps more history (merges over defaults)

  "db.cold_*":
    clean-orphans:
      older-than: 1d          # cold tables: reclaim sooner

  "db.audit":
    disabled: true            # never touch this table
```

## How resolution works

For each table, iceops starts from `defaults` and merges every matching `tables` entry
**field-by-field**, most-specific pattern winning. So `db.events` above keeps the default
`older-than: 7d` while overriding only `retain-last: 50`.

- **`disabled: true`** on any matching entry → the table is skipped entirely.
- **`engine`** resolves global → per-table (a table's engine wins).
- An **op runs only if** its section is present *after merge* **and** its `when:` condition
  passes. An op absent from a table's policy never runs for that table; an op with no
  `when:` always runs.
- Specificity: an exact table name beats a glob; among globs, more literal characters wins.

## The `when:` condition language

A single comparison, `metric OP number`, evaluated against the table's `doctor` metrics.
Operators: `>`, `>=`, `<`, `<=`, `==`, `!=`. It's a tiny parser — no `eval`, no expression
engine — and every `when:` is validated when the file loads, so a typo fails immediately,
not at 2am.

Available metrics: `small-file-ratio`, `small-file-count`, `data-file-count`,
`delete-file-count`, `delete-ratio`, `snapshot-count`, `manifest-count`, `partition-count`,
`partition-file-skew`, `oldest-snapshot-age-days`, `newest-snapshot-age-days`,
`total-data-bytes`, `avg-file-bytes`.

A metric with an unknown value (e.g. skew on a single-partition table) evaluates the
condition to **false** — iceops never acts on unknowns.

## Running it

`apply` is dry-run by default. It lists **every table in scope** with the reason for each
decision, so you see the full blast radius before executing:

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

```console
$ iceops apply --policy iceops.yaml --yes
```

apply composes the four operators in the safe order (compact → rewrite-manifests → expire →
clean-orphans); it adds no new behavior. If a step fails for a table, apply stops there for
that table (`halted at <step>`) and never runs later steps on an unexpected state. Options:
`--policy iceops.yaml`, `--catalog`, `--yes`, `--force`, `--json`.

## Running it from a scheduler

`apply` is one-shot — scheduling is your cron/Airflow/CI, not iceops. Check the policy into
git, review changes as a pull request, and run it on a schedule.

**GitHub Actions (nightly):**

```yaml
name: iceberg-maintenance
on:
  schedule: [{cron: "0 3 * * *"}]   # 03:00 UTC daily
jobs:
  maintain:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install iceops
      - run: iceops apply --policy iceops.yaml --yes
        env:
          # Catalog config via PyIceberg env vars (used when the catalog is not defined
          # in .iceops.toml). Format: PYICEBERG_CATALOG__<name>__<key> → catalog.<name>.<key>.
          # A REST catalog needs at least type + uri:
          PYICEBERG_CATALOG__PROD__TYPE: rest
          PYICEBERG_CATALOG__PROD__URI: ${{ secrets.CATALOG_URI }}
          PYICEBERG_CATALOG__PROD__CREDENTIAL: ${{ secrets.CATALOG_CREDENTIAL }}
```

> The `PYICEBERG_CATALOG__*` variables are read by PyIceberg itself; iceops falls through
> to PyIceberg's configuration when a catalog name isn't defined in `.iceops.toml`. See the
> [PyIceberg configuration docs](https://py.iceberg.apache.org/configuration/) for the full
> key list per catalog type.

**cron:**

```cron
0 3 * * *  cd /path/to/repo && iceops apply --policy iceops.yaml --yes >> maintenance.log 2>&1
```

Because a single pass respects clean-orphans' age threshold, running on a schedule is the
intended pattern: each night converges the fleet a little further and reclaims what
previous nights unreferenced.
