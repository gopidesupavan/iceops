# Contributing to iceops

Thanks for your interest. iceops is a CLI-first tool for Apache Iceberg table maintenance,
and contributions — bug reports, docs, catalog/engine coverage, new checks — are welcome.

## Development setup

iceops uses [uv](https://docs.astral.sh/uv/). No global installs needed.

```bash
git clone https://github.com/gopidesupavan/iceops
cd iceops
uv sync --all-groups          # base + dev deps
uv run pytest -q              # unit + integration (fast, no cluster)
uv run prek run --all-files   # ruff lint + format, mypy, hygiene hooks
```

Try it against a throwaway local lakehouse:

```bash
uv run python examples/demo.py
uv run iceops scan --catalog demo
```

## Test tiers

- `tests/unit/` — pure functions over plain data, no I/O. Fast; run these constantly.
- `tests/integration/` — real SQLite-catalog warehouses, real files, real commits. No
  mocks: hard cases are built physically (planted orphans, concurrent commits).
- `tests/e2e/` — the real `iceops` binary via subprocess; production behavior with `--yes`.
- Engine labs (gated) — real Spark / Trino:
  `ICEOPS_RUN_SPARK=1 uv run pytest tests/integration/test_spark_compact_lab.py`,
  `ICEOPS_RUN_TRINO=1 uv run pytest tests/integration/test_trino_compact_lab.py`
  (Trino needs Docker; it spins up a REST catalog + MinIO + Trino stack).

## Principles we hold

- **The Iceberg format layer is always PyIceberg.** iceops writes decision logic, not
  metadata/manifests/commits. A missing primitive upstream → contribute it, don't
  hand-roll spec internals.
- **Destructive ops are dry-run by default**, gated behind `--yes`, and only
  `clean-orphans` deletes physical files.
- **No mocks in integration tests.** Assert the *effect* (a deleted file is gone), not the
  absence of error. e2e means real execution, not dry-run.
- **Operators return typed models and never print;** the CLI renders them.

## Pull requests

1. Branch from `main`.
2. Add tests at the right tier (a fix → integration proof it fixes it; pure logic → unit).
3. `uv run pytest -q` and `uv run prek run --all-files` green.
4. Keep the base install lean — heavy backends go behind extras (`iceops[spark]`, etc.).

By contributing you agree your contributions are licensed under Apache-2.0.
