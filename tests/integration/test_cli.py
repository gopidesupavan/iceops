from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from iceops.cli.app import app

runner = CliRunner()


@pytest.fixture(autouse=True)
def _point_cli_at_seeded_catalog(iceops_config, monkeypatch, tmp_path):
    monkeypatch.setenv("ICEOPS_CONFIG", str(iceops_config))
    monkeypatch.chdir(tmp_path)  # keep any project-local .iceops.toml out of the tests


def test_scan_json(seeded_catalog):
    result = runner.invoke(app, ["scan", "--catalog", "test", "--json"])
    payload = json.loads(result.stdout)
    assert payload["catalog"] == "test"
    # other test modules may add tables to the session catalog; scan must see at least these
    assert {r["identifier"] for r in payload["reports"]} >= {"db.messy", "db.healthy"}
    assert result.exit_code == 1  # messy table has findings


def test_doctor_human_output(seeded_catalog):
    result = runner.invoke(app, ["doctor", "db.healthy", "--catalog", "test"])
    assert result.exit_code == 0
    assert "healthy" in result.stdout


def test_doctor_resolves_catalog_prefix(seeded_catalog):
    result = runner.invoke(app, ["doctor", "test.db.healthy"])
    assert result.exit_code == 0


def test_doctor_single_profile_needs_no_catalog_flag(seeded_catalog):
    result = runner.invoke(app, ["doctor", "db.healthy"])
    assert result.exit_code == 0


def test_cost_json(seeded_catalog):
    result = runner.invoke(app, ["cost", "db.messy", "--catalog", "test", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["identifier"] == "db.messy"
    assert payload["live_bytes"] > 0


def test_native_compact_is_still_not_implemented(seeded_catalog):
    result = runner.invoke(app, ["compact", "db.messy", "--catalog", "test"])
    assert result.exit_code == 2


def test_spark_compact_cli_dry_run(seeded_catalog):
    result = runner.invoke(
        app,
        [
            "compact",
            "db.messy",
            "--catalog",
            "test",
            "--engine",
            "spark",
            "--target-file-size",
            "128MB",
        ],
    )
    assert result.exit_code == 1
    assert "DRY RUN" in result.stdout
    assert "via spark" in result.stdout
    assert "plan kind: delegated" in result.stdout
    assert "statement:" in result.stdout
    assert "rewrite_data_files" in result.stdout


def test_spark_compact_cli_json_plan(seeded_catalog):
    result = runner.invoke(
        app, ["compact", "db.messy", "--catalog", "test", "--engine", "spark", "--json"]
    )
    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["identifier"] == "db.messy"
    assert payload["engine"] == "spark"
    assert payload["engine_catalog"] == "test"
    assert payload["action"]["op"] == "compact"
    assert payload["action"]["params"]["target_file_size_bytes"] == 512 * 1024 * 1024
    assert payload["engine_contract"]["plan_kind"] == "delegated"
    assert "rewrite_data_files" in payload["engine_contract"]["statement"]


def test_expire_cli_dry_run_then_execute(seeded_catalog):
    import pyarrow as pa

    name = "db.expirecli"
    try:
        seeded_catalog.drop_table(name)
    except Exception:
        pass
    batch = pa.table({"id": pa.array(range(5), type=pa.int64())})
    table = seeded_catalog.create_table(name, schema=batch.schema)
    for _ in range(4):
        table.append(batch)

    dry = runner.invoke(
        app, ["expire", name, "--catalog", "test", "--retain-last", "2", "--older-than", "0s"]
    )
    assert dry.exit_code == 1  # work planned, nothing done
    assert "DRY RUN" in dry.stdout
    assert "snapshot " in dry.stdout  # literal listing

    run = runner.invoke(
        app,
        ["expire", name, "--catalog", "test", "--retain-last", "2", "--older-than", "0s", "--yes"],
    )
    assert run.exit_code == 0
    assert "expired 2 snapshots" in run.stdout

    again = runner.invoke(
        app, ["expire", name, "--catalog", "test", "--retain-last", "2", "--older-than", "0s"]
    )
    assert again.exit_code == 0  # nothing left to do
    assert "nothing to expire" in again.stdout


def test_rewrite_manifests_cli_dry_run_then_execute(seeded_catalog):
    import pyarrow as pa

    name = "db.fragcli"
    try:
        seeded_catalog.drop_table(name)
    except Exception:
        pass
    batch = pa.table({"id": pa.array(range(10), type=pa.int64())})
    table = seeded_catalog.create_table(name, schema=batch.schema)
    for _ in range(4):
        table.append(batch)

    dry = runner.invoke(app, ["rewrite-manifests", name, "--catalog", "test"])
    assert dry.exit_code == 1
    assert "DRY RUN" in dry.stdout
    assert "consolidate 4 manifests" in dry.stdout

    run = runner.invoke(app, ["rewrite-manifests", name, "--catalog", "test", "--yes"])
    assert run.exit_code == 0
    assert "rewrote manifests: 4 → 1" in run.stdout

    again = runner.invoke(app, ["rewrite-manifests", name, "--catalog", "test"])
    assert again.exit_code == 0
    assert "nothing to rewrite" in again.stdout


def test_rewrite_manifests_cli_bad_size(seeded_catalog):
    result = runner.invoke(
        app, ["rewrite-manifests", "db.messy", "--catalog", "test", "--target-manifest-size", "8xb"]
    )
    assert result.exit_code == 2


def test_clean_orphans_cli_flow(seeded_catalog, tmp_path):
    import datetime as dt
    import os
    import shutil
    from pathlib import Path
    from urllib.parse import urlparse

    import pyarrow as pa

    name = "db.orphcli"
    try:
        seeded_catalog.drop_table(name)
    except Exception:
        pass
    batch = pa.table({"id": pa.array(range(10), type=pa.int64())})
    table = seeded_catalog.create_table(name, schema=batch.schema)
    table.append(batch)
    table = seeded_catalog.load_table(name)
    data_dir = Path(urlparse(table.location()).path) / "data"
    source = sorted(data_dir.glob("*.parquet"))[0]
    planted = data_dir / "00000-0-cli-orphan.parquet"
    shutil.copy(source, planted)
    old = (dt.datetime.now() - dt.timedelta(days=30)).timestamp()
    os.utime(planted, (old, old))

    dry = runner.invoke(app, ["clean-orphans", name, "--catalog", "test"])
    assert dry.exit_code == 1
    assert "DRY RUN" in dry.stdout
    assert "cli-orphan" in dry.stdout
    assert planted.exists()

    run = runner.invoke(app, ["clean-orphans", name, "--catalog", "test", "--yes"])
    assert run.exit_code == 0
    assert "deleted 1 files" in run.stdout
    assert not planted.exists()

    again = runner.invoke(app, ["clean-orphans", name, "--catalog", "test"])
    assert again.exit_code == 0
    assert "nothing to clean" in again.stdout


def test_expire_cli_bad_duration(seeded_catalog):
    result = runner.invoke(app, ["expire", "db.messy", "--catalog", "test", "--older-than", "7x"])
    assert result.exit_code == 2


def test_version():
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.stdout.strip()
