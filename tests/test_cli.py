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
    assert {r["identifier"] for r in payload["reports"]} == {"db.messy", "db.healthy"}
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


def test_stub_commands_exit_nonzero(seeded_catalog):
    result = runner.invoke(app, ["compact", "db.messy", "--catalog", "test"])
    assert result.exit_code == 2


def test_version():
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.stdout.strip()
