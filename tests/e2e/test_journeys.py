"""End-to-end journeys: the REAL iceops binary, as a user runs it in production.

No Python-level shortcuts: every step spawns the installed `iceops` console script in a
subprocess, config is discovered from ./.iceops.toml in the working directory (the real
discovery path — the integration CLI tests bypass it via env var), output is parsed from
stdout, and exit codes are asserted exactly as a cron job or CI pipeline would key on
them. House rule applies: no mocks, real warehouse, real files, real deletions.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pytest
from pyiceberg.catalog import load_catalog

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "examples"))
from table_factory import VARIATIONS, build_all  # noqa: E402

ICEOPS_BIN = shutil.which("iceops")


def iceops(*args: str, cwd: Path) -> subprocess.CompletedProcess:
    command = [ICEOPS_BIN, *args] if ICEOPS_BIN else [sys.executable, "-m", "iceops.cli.app", *args]
    env = {k: v for k, v in os.environ.items() if k != "ICEOPS_CONFIG"}
    result = subprocess.run(command, cwd=cwd, env=env, capture_output=True, text=True, timeout=120)
    # execution log: pytest shows this on failure (or live with -s) — exactly what a
    # user would have seen in their terminal
    print(f"\n$ iceops {' '.join(args)}   [exit {result.returncode}]")
    if result.stdout.strip():
        print(result.stdout.rstrip())
    if result.stderr.strip():
        print(f"[stderr] {result.stderr.rstrip()}")
    return result


def make_workspace(tmp_path_factory, name: str) -> tuple[Path, object]:
    """A self-contained production-like workspace: warehouse + ./.iceops.toml."""
    workspace = tmp_path_factory.mktemp(name)
    warehouse = workspace / "warehouse"
    warehouse.mkdir()
    (workspace / ".iceops.toml").write_text(
        "[catalogs.e2e]\n"
        'type = "sql"\n'
        f'uri = "sqlite:///{warehouse}/catalog.db"\n'
        f'warehouse = "file://{warehouse}"\n'
    )
    catalog = load_catalog(
        "e2e", type="sql", uri=f"sqlite:///{warehouse}/catalog.db", warehouse=f"file://{warehouse}"
    )
    catalog.create_namespace("db")
    return workspace, catalog


def batch(start: int = 0, rows: int = 300) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(range(start, start + rows), type=pa.int64()),
            "value": pa.array([float(i) for i in range(rows)], type=pa.float64()),
        }
    )


def backdate(path: Path, days: int = 30) -> None:
    old = (dt.datetime.now() - dt.timedelta(days=days)).timestamp()
    os.utime(path, (old, old))


@pytest.fixture(scope="module")
def journey_workspace(tmp_path_factory):
    workspace, catalog = make_workspace(tmp_path_factory, "journey")
    table = catalog.create_table("db.events", schema=batch(1).schema)
    for i in range(30):  # a streaming-ingestion victim
        table.append(batch(i * 300))
    # a failed write left a big old orphan behind (padded past the 1MB check threshold)
    data_dir = Path(str(table.location()).removeprefix("file://")) / "data"
    orphan = data_dir / "00000-0-failed-write.parquet"
    shutil.copy(sorted(data_dir.glob("*.parquet"))[0], orphan)
    with orphan.open("ab") as fh:
        fh.write(b"\0" * 2 * 1024 * 1024)
    backdate(orphan)
    return workspace


class TestMaintenanceJourney:
    """The full production loop: scan -> diagnose -> fix -> verify convergence."""

    @pytest.fixture()
    def workspace(self, journey_workspace):
        return journey_workspace

    def test_01_scan_finds_problems_and_exits_1(self, workspace):
        result = iceops("scan", "--catalog", "e2e", cwd=workspace)
        assert result.returncode == 1, result.stderr
        assert "db.events" in result.stdout and "warn" in result.stdout

    def test_02_doctor_json_names_the_findings(self, workspace):
        result = iceops("doctor", "db.events", "--catalog", "e2e", "--json", cwd=workspace)
        assert result.returncode == 1
        report = json.loads(result.stdout)
        assert report["status"] == "warn"
        check_ids = {f["check_id"] for f in report["findings"]}
        assert {"small-files", "orphan-files"} <= check_ids

    def test_03_expire_dry_run_plans_but_does_nothing(self, workspace):
        result = iceops(
            "expire", "db.events", "--catalog", "e2e", "--older-than", "0s", cwd=workspace
        )
        assert result.returncode == 1
        assert "DRY RUN" in result.stdout
        again = iceops("doctor", "db.events", "--catalog", "e2e", "--json", cwd=workspace)
        assert json.loads(again.stdout)["metrics"]["snapshot_count"] == 30  # untouched

    def test_04_fix_pipeline_executes_cleanly(self, workspace):
        for args in (
            (
                "expire",
                "db.events",
                "--catalog",
                "e2e",
                "--retain-last",
                "5",
                "--older-than",
                "0s",
                "--yes",
            ),
            ("rewrite-manifests", "db.events", "--catalog", "e2e", "--yes"),
            ("clean-orphans", "db.events", "--catalog", "e2e", "--older-than", "0s", "--yes"),
        ):
            result = iceops(*args, cwd=workspace)
            assert result.returncode == 0, f"{args[0]} failed: {result.stdout}{result.stderr}"

    def test_05_loop_converged_and_data_survived(self, workspace):
        report = json.loads(
            iceops("doctor", "db.events", "--catalog", "e2e", "--json", cwd=workspace).stdout
        )
        check_ids = {f["check_id"] for f in report["findings"]}
        assert "snapshot-bloat" not in check_ids
        assert "manifest-fragmentation" not in check_ids
        assert "orphan-files" not in check_ids
        assert report["metrics"]["manifest_count"] == 1
        # small-files remains: that is compact's (E5) job — the loop is honest about it
        assert "small-files" in check_ids

        warehouse = workspace / "warehouse"
        catalog = load_catalog(
            "e2e",
            type="sql",
            uri=f"sqlite:///{warehouse}/catalog.db",
            warehouse=f"file://{warehouse}",
        )
        assert catalog.load_table("db.events").scan().to_arrow().num_rows == 9000

    def test_06_loop_converges_and_then_holds(self, workspace):
        # Second --yes pass: the rewrite added a snapshot, so expire legitimately has
        # one more candidate — production loops converge over passes, they don't
        # freeze after one. Everything must still exit 0.
        for args in (
            (
                "expire",
                "db.events",
                "--catalog",
                "e2e",
                "--retain-last",
                "5",
                "--older-than",
                "0s",
                "--yes",
            ),
            ("rewrite-manifests", "db.events", "--catalog", "e2e", "--yes"),
            ("clean-orphans", "db.events", "--catalog", "e2e", "--older-than", "0s", "--yes"),
        ):
            assert iceops(*args, cwd=workspace).returncode == 0

        # Third pass, dry: NOW the loop must be a fixed point — nothing left to do.
        for args in (
            ("expire", "db.events", "--catalog", "e2e", "--retain-last", "5", "--older-than", "0s"),
            ("rewrite-manifests", "db.events", "--catalog", "e2e"),
            ("clean-orphans", "db.events", "--catalog", "e2e", "--older-than", "0s"),
        ):
            result = iceops(*args, cwd=workspace)
            assert result.returncode == 0, f"{args[0]} did not converge: {result.stdout}"
            assert "nothing" in result.stdout.lower()


@pytest.fixture(scope="module")
def shapes_workspace(tmp_path_factory):
    """Every partition variation from the compatibility lab, in a real CLI workspace."""
    workspace, catalog = make_workspace(tmp_path_factory, "shapes")
    results = build_all(catalog, "lab")
    return workspace, results


SHAPE_NAMES = [v.name for v in VARIATIONS]


class TestPartitionedShapesJourney:
    """The full CLI pipeline over every table shape: identity/day/month/bucket/truncate
    partitions, spec evolution, schema evolution, overwrite. What the integration matrix
    proves in-process, this proves through the real binary."""

    def _catalog(self, workspace: Path):
        warehouse = workspace / "warehouse"
        return load_catalog(
            "e2e",
            type="sql",
            uri=f"sqlite:///{warehouse}/catalog.db",
            warehouse=f"file://{warehouse}",
        )

    def test_scan_sees_every_shape(self, shapes_workspace):
        workspace, results = shapes_workspace
        result = iceops("scan", "--catalog", "e2e", "--pattern", "lab.*", cwd=workspace)
        assert result.returncode in (0, 1), result.stderr
        for identifier, status in results.items():
            if status == "ok":
                assert identifier.split(".")[1][:12] in result.stdout.replace("\n", "")

    @pytest.mark.parametrize("name", SHAPE_NAMES)
    def test_full_pipeline_preserves_every_shape(self, shapes_workspace, name):
        workspace, results = shapes_workspace
        identifier = f"lab.{name}"
        if results[identifier] != "ok":
            pytest.skip(f"shape not buildable here: {results[identifier]}")

        catalog = self._catalog(workspace)
        table = catalog.load_table(identifier)
        rows_before = table.scan().to_arrow().num_rows
        specs_before = len(table.metadata.partition_specs)

        for args in (
            (
                "expire",
                identifier,
                "--catalog",
                "e2e",
                "--retain-last",
                "1",
                "--older-than",
                "0s",
                "--yes",
            ),
            ("rewrite-manifests", identifier, "--catalog", "e2e", "--yes"),
            ("clean-orphans", identifier, "--catalog", "e2e", "--older-than", "0s", "--yes"),
        ):
            result = iceops(*args, cwd=workspace)
            assert result.returncode == 0, f"{name}/{args[0]}: {result.stdout}{result.stderr}"

        table = self._catalog(workspace).load_table(identifier)
        assert table.scan().to_arrow().num_rows == rows_before, f"{name} lost rows"
        assert len(table.metadata.partition_specs) == specs_before, f"{name} lost a spec"

        doctor_report = json.loads(
            iceops("doctor", identifier, "--catalog", "e2e", "--json", cwd=workspace).stdout
        )
        check_ids = {f["check_id"] for f in doctor_report["findings"]}
        assert "orphan-files" not in check_ids, f"{name}: orphans left behind"

    def test_partition_pruning_survives_the_pipeline(self, shapes_workspace):
        """After maintenance through the real CLI, partition filters must still work."""
        workspace, results = shapes_workspace
        if results["lab.part_identity"] != "ok":
            pytest.skip("identity shape not buildable")
        table = self._catalog(workspace).load_table("lab.part_identity")
        alpha_rows = table.scan(row_filter="category = 'alpha'").to_arrow().num_rows
        total_rows = table.scan().to_arrow().num_rows
        assert 0 < alpha_rows < total_rows  # the filter genuinely prunes


@pytest.fixture(scope="module")
def reclaim_workspace(tmp_path_factory):
    workspace, catalog = make_workspace(tmp_path_factory, "reclaim")
    table = catalog.create_table("db.state", schema=batch(1).schema)
    for i in range(4):
        table.append(batch(i * 300))
    table.overwrite(batch(90_000, rows=500))  # copy-on-write: old files become stale
    return workspace


class TestStorageReclaimJourney:
    """The CDC/overwrite story: stale bytes exist, expire+clean reclaim them physically."""

    @pytest.fixture()
    def workspace(self, reclaim_workspace):
        return reclaim_workspace

    def test_01_cost_shows_real_stale_bytes(self, workspace):
        report = json.loads(
            iceops("cost", "db.state", "--catalog", "e2e", "--json", cwd=workspace).stdout
        )
        assert report["stale_bytes"] > 0

    def test_02_expire_and_clean_physically_reclaim(self, workspace):
        parquet_before = len(list((workspace / "warehouse").rglob("*.parquet")))
        assert (
            iceops(
                "expire",
                "db.state",
                "--catalog",
                "e2e",
                "--retain-last",
                "1",
                "--older-than",
                "0s",
                "--yes",
                cwd=workspace,
            ).returncode
            == 0
        )
        result = iceops(
            "clean-orphans",
            "db.state",
            "--catalog",
            "e2e",
            "--older-than",
            "0s",
            "--yes",
            cwd=workspace,
        )
        assert result.returncode == 0
        parquet_after = len(list((workspace / "warehouse").rglob("*.parquet")))
        assert parquet_after < parquet_before  # storage was ACTUALLY freed

    def test_03_survivors_are_exactly_the_overwrite(self, workspace):
        report = json.loads(
            iceops("cost", "db.state", "--catalog", "e2e", "--json", cwd=workspace).stdout
        )
        assert report["stale_bytes"] == 0
        warehouse = workspace / "warehouse"
        catalog = load_catalog(
            "e2e",
            type="sql",
            uri=f"sqlite:///{warehouse}/catalog.db",
            warehouse=f"file://{warehouse}",
        )
        assert catalog.load_table("db.state").scan().to_arrow().num_rows == 500


@pytest.fixture(scope="module")
def tune_workspace(tmp_path_factory):
    workspace, catalog = make_workspace(tmp_path_factory, "tune")
    table = catalog.create_table("db.events", schema=batch(1).schema)
    for i in range(20):  # fragmented: 20 manifests
        table.append(batch(i * 300))
    return workspace


class TestTuneJourney:
    """The flagship: one command replaces the manual maintenance pipeline."""

    @pytest.fixture()
    def workspace(self, tune_workspace):
        return tune_workspace

    def _doctor(self, workspace):
        return json.loads(
            iceops("doctor", "db.events", "--catalog", "e2e", "--json", cwd=workspace).stdout
        )

    def test_01_dry_run_plans_and_skips_compact_without_engine(self, workspace):
        result = iceops("tune", "db.events", "--catalog", "e2e", cwd=workspace)
        assert result.returncode == 1  # actionable, but dry run
        assert "DRY RUN" in result.stdout
        assert "skipped — no --engine" in result.stdout
        assert self._doctor(workspace)["metrics"]["manifest_count"] == 20  # untouched

    def test_02_one_command_converges_the_table(self, workspace):
        # older-than 0s so expire acts on this fresh table too
        result = iceops(
            "tune", "db.events", "--catalog", "e2e", "--older-than", "0s", "--yes", cwd=workspace
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert "tuned: ran" in result.stdout

        report = self._doctor(workspace)
        check_ids = {f["check_id"] for f in report["findings"]}
        assert "manifest-fragmentation" not in check_ids
        assert "snapshot-bloat" not in check_ids
        assert report["metrics"]["manifest_count"] == 1

        warehouse = workspace / "warehouse"
        catalog = load_catalog(
            "e2e",
            type="sql",
            uri=f"sqlite:///{warehouse}/catalog.db",
            warehouse=f"file://{warehouse}",
        )
        assert catalog.load_table("db.events").scan().to_arrow().num_rows == 6000

    def test_03_second_run_is_a_fixed_point(self, workspace):
        result = iceops(
            "tune", "db.events", "--catalog", "e2e", "--older-than", "0s", cwd=workspace
        )
        assert result.returncode == 0
        assert "nothing to tune" in result.stdout


@pytest.fixture(scope="module")
def safety_workspace(tmp_path_factory):
    workspace, catalog = make_workspace(tmp_path_factory, "safety")
    table = catalog.create_table("db.managed", schema=batch(1).schema)
    table.append(batch())
    with table.transaction() as tx:
        tx.set_properties({"self-optimizing.enabled": "true"})  # looks Amoro-managed
    return workspace


class TestSafetyJourney:
    """The guard rails, exercised exactly as an operator would hit them."""

    @pytest.fixture()
    def workspace(self, safety_workspace):
        return safety_workspace

    def test_managed_table_is_refused_with_exit_2(self, workspace):
        result = iceops(
            "expire", "db.managed", "--catalog", "e2e", "--older-than", "0s", cwd=workspace
        )
        assert result.returncode == 2
        assert "managed by amoro" in result.stderr.lower()

    def test_force_overrides_but_still_dry_runs(self, workspace):
        result = iceops(
            "expire",
            "db.managed",
            "--catalog",
            "e2e",
            "--older-than",
            "0s",
            "--force",
            cwd=workspace,
        )
        assert result.returncode == 0  # nothing to expire (1 snapshot), but not refused

    def test_bad_input_exits_2(self, workspace):
        assert (
            iceops(
                "expire", "db.managed", "--catalog", "e2e", "--older-than", "banana", cwd=workspace
            ).returncode
            == 2
        )
        assert iceops("doctor", "db.nope", "--catalog", "e2e", cwd=workspace).returncode == 2

    def test_metadata_json_survives_aggressive_cleaning(self, workspace):
        warehouse = workspace / "warehouse"
        before = len(list(warehouse.rglob("*.metadata.json")))
        iceops(
            "clean-orphans",
            "db.managed",
            "--catalog",
            "e2e",
            "--older-than",
            "0s",
            "--force",
            "--yes",
            cwd=workspace,
        )
        assert len(list(warehouse.rglob("*.metadata.json"))) >= before
