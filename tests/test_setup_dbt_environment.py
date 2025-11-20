from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from scripts.setup_dbt_environment import main as setup_main


class FakeCompletedProcess:
    def __init__(self, returncode: int = 0) -> None:
        self.returncode = returncode


def test_setup_dbt_environment_writes_duckdb_profile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    commands = []

    def fake_run(cmd, check):
        commands.append(cmd)
        if check and cmd[0] != "python":
            raise AssertionError("Unexpected command")
        return FakeCompletedProcess()

    monkeypatch.setattr(subprocess, "run", fake_run)

    profiles_dir = tmp_path / ".dbt"
    result = setup_main(
        [
            "--adapter",
            "duckdb",
            "--profiles-dir",
            str(profiles_dir),
            "--profile-name",
            "canvas",
        ]
    )

    assert result == 0
    assert len(commands) == 2  # pip install dbt-core + adapter
    profile_content = (profiles_dir / "profiles.yml").read_text(encoding="utf-8")
    assert "type: duckdb" in profile_content
    assert "canvas-exporter.duckdb" in profile_content


def test_setup_dbt_environment_requires_databricks_params(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subprocess, "run", lambda cmd, check: FakeCompletedProcess())

    result = setup_main(
        [
            "--adapter",
            "databricks",
            "--profiles-dir",
            str(tmp_path / ".dbt"),
        ]
    )

    assert result == 1