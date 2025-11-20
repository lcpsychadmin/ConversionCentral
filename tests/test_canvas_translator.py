from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Dict

import pytest

from app.schemas.canvas import CanvasGraph
from app.services.canvas_translator import CanvasToDbtTranslator

FIXTURES_ROOT = Path(__file__).parent / "fixtures"


def load_canvas_graph(name: str) -> CanvasGraph:
    fixture_path = FIXTURES_ROOT / "canvas" / f"{name}.json"
    with fixture_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return CanvasGraph.parse_obj(payload)


def load_expected_files(name: str) -> Dict[str, str]:
    root = FIXTURES_ROOT / "dbt" / name
    expected: Dict[str, str] = {}
    for file_path in root.rglob("*"):
        if not file_path.is_file():
            continue
        relative_path = file_path.relative_to(root).as_posix()
        with file_path.open("r", encoding="utf-8") as handle:
            expected[relative_path] = handle.read()
    return expected


def test_canvas_translator_generates_expected_bundle() -> None:
    graph = load_canvas_graph("simple_model")
    translator = CanvasToDbtTranslator(generate_manifest=False)

    bundle = translator.translate(graph)
    actual_files = bundle.file_map()
    expected_files = load_expected_files("simple_model")

    # All expected files should exist with identical content
    for path, expected_content in expected_files.items():
        assert path in actual_files, f"Missing generated artifact for {path}"
        assert actual_files[path] == expected_content

    # Canvas manifest should round-trip the graph payload
    assert "canvas.json" in actual_files
    manifest = json.loads(actual_files["canvas.json"])
    assert manifest == graph.dict(by_alias=True, exclude_none=True)

    # No unexpected files aside from canvas manifest
    unexpected = set(actual_files) - set(expected_files) - {"canvas.json"}
    assert not unexpected, f"Translator produced unexpected artifacts: {sorted(unexpected)}"

    # Translator presently omits warnings when none are supplied
    assert bundle.warnings is None


@pytest.mark.skipif(
    os.environ.get("DBT_RUN_PARSING_TESTS") != "1",
    reason="Set DBT_RUN_PARSING_TESTS=1 to enable the dbt parse integration test.",
)
@pytest.mark.integration
def test_translator_bundle_parses_with_dbt(tmp_path: Path) -> None:
    graph = load_canvas_graph("simple_model")
    translator = CanvasToDbtTranslator(generate_manifest=False)

    bundle = translator.translate(graph)

    project_root = tmp_path / "canvas_project"
    for path, content in bundle.file_map().items():
        target_path = project_root / path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(content, encoding="utf-8")

    profiles_root = tmp_path / "profiles"
    profiles_root.mkdir(parents=True, exist_ok=True)
    (profiles_root / "profiles.yml").write_text(
        """
        default:
          outputs:
            dev:
              type: duckdb
              path: ':memory:'
          target: dev
        """.strip()
        + "\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(profiles_root)

    result = subprocess.run(
        ["dbt", "parse"],
        cwd=project_root,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        pytest.fail(
            f"dbt parse failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}",
        )
