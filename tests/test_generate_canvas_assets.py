from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.schemas.canvas import CanvasGraph
from app.schemas.dbt import DbtArtifactBundle, DbtArtifactFile
from app.services.databricks_sql import DatabricksConnectionParams
from scripts.generate_canvas_assets import run_generate_canvas_assets

CANVAS_FIXTURE = Path(__file__).parent / "fixtures" / "canvas" / "simple_model.json"


class StubTranslator:
    def __init__(self, bundle: DbtArtifactBundle) -> None:
        self.bundle = bundle
        self.calls = 0

    def translate(self, graph: CanvasGraph) -> DbtArtifactBundle:
        self.calls += 1
        return self.bundle


@pytest.fixture
def canvas_graph() -> CanvasGraph:
    payload = json.loads(CANVAS_FIXTURE.read_text(encoding="utf-8"))
    return CanvasGraph.parse_obj(payload)


def test_run_generate_canvas_assets_skips_testgen_and_writes_files(tmp_path: Path, canvas_graph: CanvasGraph) -> None:
    bundle = DbtArtifactBundle(
        files=[
            DbtArtifactFile(path="models/staging/dim_customers.sql", content="select 1\n"),
            DbtArtifactFile(path="target/manifest.json", content="{}\n"),
        ]
    )
    translator = StubTranslator(bundle)

    result = run_generate_canvas_assets(
        graph=canvas_graph,
        output_dir=tmp_path,
        skip_testgen=True,
        generate_manifest=False,
        translator=translator,
    )

    assert result is bundle
    assert translator.calls == 1
    assert (tmp_path / "models/staging/dim_customers.sql").read_text(encoding="utf-8") == "select 1\n"
    assert (tmp_path / "target/manifest.json").read_text(encoding="utf-8") == "{}\n"


def test_run_generate_canvas_assets_syncs_testgen(monkeypatch: pytest.MonkeyPatch, canvas_graph: CanvasGraph) -> None:
    bundle = DbtArtifactBundle(files=[DbtArtifactFile(path="canvas.json", content="{}")])
    translator = StubTranslator(bundle)

    captured = {}

    def fake_sync(graph: CanvasGraph, params: DatabricksConnectionParams, *, translator: StubTranslator):
        captured["graph"] = graph
        captured["params"] = params
        captured["translator_calls_before"] = translator.calls
        translator.calls += 1
        return bundle

    monkeypatch.setattr("scripts.generate_canvas_assets.sync_canvas_graph_to_testgen", fake_sync)

    params = DatabricksConnectionParams(
        workspace_host="example",
        http_path="/sql/foo",
        access_token="token",
        catalog="main",
        schema_name="default",
        data_quality_schema="dq",
    )

    result = run_generate_canvas_assets(
        graph=canvas_graph,
        skip_testgen=False,
        generate_manifest=False,
        databricks_params=params,
        translator=translator,
    )

    assert result is bundle
    assert captured["graph"] is canvas_graph
    assert captured["params"] is params
    assert captured["translator_calls_before"] == 0
    assert translator.calls == 1


def test_run_generate_canvas_assets_requires_params(canvas_graph: CanvasGraph) -> None:
    with pytest.raises(ValueError):
        run_generate_canvas_assets(
            graph=canvas_graph,
            skip_testgen=False,
            generate_manifest=False,
        )
