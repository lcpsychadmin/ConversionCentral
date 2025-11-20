from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.schemas.canvas import CanvasGraph
from app.schemas.dbt import DbtArtifactBundle, DbtArtifactFile
from app.services.canvas_testgen_sync import (
    CanvasTestGenSyncError,
    sync_canvas_graph_to_testgen,
)
from app.services.databricks_sql import DatabricksConnectionParams

CANVAS_FIXTURE = Path(__file__).parent / "fixtures" / "canvas" / "simple_model.json"
MANIFEST_FIXTURE = Path(__file__).parent / "fixtures" / "dbt" / "manifest" / "simple_manifest.json"


class DummyTranslator:
    def __init__(self, bundle: DbtArtifactBundle) -> None:
        self._bundle = bundle

    def translate(self, graph: CanvasGraph) -> DbtArtifactBundle:  # pragma: no cover - interface only
        return self._bundle


def load_canvas_graph() -> CanvasGraph:
    payload = json.loads(CANVAS_FIXTURE.read_text(encoding="utf-8"))
    return CanvasGraph.parse_obj(payload)


def build_bundle(with_manifest: bool = True) -> DbtArtifactBundle:
    files = [
        DbtArtifactFile(path="canvas.json", content=CANVAS_FIXTURE.read_text(encoding="utf-8")),
    ]
    if with_manifest:
        files.append(
            DbtArtifactFile(path="target/manifest.json", content=MANIFEST_FIXTURE.read_text(encoding="utf-8"))
        )
    return DbtArtifactBundle(files=files)


@pytest.fixture
def databricks_params() -> DatabricksConnectionParams:
    return DatabricksConnectionParams(
        workspace_host="example.cloud.databricks.com",
        http_path="/sql/1.0/warehouses/abc",
        access_token="token",
        catalog="main",
        schema_name="default",
        data_quality_schema="dq",
    )


def test_sync_canvas_updates_testgen(monkeypatch: pytest.MonkeyPatch, databricks_params: DatabricksConnectionParams) -> None:
    graph = load_canvas_graph()
    bundle = build_bundle()
    translator = DummyTranslator(bundle)

    captured_seed = {}

    def fake_ensure(params, *, seed_override=None):  # pragma: no cover - simple test helper
        captured_seed["params"] = params
        captured_seed["seed"] = seed_override

    monkeypatch.setattr(
        "app.services.canvas_testgen_sync.ensure_data_quality_metadata",
        fake_ensure,
        raising=False,
    )

    result_bundle = sync_canvas_graph_to_testgen(graph, databricks_params, translator=translator)
    assert result_bundle is bundle

    assert captured_seed["params"] == databricks_params
    seed = captured_seed["seed"]
    assert seed is not None
    assert len(seed.projects) == 1
    project = seed.projects[0]
    assert project.project_key == "canvas:canvas_demo"
    connection_tables = [table for conn in project.connections for group in conn.table_groups for table in group.tables]
    table_names = {table.table_name for table in connection_tables}
    assert table_names == {"crm_customers", "dim_customers"}


def test_sync_canvas_requires_manifest(monkeypatch: pytest.MonkeyPatch, databricks_params: DatabricksConnectionParams) -> None:
    graph = load_canvas_graph()
    bundle = build_bundle(with_manifest=False)
    translator = DummyTranslator(bundle)

    monkeypatch.setattr(
        "app.services.canvas_testgen_sync.ensure_data_quality_metadata",
        lambda *_, **__: None,
        raising=False,
    )

    with pytest.raises(CanvasTestGenSyncError):
        sync_canvas_graph_to_testgen(graph, databricks_params, translator=translator)
