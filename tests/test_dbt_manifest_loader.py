from __future__ import annotations

import json
from pathlib import Path

from app.services.dbt_manifest_loader import load_manifest, load_manifest_from_str

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "dbt" / "manifest" / "simple_manifest.json"


def test_load_manifest_from_path() -> None:
    manifest = load_manifest(FIXTURE_PATH)

    assert manifest.metadata is not None
    assert manifest.metadata.project_name == "canvas_demo"
    assert manifest.metadata.adapter_type == "duckdb"

    model_key = "model.canvas_demo.dim_customers"
    assert model_key in manifest.nodes
    node = manifest.nodes[model_key]
    assert node.resource_type == "model"
    assert node.depends_on.nodes == ["source.canvas_demo.crm.crm_customers"]
    assert node.tags == ["staging", "dimensions"]

    source_key = "source.canvas_demo.crm.crm_customers"
    assert source_key in manifest.sources
    source = manifest.sources[source_key]
    assert source.source_name == "crm"
    assert source.meta.get("sourceName") == "crm"


def test_load_manifest_from_str_round_trip() -> None:
    content = FIXTURE_PATH.read_text(encoding="utf-8")
    manifest = load_manifest_from_str(content)

    assert "model.canvas_demo.dim_customers" in manifest.nodes
    assert manifest.nodes["model.canvas_demo.dim_customers"].depends_on.nodes == [
        "source.canvas_demo.crm.crm_customers"
    ]

    payload = json.loads(content)
    manifest_from_mapping = load_manifest(payload)
    assert manifest_from_mapping.metadata.project_name == "canvas_demo"
