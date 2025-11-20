from __future__ import annotations

from typing import Optional

from app.schemas.canvas import CanvasGraph
from app.schemas.dbt import DbtArtifactBundle
from app.services.canvas_testgen_mapper import ManifestToDataQualityMapper
from app.services.canvas_translator import CanvasToDbtTranslator
from app.services.data_quality_metadata import ensure_data_quality_metadata
from app.services.dbt_manifest_loader import load_manifest
from app.services.databricks_sql import DatabricksConnectionParams


class CanvasTestGenSyncError(RuntimeError):
    """Raised when exporting a Canvas graph to TestGen metadata fails."""


def sync_canvas_graph_to_testgen(
    graph: CanvasGraph,
    params: DatabricksConnectionParams,
    *,
    translator: Optional[CanvasToDbtTranslator] = None,
    mapper: Optional[ManifestToDataQualityMapper] = None,
) -> DbtArtifactBundle:
    """Export the Canvas graph to dbt artifacts and update TestGen metadata."""

    translator = translator or CanvasToDbtTranslator(generate_manifest=True)
    bundle = translator.translate(graph)

    file_map = bundle.file_map()
    manifest_content = file_map.get("target/manifest.json")
    if not manifest_content:
        raise CanvasTestGenSyncError(
            "Canvas translator did not produce target/manifest.json; ensure manifest generation is enabled."
        )

    try:
        manifest = load_manifest(manifest_content)
    except Exception as exc:  # pragma: no cover - parsing guard
        raise CanvasTestGenSyncError("Failed to parse dbt manifest") from exc

    mapper = mapper or ManifestToDataQualityMapper()
    seed = mapper.build_seed(manifest, default_catalog=params.catalog)

    ensure_data_quality_metadata(params, seed_override=seed)
    return bundle


__all__ = ["CanvasTestGenSyncError", "sync_canvas_graph_to_testgen"]
