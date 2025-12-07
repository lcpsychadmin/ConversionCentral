from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Iterable, Optional

from app.schemas.canvas import CanvasGraph
from app.schemas.dbt import DbtArtifactBundle
from app.services.canvas_testgen_sync import (
    CanvasTestGenSyncError,
    sync_canvas_graph_to_testgen,
)
from app.services.canvas_translator import CanvasToDbtTranslator
from app.services.databricks_sql import DatabricksConnectionParams


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate dbt artifacts (and optionally sync TestGen metadata) from a Canvas graph JSON payload.",
    )
    parser.add_argument("graph", help="Path to the Canvas graph JSON file.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Directory to write generated dbt artifacts. Files are written relative to the bundle paths.",
    )
    parser.add_argument(
        "--skip-testgen",
        action="store_true",
        help="Only generate dbt artifacts; do not update Databricks/TestGen metadata.",
    )
    parser.add_argument(
        "--generate-manifest",
        action="store_true",
        help="When used with --skip-testgen, force manifest generation via dbt parse.",
    )
    parser.add_argument("--databricks-host", default=os.getenv("DATABRICKS_HOST"))
    parser.add_argument("--databricks-http-path", default=os.getenv("DATABRICKS_HTTP_PATH"))
    parser.add_argument("--databricks-token", default=os.getenv("DATABRICKS_TOKEN"))
    parser.add_argument("--databricks-catalog", default=os.getenv("DATABRICKS_CATALOG"))
    parser.add_argument("--databricks-schema", default=os.getenv("DATABRICKS_SCHEMA"))
    parser.add_argument("--databricks-dq-schema", default=os.getenv("DATABRICKS_DQ_SCHEMA"))
    return parser.parse_args(argv)


def _load_graph(path: Path) -> CanvasGraph:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return CanvasGraph.parse_obj(payload)


def _ensure_databricks_params(args: argparse.Namespace) -> DatabricksConnectionParams:
    missing = [
        name
        for name in ("databricks_host", "databricks_http_path", "databricks_token", "databricks_dq_schema")
        if not getattr(args, name)
    ]
    if missing:
        raise ValueError(
            "Missing required Databricks/TestGen parameters: " + ", ".join(missing) + ". "
            "Provide CLI arguments or set the corresponding environment variables."
        )

    return DatabricksConnectionParams(
        workspace_host=args.databricks_host,
        http_path=args.databricks_http_path,
        access_token=args.databricks_token,
        catalog=args.databricks_catalog,
        schema_name=args.databricks_schema,
        data_quality_schema=args.databricks_dq_schema,
    )


def _write_bundle(bundle: DbtArtifactBundle, output_dir: Path) -> None:
    for artifact in bundle.files:
        target_path = output_dir / artifact.path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(artifact.content, encoding="utf-8")


def run_generate_canvas_assets(
    *,
    graph: CanvasGraph,
    output_dir: Optional[Path] = None,
    skip_testgen: bool,
    generate_manifest: bool,
    databricks_params: Optional[DatabricksConnectionParams] = None,
    translator: Optional[CanvasToDbtTranslator] = None,
) -> DbtArtifactBundle:
    if skip_testgen:
        translator = translator or CanvasToDbtTranslator(generate_manifest=generate_manifest)
        bundle = translator.translate(graph)
    else:
        if databricks_params is None:
            raise ValueError("Databricks parameters are required when updating TestGen metadata.")
        translator = translator or CanvasToDbtTranslator(generate_manifest=True)
        bundle = sync_canvas_graph_to_testgen(graph, databricks_params, translator=translator)

    if output_dir is not None:
        _write_bundle(bundle, output_dir)

    return bundle


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    graph_path = Path(args.graph)
    if not graph_path.exists():
        print(f"Graph file not found: {graph_path}", file=os.sys.stderr)
        return 1

    try:
        graph = _load_graph(graph_path)
    except Exception as exc:  # pragma: no cover - defensive guard for JSON parse errors
        print(f"Failed to load Canvas graph: {exc}", file=os.sys.stderr)
        return 1

    databricks_params = None
    if not args.skip_testgen:
        try:
            databricks_params = _ensure_databricks_params(args)
        except ValueError as exc:
            print(str(exc), file=os.sys.stderr)
            return 1

    try:
        bundle = run_generate_canvas_assets(
            graph=graph,
            output_dir=args.output_dir,
            skip_testgen=args.skip_testgen,
            generate_manifest=args.generate_manifest,
            databricks_params=databricks_params,
        )
    except CanvasTestGenSyncError as exc:
        print(f"TestGen sync failed: {exc}", file=os.sys.stderr)
        return 1
    except ValueError as exc:
        print(str(exc), file=os.sys.stderr)
        return 1

    print(f"Generated {len(bundle.files)} artifacts.")
    if bundle.warnings:
        for warning in bundle.warnings:
            print(f"Warning: {warning}")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
