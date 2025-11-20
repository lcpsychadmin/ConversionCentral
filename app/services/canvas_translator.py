from __future__ import annotations

import json
import os
import re
import textwrap
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, Iterable, List, Optional, Tuple

import subprocess
import yaml

from app.schemas.canvas import (
    CanvasGraph,
    CanvasGraphColumn,
    CanvasGraphNode,
    CanvasGraphNodeConfig,
)
from app.schemas.dbt import DbtArtifactBundle, DbtArtifactFile

_VALID_IDENTIFIER_RE = re.compile(r"[^0-9a-zA-Z_]+")


@dataclass
class _ModelDependencies:
    source_nodes: List[str]
    ref_nodes: List[str]


class CanvasToDbtTranslator:
    """Translate a Canvas graph into dbt project artifacts."""

    def __init__(
        self,
        *,
        default_project_name: str = "canvas_project",
        generate_manifest: Optional[bool] = None,
    ) -> None:
        self.default_project_name = default_project_name
        if generate_manifest is None:
            env_flag = os.environ.get("DBT_GENERATE_MANIFEST")
            self.generate_manifest = env_flag == "1"
        else:
            self.generate_manifest = generate_manifest

    def translate(self, graph: CanvasGraph) -> DbtArtifactBundle:
        nodes_by_id: Dict[str, CanvasGraphNode] = {node.id: node for node in graph.nodes}
        slug_map: Dict[str, str] = {node_id: self._node_slug(node) for node_id, node in nodes_by_id.items()}
        dependencies = self._build_dependencies(graph, nodes_by_id)

        files: List[DbtArtifactFile] = []
        files.append(self._build_project_file(graph))
        files.extend(self._build_source_files(nodes_by_id))
        files.extend(self._build_model_files(nodes_by_id, slug_map, dependencies))
        files.append(self._build_canvas_file(graph))

        warnings: List[str] = list(graph.warnings or [])
        if self.generate_manifest:
            manifest_content, manifest_warning = self._build_manifest_file(files)
            if manifest_content:
                files.append(DbtArtifactFile(path="target/manifest.json", content=manifest_content))
            if manifest_warning:
                warnings.append(manifest_warning)

        return DbtArtifactBundle(files=files, warnings=warnings or None)

    # ------------------------------------------------------------------
    # Builders

    def _build_project_file(self, graph: CanvasGraph) -> DbtArtifactFile:
        project_name = self._project_name(graph)
        content = textwrap.dedent(
            f"""
            name: {project_name}
            version: '1.0'
            config-version: 2
            profile: default
            model-paths: ['models']
            seed-paths: ['seeds']
            snapshot-paths: ['snapshots']
            analysis-paths: ['analyses']
            test-paths: ['tests']
            macro-paths: ['macros']
            packages-install-path: 'dbt_packages'
            target-path: 'target'
            clean-targets: ['target', 'dbt_packages']
            """
        ).strip()
        content += "\n"
        return DbtArtifactFile(path="dbt_project.yml", content=content)

    def _build_source_files(self, nodes_by_id: Dict[str, CanvasGraphNode]) -> List[DbtArtifactFile]:
        sources_by_name: Dict[str, List[CanvasGraphNode]] = defaultdict(list)
        for node in nodes_by_id.values():
            if node.resource_type != "source":
                continue
            source_name = self._resolve_source_name(node)
            sources_by_name[source_name].append(node)

        files: List[DbtArtifactFile] = []
        for source_name, nodes in sorted(sources_by_name.items()):
            document = self._render_source_document(source_name, nodes)
            path = f"models/sources/{source_name}.yml"
            files.append(DbtArtifactFile(path=path, content=document))
        return files

    def _build_model_files(
        self,
        nodes_by_id: Dict[str, CanvasGraphNode],
        slug_map: Dict[str, str],
        dependencies: Dict[str, _ModelDependencies],
    ) -> List[DbtArtifactFile]:
        files: List[DbtArtifactFile] = []
        for node in nodes_by_id.values():
            if node.resource_type not in {"model", "seed", "snapshot"}:
                continue
            slug = slug_map[node.id]
            layer_folder = self._resolve_layer_folder(node)
            model_sql = self._render_model_sql(node, dependencies.get(node.id), nodes_by_id, slug_map)
            files.append(
                DbtArtifactFile(
                    path=f"models/{layer_folder}/{slug}.sql",
                    content=model_sql,
                )
            )
            schema_yaml = self._render_model_schema_yaml(node, slug, dependencies.get(node.id))
            files.append(
                DbtArtifactFile(
                    path=f"models/{layer_folder}/{slug}.yml",
                    content=schema_yaml,
                )
            )
        return files

    def _build_canvas_file(self, graph: CanvasGraph) -> DbtArtifactFile:
        payload = graph.dict(by_alias=True, exclude_none=True)
        canvas_json = json.dumps(payload, indent=2, sort_keys=True) + "\n"
        return DbtArtifactFile(path="canvas.json", content=canvas_json)

    # ------------------------------------------------------------------
    # Rendering helpers

    def _render_source_document(self, source_name: str, nodes: Iterable[CanvasGraphNode]) -> str:
        tables: List[dict] = []
        for node in sorted(nodes, key=lambda item: self._resolve_table_name(item)):
            table_entry = {
                "name": self._resolve_table_name(node),
            }
            if node.description:
                table_entry["description"] = node.description
            columns_payload = self._render_columns(node.columns)
            if columns_payload:
                table_entry["columns"] = columns_payload
            if node.meta:
                table_entry["meta"] = node.meta
            tables.append(table_entry)

        document = {
            "version": 2,
            "sources": [
                {
                    "name": source_name,
                    "tables": tables,
                }
            ],
        }
        return yaml.safe_dump(document, sort_keys=False)

    def _render_model_schema_yaml(
        self,
        node: CanvasGraphNode,
        slug: str,
        dependencies: _ModelDependencies | None,
    ) -> str:
        config = node.config or CanvasGraphNodeConfig()
        model_entry: dict = {
            "name": slug,
        }
        description = node.description or node.label
        if description:
            model_entry["description"] = description
        columns_payload = self._render_columns(node.columns)
        if columns_payload:
            model_entry["columns"] = columns_payload
        if config.tags:
            model_entry["tags"] = config.tags
        if config.meta:
            model_entry["meta"] = config.meta
        if node.meta:
            meta = model_entry.setdefault("meta", {})
            meta.update(node.meta)
        owner = config.owner
        if owner:
            meta = model_entry.setdefault("meta", {})
            meta.setdefault("owner", owner)
        access = config.access
        if access:
            model_entry["access"] = access
        if config.tests:
            model_entry["tests"] = [self._format_test(test.name, test.arguments) for test in config.tests]
        exposures = config.exposures
        if exposures:
            model_entry["exposures"] = [exposure.dict(exclude_none=True) for exposure in exposures]
        metrics = config.metrics
        if metrics:
            model_entry["metrics"] = [metric.dict(exclude_none=True) for metric in metrics]
        document = {
            "version": 2,
            "models": [model_entry],
        }
        return yaml.safe_dump(document, sort_keys=False)

    def _render_model_sql(
        self,
        node: CanvasGraphNode,
        dependencies: _ModelDependencies | None,
        nodes_by_id: Dict[str, CanvasGraphNode],
        slug_map: Dict[str, str],
    ) -> str:
        config_block = self._render_config_block(node.config)
        dependency_block = self._render_dependency_block(dependencies, nodes_by_id, slug_map)
        body = "select 1 as placeholder\n"
        if dependency_block:
            body = dependency_block
        content_parts = [section for section in [config_block, body] if section]
        content = "\n".join(content_parts)
        if not content.endswith("\n"):
            content += "\n"
        return content

    def _render_columns(self, columns: List[CanvasGraphColumn] | None) -> List[dict]:
        if not columns:
            return []
        payload: List[dict] = []
        for column in columns:
            entry: dict = {"name": column.name}
            if column.description:
                entry["description"] = column.description
            if column.data_type:
                entry["data_type"] = column.data_type
            if column.meta:
                entry["meta"] = column.meta
            tests_payload = []
            if column.tests:
                for column_test in column.tests:
                    tests_payload.append(self._format_test(column_test.name, column_test.arguments))
            if tests_payload:
                entry["tests"] = tests_payload
            payload.append(entry)
        return payload

    def _render_config_block(self, config: CanvasGraphNodeConfig | None) -> str:
        if not config:
            return ""
        parameters: List[str] = []
        if config.materialization:
            parameters.append(f"materialized='{config.materialization}'")
        if config.tags:
            parameters.append(f"tags={self._format_list(config.tags)}")
        if not parameters:
            return ""
        return f"{{{{ config({', '.join(parameters)}) }}}}\n"

    def _render_dependency_block(
        self,
        dependencies: _ModelDependencies | None,
        nodes_by_id: Dict[str, CanvasGraphNode],
        slug_map: Dict[str, str],
    ) -> str:
        if not dependencies:
            return ""
        ctes: List[str] = []
        alias_sequence: List[str] = []

        for index, node_id in enumerate(dependencies.source_nodes):
            node = nodes_by_id.get(node_id)
            if not node:
                continue
            source_name = self._resolve_source_name(node)
            table_name = self._resolve_table_name(node)
            alias = f"source_{index}"
            alias_sequence.append(alias)
            ctes.append(
                textwrap.dedent(
                    f"""
                    {alias} as (
                        select *
                        from {{{{ source('{source_name}', '{table_name}') }}}}
                    )
                    """
                ).strip()
            )

        ref_offset = len(alias_sequence)
        for index, node_id in enumerate(dependencies.ref_nodes):
            node = nodes_by_id.get(node_id)
            if not node:
                continue
            ref_name = slug_map[node_id]
            alias = f"ref_{ref_offset + index}"
            alias_sequence.append(alias)
            ctes.append(
                textwrap.dedent(
                    f"""
                    {alias} as (
                        select *
                        from {{{{ ref('{ref_name}') }}}}
                    )
                    """
                ).strip()
            )

        if not ctes:
            return ""

        with_block = "with\n    " + ",\n    ".join(cte.replace("\n", "\n    ") for cte in ctes)
        select_block = "select *\nfrom {alias}\n".format(alias=alias_sequence[0])
        return f"{with_block}\n\n{select_block}"

    def _format_test(self, name: str, arguments: dict | None) -> dict | str:
        if not arguments:
            return name
        return {name: arguments}

    def _format_list(self, values: Iterable[str]) -> str:
        joined = ", ".join(f"'{value}'" for value in values)
        return f"[{joined}]"

    def _build_manifest_file(self, files: List[DbtArtifactFile]) -> Tuple[Optional[str], Optional[str]]:
        try:
            with TemporaryDirectory(prefix="canvas_dbt_") as tmpdir:
                project_root = Path(tmpdir)
                for artifact in files:
                    target_path = project_root / artifact.path
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    target_path.write_text(artifact.content, encoding="utf-8")

                profiles_dir = project_root / ".profiles"
                profiles_dir.mkdir(parents=True, exist_ok=True)
                (profiles_dir / "profiles.yml").write_text(
                    textwrap.dedent(
                        """
                        default:
                          outputs:
                            dev:
                              type: duckdb
                              path: ':memory:'
                          target: dev
                        """
                    ).strip()
                    + "\n",
                    encoding="utf-8",
                )

                env = os.environ.copy()
                env.setdefault("DBT_PROFILES_DIR", str(profiles_dir))

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
                    warning = (
                        "dbt manifest generation failed: "
                        f"return code {result.returncode}. stderr: {result.stderr.strip()}"
                    )
                    return None, warning

                manifest_path = project_root / "target" / "manifest.json"
                if not manifest_path.exists():
                    return None, "dbt parse completed but manifest.json was not produced."

                manifest_content = manifest_path.read_text(encoding="utf-8")
                if not manifest_content.endswith("\n"):
                    manifest_content += "\n"
                return manifest_content, None
        except FileNotFoundError:
            return None, "dbt CLI not found; skipping manifest generation."
        except Exception as exc:  # pragma: no cover - defensive guard
            return None, f"Unexpected error during manifest generation: {exc}"

        return None, None

    # ------------------------------------------------------------------
    # Dependency helpers

    def _build_dependencies(
        self,
        graph: CanvasGraph,
        nodes_by_id: Dict[str, CanvasGraphNode],
    ) -> Dict[str, _ModelDependencies]:
        dependencies: Dict[str, _ModelDependencies] = {}
        for edge in graph.edges:
            if edge.target not in nodes_by_id:
                continue
            target_node = nodes_by_id[edge.target]
            if target_node.resource_type not in {"model", "seed", "snapshot"}:
                continue
            entry = dependencies.setdefault(edge.target, _ModelDependencies(source_nodes=[], ref_nodes=[]))
            if edge.type == "source":
                entry.source_nodes.append(edge.source)
            elif edge.type == "ref":
                entry.ref_nodes.append(edge.source)
        return dependencies

    # ------------------------------------------------------------------
    # Metadata helpers

    def _project_name(self, graph: CanvasGraph) -> str:
        metadata = graph.metadata
        if metadata:
            for candidate in [metadata.project_name, metadata.definition_id, metadata.project_id]:
                if candidate:
                    return self._slugify(candidate)
        timestamp = datetime.utcnow().strftime("canvas_%Y%m%d")
        return self._slugify(self.default_project_name or timestamp)

    def _resolve_layer_folder(self, node: CanvasGraphNode) -> str:
        if node.layer:
            return self._slugify(node.layer)
        if node.resource_type == "seed":
            return "seeds"
        if node.resource_type == "snapshot":
            return "snapshots"
        return "models"

    def _resolve_source_name(self, node: CanvasGraphNode) -> str:
        meta = node.meta or {}
        for candidate in [
            meta.get("sourceName"),
            meta.get("source_name"),
            meta.get("system"),
            node.origin.schema_name if node.origin else None,
            node.origin.system_id if node.origin else None,
            node.name,
            node.label,
        ]:
            if candidate:
                return self._slugify(candidate)
        return "source"

    def _resolve_table_name(self, node: CanvasGraphNode) -> str:
        for candidate in [node.name, node.label, node.id]:
            if candidate:
                return self._slugify(candidate)
        return "table"

    def _node_slug(self, node: CanvasGraphNode) -> str:
        return self._slugify(node.name or node.label or node.id)

    def _slugify(self, value: str) -> str:
        sanitized = _VALID_IDENTIFIER_RE.sub("_", value.strip().lower())
        sanitized = re.sub(r"_+", "_", sanitized).strip("_")
        if not sanitized:
            return "item"
        if sanitized[0].isdigit():
            sanitized = f"_{sanitized}"
        return sanitized


__all__ = ["CanvasToDbtTranslator"]
