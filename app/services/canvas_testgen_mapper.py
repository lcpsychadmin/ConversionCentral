from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

from app.schemas.dbt_manifest import DbtManifest, DbtManifestSource
from app.services.data_quality_seed import (
    ConnectionSeed,
    DataQualitySeed,
    ProjectSeed,
    TableGroupSeed,
    TableSeed,
)

_VALID_IDENTIFIER_RE = re.compile(r"[^0-9a-zA-Z_]+")


@dataclass
class _TableRecord:
    table_id: str
    table_name: str
    schema_name: Optional[str]
    database: Optional[str]
    source_table_id: Optional[str]
    resource_type: str


class ManifestToDataQualityMapper:
    """Convert a dbt manifest into Data Quality provisioning seeds."""

    def __init__(
        self,
        *,
        project_key_prefix: str = "canvas:",
        default_schema: Optional[str] = None,
    ) -> None:
        self.project_key_prefix = project_key_prefix
        self.default_schema = default_schema

    def build_seed(
        self,
        manifest: DbtManifest,
        *,
        default_catalog: Optional[str] = None,
    ) -> DataQualitySeed:
        project_name = (manifest.metadata.project_name if manifest.metadata else None) or "canvas_project"
        project_key = f"{self.project_key_prefix}{self._slugify(project_name)}"
        sql_flavor = "dbt-core"
        if manifest.metadata and manifest.metadata.adapter_type:
            sql_flavor = manifest.metadata.adapter_type

        source_table_map = self._collect_source_ids(project_key, manifest.sources.values())
        table_records = self._collect_tables(
            project_key=project_key,
            manifest=manifest,
            source_table_map=source_table_map,
        )

        connections = self._build_connections(
            project_key=project_key,
            project_display=project_name,
            table_records=table_records,
            default_catalog=default_catalog,
        )

        project_seed = ProjectSeed(
            project_key=project_key,
            name=project_name,
            description=None,
            sql_flavor=sql_flavor,
            connections=tuple(connections),
        )
        return DataQualitySeed(projects=(project_seed,))

    # ------------------------------------------------------------------
    # Internal helpers

    def _collect_source_ids(
        self,
        project_key: str,
        sources: Iterable[DbtManifestSource],
    ) -> Dict[str, str]:
        source_ids: Dict[str, str] = {}
        for source in sources:
            schema_name = source.schema_ or self.default_schema
            table_name = source.identifier or source.name
            table_id = self._table_id(project_key, schema_name, table_name, "source")
            source_ids[source.unique_id] = table_id
        return source_ids

    def _collect_tables(
        self,
        *,
        project_key: str,
        manifest: DbtManifest,
        source_table_map: Dict[str, str],
    ) -> List[_TableRecord]:
        records: List[_TableRecord] = []

        for source in manifest.sources.values():
            schema_name = source.schema_ or self.default_schema
            table_name = source.identifier or source.name
            table_id = source_table_map[source.unique_id]
            records.append(
                _TableRecord(
                    table_id=table_id,
                    table_name=table_name,
                    schema_name=schema_name,
                    database=source.database,
                    source_table_id=None,
                    resource_type="source",
                )
            )

        for node in manifest.nodes.values():
            if node.resource_type not in {"model", "seed", "snapshot"}:
                continue
            schema_name = node.schema_ or self.default_schema
            table_name = node.alias or node.name
            table_id = self._table_id(project_key, schema_name, table_name, node.resource_type)
            dependencies = node.depends_on.nodes if node.depends_on else []
            source_ref = next((dep for dep in dependencies if dep in source_table_map), None)
            source_table_id = source_table_map.get(source_ref) if source_ref else None
            records.append(
                _TableRecord(
                    table_id=table_id,
                    table_name=table_name,
                    schema_name=schema_name,
                    database=node.database,
                    source_table_id=source_table_id,
                    resource_type=node.resource_type,
                )
            )

        return records

    def _build_connections(
        self,
        *,
        project_key: str,
        project_display: str,
        table_records: List[_TableRecord],
        default_catalog: Optional[str],
    ) -> List[ConnectionSeed]:
        tables_by_schema: Dict[str, List[_TableRecord]] = {}
        for record in table_records:
            schema_key = record.schema_name or "__default__"
            tables_by_schema.setdefault(schema_key, []).append(record)

        connections: List[ConnectionSeed] = []
        for schema_key, tables in sorted(tables_by_schema.items()):
            schema_name = None if schema_key == "__default__" else schema_key
            connection_id = f"{project_key}:{schema_key}"
            table_group_id = f"{connection_id}:tables"

            table_seeds = tuple(
                TableSeed(
                    table_id=record.table_id,
                    table_group_id=table_group_id,
                    schema_name=record.schema_name,
                    table_name=record.table_name,
                    source_table_id=record.source_table_id,
                )
                for record in sorted(tables, key=lambda r: (r.resource_type, r.table_name))
            )

            catalog = next((record.database for record in tables if record.database), default_catalog)
            connection_seed = ConnectionSeed(
                connection_id=connection_id,
                project_key=project_key,
                system_id=connection_id,
                name=f"{project_display} · {schema_name or 'default'}",
                catalog=catalog,
                schema_name=schema_name,
                http_path=None,
                managed_credentials_ref=None,
                is_active=True,
                table_groups=(
                    TableGroupSeed(
                        table_group_id=table_group_id,
                        connection_id=connection_id,
                        name=f"{schema_name or 'Default'} Tables",
                        description=f"Generated from dbt schema {schema_name or 'default'}",
                        tables=table_seeds,
                    ),
                ),
            )
            connections.append(connection_seed)

        return connections

    def _table_id(
        self,
        project_key: str,
        schema_name: Optional[str],
        table_name: str,
        resource_type: str,
    ) -> str:
        schema_component = self._slugify(schema_name) if schema_name else "default"
        table_component = self._slugify(table_name)
        return f"{project_key}:{schema_component}:{table_component}:{resource_type}"

    def _slugify(self, value: Optional[str]) -> str:
        if not value:
            return "item"
        sanitized = _VALID_IDENTIFIER_RE.sub("_", value.strip().lower())
        sanitized = re.sub(r"_+", "_", sanitized).strip("_")
        if not sanitized:
            return "item"
        if sanitized[0].isdigit():
            sanitized = f"_{sanitized}"
        return sanitized


__all__ = ["ManifestToDataQualityMapper"]
