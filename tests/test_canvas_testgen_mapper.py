from __future__ import annotations

import json
from pathlib import Path

from app.schemas.dbt_manifest import DbtManifest
from app.services.canvas_testgen_mapper import ManifestToDataQualityMapper

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "dbt" / "manifest" / "simple_manifest.json"


def load_manifest() -> DbtManifest:
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    return DbtManifest.parse_obj(payload)


def test_mapper_builds_project_connections_and_tables() -> None:
    manifest = load_manifest()
    mapper = ManifestToDataQualityMapper()
    seed = mapper.build_seed(manifest, default_catalog="main")

    assert len(seed.projects) == 1
    project = seed.projects[0]
    assert project.project_key == "canvas:canvas_demo"
    assert project.sql_flavor == "duckdb"

    assert len(project.connections) == 2  # staging models + crm sources
    connection_names = {connection.name for connection in project.connections}
    assert "canvas_demo · staging" in connection_names
    assert "canvas_demo · crm" in connection_names

    staging_connection = next(connection for connection in project.connections if connection.schema_name == "staging")
    assert len(staging_connection.table_groups) == 1
    staging_tables = staging_connection.table_groups[0].tables
    assert {table.table_name for table in staging_tables} == {"dim_customers"}
    dim_table = staging_tables[0]
    assert dim_table.table_id.endswith(":model")

    crm_connection = next(connection for connection in project.connections if connection.schema_name == "crm")
    crm_tables = crm_connection.table_groups[0].tables
    assert {table.table_name for table in crm_tables} == {"crm_customers"}

    model_source_id = staging_tables[0].source_table_id
    assert model_source_id is not None
    assert model_source_id in {table.table_id for table in crm_tables}
