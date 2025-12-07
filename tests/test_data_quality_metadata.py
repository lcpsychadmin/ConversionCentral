from __future__ import annotations

from types import SimpleNamespace
from typing import List
from uuid import uuid4

import pytest

from app.services.data_quality_metadata import ensure_data_quality_metadata
from app.services.data_quality_seed import (
    ConnectionSeed,
    DataQualitySeed,
    ProjectSeed,
    TableGroupSeed,
    TableSeed,
)
from app.services.data_quality_keys import (
    build_connection_id,
    build_project_key,
    build_table_group_id,
    build_table_id,
)
from app.services import data_quality_metadata
from app.services.data_quality_backend import LOCAL_BACKEND
from app.services.databricks_sql import DatabricksConnectionParams


def _build_params(**overrides) -> DatabricksConnectionParams:
    base = dict(
        workspace_host="adb-1234",
        http_path="/sql/1.0/warehouses/abc",
        access_token="token",
        catalog="sandbox",
        data_quality_schema="dq",
        data_quality_storage_format="delta",
        data_quality_auto_manage_tables=True,
    )
    base.update(overrides)
    return DatabricksConnectionParams(**base)


def test_ensure_data_quality_metadata_skips_when_auto_manage_disabled(monkeypatch):
    params = _build_params(data_quality_auto_manage_tables=False)

    called: bool = False

    def _sentinel_apply(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(data_quality_metadata, "_apply_schema", _sentinel_apply)
    monkeypatch.setattr(data_quality_metadata, "_collect_metadata", lambda params: DataQualitySeed())

    ensure_data_quality_metadata(params)

    assert called is False


def test_ensure_data_quality_metadata_skips_when_schema_missing(monkeypatch):
    params = _build_params(data_quality_schema="  ")

    called = False

    def _sentinel_apply(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(data_quality_metadata, "_apply_schema", _sentinel_apply)
    monkeypatch.setattr(data_quality_metadata, "_collect_metadata", lambda params: DataQualitySeed())

    ensure_data_quality_metadata(params)

    assert called is False


def test_ensure_data_quality_metadata_executes_expected_statements(monkeypatch):
    executed: List[str] = []

    class DummyConnection:
        def __enter__(self) -> "DummyConnection":
            return self

        def __exit__(self, exc_type, exc_value, traceback) -> None:
            return None

        def execute(self, clause, *args, **kwargs):
            executed.append(getattr(clause, "text", str(clause)))

    class DummyEngine:
        def connect(self) -> DummyConnection:
            return DummyConnection()

        def dispose(self) -> None:
            return None

    def _fake_create_engine(*args, **kwargs):
        return DummyEngine()

    monkeypatch.setattr(data_quality_metadata, "create_engine", _fake_create_engine)
    monkeypatch.setattr(data_quality_metadata, "build_sqlalchemy_url", lambda params: "db://ignored")
    monkeypatch.setattr(data_quality_metadata, "_collect_metadata", lambda params: DataQualitySeed())

    params = _build_params()

    ensure_data_quality_metadata(params)

    assert len(executed) == 38
    assert executed[0] == "CREATE SCHEMA IF NOT EXISTS `sandbox`.`dq`"
    assert executed[1].startswith("CREATE TABLE IF NOT EXISTS `sandbox`.`dq`.`dq_projects`")
    assert "USING DELTA" in executed[1]
    assert "TBLPROPERTIES" in executed[1]
    assert any("`dq_test_suites`" in statement for statement in executed)
    assert any("`dq_profile_columns`" in statement for statement in executed)
    assert any("`dq_profile_column_values`" in statement for statement in executed)
    assert any("`dq_profile_results`" in statement for statement in executed)
    assert any("`dq_profile_anomaly_results`" in statement for statement in executed)
    assert any("`dq_profile_operations`" in statement for statement in executed)
    assert any("`dq_data_table_chars`" in statement for statement in executed)
    assert any("`dq_data_column_chars`" in statement for statement in executed)
    assert any("ALTER TABLE `sandbox`.`dq`.`dq_table_groups` ADD COLUMNS (profiling_job_id STRING)" == statement for statement in executed)
    assert any("ALTER TABLE `sandbox`.`dq`.`dq_profiles` ADD COLUMNS (databricks_run_id STRING)" == statement for statement in executed)
    assert any("ALTER TABLE `sandbox`.`dq`.`dq_profiles` ADD COLUMNS (payload_path STRING)" == statement for statement in executed)
    assert any("ALTER TABLE `sandbox`.`dq`.`dq_profiles` ADD COLUMNS (table_count BIGINT)" == statement for statement in executed)
    assert any("ALTER TABLE `sandbox`.`dq`.`dq_profiles` ADD COLUMNS (dq_score_profiling DOUBLE)" == statement for statement in executed)
    assert any("ALTER TABLE `sandbox`.`dq`.`dq_data_table_chars` ADD COLUMNS (last_complete_profile_run_id STRING)" == statement for statement in executed)
    assert any("ALTER TABLE `sandbox`.`dq`.`dq_data_column_chars` ADD COLUMNS (last_complete_profile_run_id STRING)" == statement for statement in executed)
    assert "DELETE FROM `sandbox`.`dq`.`dq_settings` WHERE key = 'schema_version'" in executed
    assert "INSERT INTO `sandbox`.`dq`.`dq_settings` (key, value, updated_at) VALUES ('schema_version', '3', current_timestamp())" in executed


@pytest.mark.parametrize("storage_format", ["iceberg", "csv"])
def test_ensure_data_quality_metadata_skips_unsupported_storage(monkeypatch, storage_format):
    params = _build_params(data_quality_storage_format=storage_format)

    called: bool = False

    def _sentinel_apply(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(data_quality_metadata, "_apply_schema", _sentinel_apply)
    monkeypatch.setattr(data_quality_metadata, "_collect_metadata", lambda params: DataQualitySeed())

    ensure_data_quality_metadata(params)

    assert called is False


def test_ensure_data_quality_metadata_seeds_metadata(monkeypatch):
    executed: List[str] = []

    class DummyConnection:
        def __enter__(self) -> "DummyConnection":
            return self

        def __exit__(self, exc_type, exc_value, traceback) -> None:
            return None

        def execute(self, clause, *args, **kwargs):
            executed.append(getattr(clause, "text", str(clause)))

    class DummyEngine:
        def connect(self) -> DummyConnection:
            return DummyConnection()

        def dispose(self) -> None:
            return None

    def _fake_create_engine(*args, **kwargs):
        return DummyEngine()

    def _seed_stub(params: DatabricksConnectionParams) -> DataQualitySeed:
        system_identifier = "proj"
        data_object_identifier = "object-1"
        connection_identifier = "connection-1"
        selection_identifier = "selection-1234"

        project_key = build_project_key(system_identifier, data_object_identifier)
        connection_id = build_connection_id(connection_identifier, data_object_identifier)
        table_group_id = build_table_group_id(connection_identifier, data_object_identifier)

        table_seed = TableSeed(
            table_id=build_table_id(selection_identifier, data_object_identifier),
            table_group_id=table_group_id,
            schema_name="analytics",
            table_name="orders",
        )
        group_seed = TableGroupSeed(
            table_group_id=table_group_id,
            connection_id=connection_id,
            name="Test Tables",
            tables=(table_seed,),
        )
        connection_seed = ConnectionSeed(
            connection_id=connection_id,
            project_key=project_key,
            system_id=system_identifier,
            name="System (JDBC)",
            catalog="sandbox",
            schema_name="analytics",
            http_path="/sql/warehouse",
            managed_credentials_ref=None,
            is_active=True,
            table_groups=(group_seed,),
        )
        project_seed = ProjectSeed(
            project_key=project_key,
            name="System",
            description=None,
            sql_flavor="databricks-sql",
            connections=(connection_seed,),
        )
        return DataQualitySeed(projects=(project_seed,))

    monkeypatch.setattr(data_quality_metadata, "create_engine", _fake_create_engine)
    monkeypatch.setattr(data_quality_metadata, "build_sqlalchemy_url", lambda params: "db://ignored")
    monkeypatch.setattr(data_quality_metadata, "_collect_metadata", _seed_stub)

    params = _build_params()

    ensure_data_quality_metadata(params)

    assert any("MERGE INTO `sandbox`.`dq`.`dq_projects`" in stmt for stmt in executed)
    assert any("MERGE INTO `sandbox`.`dq`.`dq_connections`" in stmt for stmt in executed)
    assert any("MERGE INTO `sandbox`.`dq`.`dq_table_groups`" in stmt for stmt in executed)
    assert any("MERGE INTO `sandbox`.`dq`.`dq_tables`" in stmt for stmt in executed)
    assert any("DELETE FROM `sandbox`.`dq`.`dq_connections`" in stmt for stmt in executed)


def test_ensure_data_quality_metadata_local_backend(monkeypatch):
    params = _build_params()

    class DummyRepo:
        def __init__(self):
            self.seeded = False

        def seed_metadata(self, seed):
            self.seeded = True

    repo_instance = DummyRepo()

    monkeypatch.setattr(data_quality_metadata, "get_metadata_backend", lambda: LOCAL_BACKEND)
    monkeypatch.setattr(data_quality_metadata, "LocalDataQualityRepository", lambda: repo_instance)
    monkeypatch.setattr(data_quality_metadata, "_collect_metadata", lambda params: DataQualitySeed())

    def _fail(*args, **kwargs):  # pragma: no cover - validation helper
        raise AssertionError("Databricks schema provisioning should not run for local backend")

    monkeypatch.setattr(data_quality_metadata, "_apply_schema", _fail)

    ensure_data_quality_metadata(params)

    assert repo_instance.seeded is True


def test_definition_table_entry_uses_ingestion_schema_for_native_tables():
    system_id = uuid4()
    definition_table_id = uuid4()
    system = SimpleNamespace(id=system_id, name="Demo Data", physical_name=None)
    table = SimpleNamespace(
        id=uuid4(),
        schema_name="raw",
        physical_name="Customers",
        name=None,
        system_id=system_id,
    )
    definition_table = SimpleNamespace(id=definition_table_id, table=table, constructed_table=None)
    params = _build_params(schema_name="stage")

    entry = data_quality_metadata._definition_table_entry(definition_table, system, params)

    assert entry is not None
    assert entry.schema_name == "demo_data"
    assert entry.table_name == "raw_customers"
    assert entry.source_table_id == str(table.id)


def test_definition_table_entry_uses_table_schema_for_external_tables():
    system = SimpleNamespace(id=uuid4(), name="CRM", physical_name=None)
    other_system_id = uuid4()
    table = SimpleNamespace(
        id=uuid4(),
        schema_name="analytics",
        physical_name="orders",
        name="Orders",
        system_id=other_system_id,
    )
    definition_table = SimpleNamespace(id=uuid4(), table=table, constructed_table=None)
    params = _build_params()

    entry = data_quality_metadata._definition_table_entry(definition_table, system, params)

    assert entry is not None
    assert entry.schema_name == "analytics"
    assert entry.table_name == "orders"


def test_definition_table_entry_handles_constructed_tables(monkeypatch):
    system = SimpleNamespace(id=uuid4(), name="ERP", physical_name="erp")
    constructed_table = SimpleNamespace(id=uuid4(), name="curated_orders", schema_name="cstm_schema")
    table = SimpleNamespace(id=uuid4(), schema_name=None, physical_name=None, system_id=system.id)
    definition_table = SimpleNamespace(
        id=uuid4(),
        table=table,
        constructed_table=constructed_table,
    )
    params = _build_params(constructed_schema="constructed")

    def _resolve(*, overrides=None, table=None, system=None, fallback_schema=None):
        return "constructed_data" if table is constructed_table else "constructed"

    monkeypatch.setattr(data_quality_metadata, "resolve_constructed_schema", _resolve)

    entry = data_quality_metadata._definition_table_entry(definition_table, system, params)

    assert entry is not None
    assert entry.schema_name == "constructed_data"
    assert entry.table_name == constructed_table.name
