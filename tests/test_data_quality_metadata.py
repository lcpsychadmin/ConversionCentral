from __future__ import annotations

from typing import List

import pytest

from app.services.data_quality_metadata import (
    ensure_data_quality_metadata,
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

    assert len(executed) == 17
    assert executed[0] == "CREATE SCHEMA IF NOT EXISTS `sandbox`.`dq`"
    assert executed[1].startswith("CREATE TABLE IF NOT EXISTS `sandbox`.`dq`.`dq_projects`")
    assert "USING DELTA" in executed[1]
    assert "TBLPROPERTIES" in executed[1]
    assert any("`dq_test_suites`" in statement for statement in executed)
    assert any("ALTER TABLE `sandbox`.`dq`.`dq_table_groups` ADD COLUMNS (profiling_job_id STRING)" == statement for statement in executed)
    assert any("ALTER TABLE `sandbox`.`dq`.`dq_profiles` ADD COLUMNS (databricks_run_id STRING)" == statement for statement in executed)
    assert "DELETE FROM `sandbox`.`dq`.`dq_settings` WHERE key = 'schema_version'" in executed
    assert "INSERT INTO `sandbox`.`dq`.`dq_settings` (key, value, updated_at) VALUES ('schema_version', '1', current_timestamp())" in executed


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
