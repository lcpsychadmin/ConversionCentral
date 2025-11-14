from typing import Iterator
from unittest.mock import MagicMock

import pytest

from app.ingestion.engine import reset_ingestion_engine
from app.services.databricks_sql import DatabricksConnectionParams
from app.services.ingestion_loader import DatabricksTableLoader, SparkTableLoader
from app.services.scheduled_ingestion import ScheduledIngestionEngine


def _stub_params(method: str) -> DatabricksConnectionParams:
    return DatabricksConnectionParams(
        workspace_host="adb-example.databricks.net",
        http_path="/sql/1.0/warehouses/1234567890abcdef",
        access_token="token",
        catalog="workspace",
        schema_name="default",
        constructed_schema="constructed",
        ingestion_batch_rows=500,
        ingestion_method=method,
    )


def _patch_ingestion_dependencies(monkeypatch: pytest.MonkeyPatch, params: DatabricksConnectionParams) -> None:
    fake_engine = MagicMock()
    fake_engine.dialect.name = "databricks"

    monkeypatch.setattr("app.ingestion.engine.get_ingestion_connection_params", lambda: params)
    monkeypatch.setattr("app.services.ingestion_loader.get_ingestion_connection_params", lambda: params)
    monkeypatch.setattr("app.services.scheduled_ingestion.get_ingestion_connection_params", lambda: params)
    monkeypatch.setattr("app.services.ingestion_loader.get_ingestion_engine", lambda: fake_engine)


@pytest.mark.usefixtures("reset_ingestion_state")
def test_scheduled_ingestion_uses_sql_loader(monkeypatch: pytest.MonkeyPatch) -> None:
    params = _stub_params("sql")
    _patch_ingestion_dependencies(monkeypatch, params)

    engine = ScheduledIngestionEngine()
    loader = engine._get_loader()

    assert isinstance(loader, DatabricksTableLoader)
    assert engine._get_loader() is loader


@pytest.mark.usefixtures("reset_ingestion_state")
def test_scheduled_ingestion_uses_spark_loader(monkeypatch: pytest.MonkeyPatch) -> None:
    params = _stub_params("spark")
    _patch_ingestion_dependencies(monkeypatch, params)

    engine = ScheduledIngestionEngine()
    loader = engine._get_loader()

    assert isinstance(loader, SparkTableLoader)
    assert engine._get_loader() is loader


def test_spark_loader_extracts_warehouse_id() -> None:
    assert SparkTableLoader._extract_warehouse_id("/sql/1.0/warehouses/abc123") == "abc123"
    assert SparkTableLoader._extract_warehouse_id("sql/protocolv1/o/0/xyz789") == "xyz789"
    assert SparkTableLoader._extract_warehouse_id(None) is None


@pytest.fixture(name="reset_ingestion_state")
def fixture_reset_ingestion_state() -> Iterator[None]:
    reset_ingestion_engine()
    yield
    reset_ingestion_engine()
