from contextlib import contextmanager

from sqlalchemy import select

from app.config import get_settings
from app.ingestion.engine import reset_ingestion_engine
from app.models import System, SystemConnection
from app.services import databricks_bootstrap
from app.services.databricks_bootstrap import ensure_databricks_connection


def test_ensure_databricks_connection_creates_managed_records(monkeypatch, db_session):
    monkeypatch.delenv("INGESTION_DATABASE_URL", raising=False)
    monkeypatch.setenv("DATABRICKS_HOST", "adb-12345.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/abc123")
    monkeypatch.setenv("DATABRICKS_TOKEN", "test-token")
    monkeypatch.setenv("DATABRICKS_CATALOG", "main")
    monkeypatch.setenv("DATABRICKS_SCHEMA", "analytics")

    @contextmanager
    def _session_override():
        yield db_session

    monkeypatch.setattr(databricks_bootstrap, "SessionLocal", _session_override)
    monkeypatch.setattr("app.ingestion.engine.SessionLocal", _session_override)

    dq_calls: list[object] = []

    def _capture_metadata_call(params):
        dq_calls.append(params)

    monkeypatch.setattr(databricks_bootstrap, "ensure_data_quality_metadata", _capture_metadata_call)

    get_settings.cache_clear()
    reset_ingestion_engine()

    ensure_databricks_connection()

    system = (
        db_session.execute(select(System).where(System.name == "Databricks Warehouse"))
        .scalars()
        .first()
    )
    assert system is not None
    assert system.physical_name == "databricks_warehouse"
    assert system.system_type == "warehouse"
    assert system.status == "active"

    connection = (
        db_session.execute(select(SystemConnection).where(SystemConnection.system_id == system.id))
        .scalars()
        .first()
    )
    assert connection is not None
    assert connection.connection_type == "jdbc"
    assert connection.auth_method == "username_password"
    assert connection.active is True
    assert connection.ingestion_enabled is False
    assert connection.connection_string.startswith(
        "jdbc:databricks://token:@adb-12345.azuredatabricks.net"
    )

    # Subsequent calls should be idempotent
    ensure_databricks_connection()
    duplicate_count = (
        db_session.execute(select(SystemConnection).where(SystemConnection.system_id == system.id))
        .scalars()
        .all()
    )
    assert len(duplicate_count) == 1
    assert len(dq_calls) >= 1

    get_settings.cache_clear()
    reset_ingestion_engine()