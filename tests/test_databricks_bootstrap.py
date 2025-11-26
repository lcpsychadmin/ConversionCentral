from contextlib import contextmanager

from sqlalchemy import select

from app.config import get_settings
from app.ingestion.engine import reset_ingestion_engine
from app.models import System, SystemConnection
from app.services import databricks_bootstrap
from app.services.databricks_bootstrap import ensure_databricks_connection, ensure_databricks_schemas


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
    schema_calls: list[object] = []

    def _capture_metadata_call(params):
        dq_calls.append(params)

    monkeypatch.setattr(databricks_bootstrap, "ensure_data_quality_metadata", _capture_metadata_call)

    def _capture_schema_call(params):
        schema_calls.append(params)

    monkeypatch.setattr(databricks_bootstrap, "ensure_databricks_schemas", _capture_schema_call)

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
    assert len(schema_calls) >= 1

    get_settings.cache_clear()
    reset_ingestion_engine()


def test_ensure_databricks_schemas_creates_all_targets(monkeypatch):
    statements: list[str] = []

    class DummyConnection:
        def execute(self, statement):
            text_clause = getattr(statement, "text", str(statement))
            statements.append(text_clause)

    @contextmanager
    def _begin():
        yield DummyConnection()

    class DummyEngine:
        def begin(self):
            return _begin()

        def dispose(self):
            pass

    def _fake_create_engine(url, **_):
        return DummyEngine()

    monkeypatch.setattr(databricks_bootstrap, "create_engine", _fake_create_engine)

    params = databricks_bootstrap.DatabricksConnectionParams(
        workspace_host="test.cloud.databricks.com",
        http_path="/sql/warehouse/123",
        access_token="token",
        catalog="workspace",
        schema_name="default",
        constructed_schema="constructed",
        data_quality_schema="dq",
    )

    ensure_databricks_schemas(params)

    assert "CREATE SCHEMA IF NOT EXISTS `workspace`.`default`" in statements
    assert "CREATE SCHEMA IF NOT EXISTS `workspace`.`constructed`" in statements
    assert "CREATE SCHEMA IF NOT EXISTS `workspace`.`dq`" in statements


def test_ensure_databricks_schemas_skips_when_no_targets(monkeypatch):
    created: list[object] = []

    def _fake_create_engine(*args, **kwargs):
        created.append((args, kwargs))
        raise AssertionError("Should not be called")

    monkeypatch.setattr(databricks_bootstrap, "create_engine", _fake_create_engine)

    params = databricks_bootstrap.DatabricksConnectionParams(
        workspace_host="test.cloud.databricks.com",
        http_path="/sql/warehouse/123",
        access_token="token",
    )

    ensure_databricks_schemas(params)

    assert created == []