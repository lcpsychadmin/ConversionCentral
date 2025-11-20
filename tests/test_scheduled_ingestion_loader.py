from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from app.ingestion.engine import reset_ingestion_engine
from app.services.databricks_sql import DatabricksConnectionParams
from sqlalchemy import Column, Integer, MetaData, Table

from app.services.data_quality_keys import build_project_key, build_table_group_id
from app.services.ingestion_loader import BaseTableLoader, DatabricksTableLoader, SparkTableLoader
from app.services.scheduled_ingestion import (
    IngestionOutcome,
    ScheduleSnapshot,
    ScheduledIngestionEngine,
    build_ingestion_schema_name,
    build_ingestion_table_name,
)
from app.schemas import IngestionLoadStrategy, IngestionRunStatus, SystemConnectionType
from app.models import (
    ConnectionTableSelection,
    DataObject,
    DataObjectSystem,
    IngestionRun,
    IngestionSchedule,
    ProcessArea,
    System,
    SystemConnection,
)


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


class _RecordingLoader(BaseTableLoader):
    def __init__(self, default_schema: str = "conversion_central") -> None:
        super().__init__(default_schema=default_schema)
        self.last_plan = None
        self.last_rows = 0

    def load_rows(  # type: ignore[override]
        self,
        plan,
        rows,
        columns,
    ) -> int:
        self.last_plan = plan
        self.last_rows = len(rows)
        return len(rows)


class _StubSystem:
    def __init__(self, name: str | None = None, physical_name: str | None = None) -> None:
        self.name = name
        self.physical_name = physical_name


class _StubConnection:
    def __init__(self, system: _StubSystem | None = None) -> None:
        self.system = system


def test_build_ingestion_schema_name_prefers_system_name() -> None:
    connection = _StubConnection(system=_StubSystem(name="CRM Prod", physical_name="CRM01"))
    assert build_ingestion_schema_name(connection) == "crm_prod"


def test_build_ingestion_schema_name_falls_back_to_physical_name() -> None:
    connection = _StubConnection(system=_StubSystem(name=None, physical_name="ERP_System"))
    assert build_ingestion_schema_name(connection) == "erp_system"


def test_build_ingestion_schema_name_handles_missing_metadata() -> None:
    connection = _StubConnection(system=None)
    assert build_ingestion_schema_name(connection) is None


def test_build_ingestion_table_name_prefixes_schema() -> None:
    connection = _StubConnection()

    class _Selection:  # minimal stub with required attributes
        schema_name = "Sales"
        table_name = "Orders"

    table_name = build_ingestion_table_name(connection, _Selection())
    assert table_name == "sales_orders"


def test_scheduled_ingestion_uses_system_schema_when_available() -> None:
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(
        "app.services.scheduled_ingestion.get_ingestion_connection_params",
        lambda: _stub_params("sql"),
    )
    loader = _RecordingLoader()
    engine = ScheduledIngestionEngine(loader=loader)
    metadata = MetaData()
    source_table = Table("orders", metadata, Column("id", Integer, primary_key=True))

    snapshot = ScheduleSnapshot(
        id=uuid4(),
        load_strategy=IngestionLoadStrategy.FULL,
        watermark_column=None,
        primary_key_column=None,
        batch_size=100,
        target_schema=None,
        system_schema="crm_prod",
        target_table_name="sales_orders",
        connection_string="",
        connection_type=SystemConnectionType.JDBC,
        source_schema="sales",
        source_table="orders",
        last_watermark_timestamp=None,
        last_watermark_id=None,
    )

    try:
        engine._flush_batch(snapshot, [{"id": 1}], True, False, source_table)  # type: ignore[arg-type]
    finally:
        monkeypatch.undo()

    assert loader.last_plan is not None
    assert loader.last_plan.schema == "crm_prod"
    assert loader.last_plan.table_name == "sales_orders"


def test_scheduled_ingestion_falls_back_to_loader_default_schema() -> None:
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(
        "app.services.scheduled_ingestion.get_ingestion_connection_params",
        lambda: _stub_params("sql"),
    )
    loader = _RecordingLoader(default_schema="conversion_central")
    engine = ScheduledIngestionEngine(loader=loader)
    metadata = MetaData()
    source_table = Table("orders", metadata, Column("id", Integer, primary_key=True))

    snapshot = ScheduleSnapshot(
        id=uuid4(),
        load_strategy=IngestionLoadStrategy.FULL,
        watermark_column=None,
        primary_key_column=None,
        batch_size=100,
        target_schema=None,
        system_schema=None,
        target_table_name="sales_orders",
        connection_string="",
        connection_type=SystemConnectionType.JDBC,
        source_schema="sales",
        source_table="orders",
        last_watermark_timestamp=None,
        last_watermark_id=None,
    )

    try:
        engine._flush_batch(snapshot, [{"id": 1}], True, False, source_table)  # type: ignore[arg-type]
    finally:
        monkeypatch.undo()

    assert loader.last_plan is not None
    assert loader.last_plan.schema == "conversion_central"
    assert loader.last_plan.table_name == "sales_orders"


def test_completed_ingestion_triggers_data_quality_run(db_session, monkeypatch: pytest.MonkeyPatch) -> None:
    engine = ScheduledIngestionEngine(session_factory=lambda: db_session)

    @contextmanager
    def _scope():
        try:
            yield db_session
            db_session.commit()
        except Exception:
            db_session.rollback()
            raise

    engine._session_scope = _scope  # type: ignore[assignment]

    captured: dict[str, object] = {}

    def _record(project_key: str, *, test_suite_key: str | None = None, trigger_source: str | None = None, status: str = "pending") -> None:
        captured["project_key"] = project_key
        captured["test_suite_key"] = test_suite_key
        captured["trigger_source"] = trigger_source
        captured["status"] = status

    monkeypatch.setattr("app.services.scheduled_ingestion.queue_validation_run", _record)

    system = System(name="CRM", physical_name="CRM")
    db_session.add(system)
    db_session.flush()

    process_area = ProcessArea(name="Sales")
    db_session.add(process_area)
    db_session.flush()

    data_object = DataObject(process_area_id=process_area.id, name="Pipeline")
    db_session.add(data_object)
    db_session.flush()

    db_session.add(DataObjectSystem(data_object_id=data_object.id, system_id=system.id))
    db_session.flush()

    connection = SystemConnection(
        system_id=system.id,
        connection_type=SystemConnectionType.JDBC.value,
        connection_string="jdbc://crm",
        auth_method="username_password",
        active=True,
        ingestion_enabled=True,
    )
    db_session.add(connection)
    db_session.flush()

    selection = ConnectionTableSelection(
        system_connection_id=connection.id,
        schema_name="sales",
        table_name="orders",
        table_type="table",
    )
    db_session.add(selection)
    db_session.flush()

    schedule = IngestionSchedule(
        connection_table_selection_id=selection.id,
        schedule_expression="0 * * * *",
        timezone="UTC",
        load_strategy=IngestionLoadStrategy.FULL.value,
        batch_size=100,
        is_active=True,
    )
    db_session.add(schedule)
    db_session.flush()

    run = IngestionRun(
        ingestion_schedule_id=schedule.id,
        status=IngestionRunStatus.RUNNING.value,
        started_at=datetime.now(timezone.utc),
    )
    db_session.add(run)
    db_session.commit()

    outcome = IngestionOutcome(
        rows_loaded=25,
        rows_expected=25,
        watermark_timestamp=datetime.now(timezone.utc),
        watermark_id=100,
        query_text="SELECT * FROM source",
    )

    engine._mark_run_completed(schedule.id, run.id, outcome)

    expected_project_key = build_project_key(system.id, data_object.id)
    expected_suite_key = build_table_group_id(connection.id, data_object.id)

    assert captured["project_key"] == expected_project_key
    assert captured["test_suite_key"] == expected_suite_key
    assert captured["trigger_source"] == f"ingestion-run:{run.id}"
    assert captured["status"] == "pending"
