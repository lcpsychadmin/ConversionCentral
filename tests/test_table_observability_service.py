from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy.orm import Session

from app.models import (
    ConnectionTableSelection,
    DataQualityDataColumnCharacteristic,
    DataQualityDataTableCharacteristic,
    SystemConnection,
    TableObservabilityMetric as TableObservabilityMetricModel,
)
from app.services.table_observability import (
    TABLE_OBSERVABILITY_CATEGORY_KEYS,
    TableObservabilityRunService,
    TableObservabilityScheduleService,
)


def _build_connection() -> SystemConnection:
    return SystemConnection(
        display_name="Primary",
        connection_string="jdbc://example",
        auth_method="username_password",
        active=True,
    )


def _build_selection(connection: SystemConnection) -> ConnectionTableSelection:
    return ConnectionTableSelection(
        system_connection=connection,
        schema_name="analytics",
        table_name="orders",
        table_type="BASE TABLE",
        column_count=6,
        estimated_rows=12_500,
    )


def test_schedule_service_creates_default_categories(db_session: Session) -> None:
    service = TableObservabilityScheduleService(lambda: db_session)

    schedules = service.ensure_category_schedules(session=db_session)

    assert {schedule.category_name for schedule in schedules} == set(TABLE_OBSERVABILITY_CATEGORY_KEYS)
    assert all(schedule.cron_expression for schedule in schedules)


def test_run_service_persists_structural_metrics(db_session: Session) -> None:
    connection = _build_connection()
    selection = _build_selection(connection)
    table_stats = DataQualityDataTableCharacteristic(
        table_id="orders",
        schema_name=selection.schema_name,
        table_name=selection.table_name,
        record_count=15_000,
        column_count=6,
        data_point_count=90_000,
        latest_run_completed_at=datetime.now(timezone.utc) - timedelta(hours=2),
    )
    column_stats = DataQualityDataColumnCharacteristic(
        column_id="orders_id",
        schema_name=selection.schema_name,
        table_name=selection.table_name,
        column_name="id",
        data_type="bigint",
        pii_risk="high",
    )

    db_session.add_all([connection, selection, table_stats, column_stats])
    db_session.commit()

    service = TableObservabilityRunService(lambda: db_session)
    run = service.run_for_category("structural", session=db_session)
    db_session.commit()

    metrics = (
        db_session.query(TableObservabilityMetricModel)
        .filter(TableObservabilityMetricModel.run_id == run.id)
        .all()
    )

    assert len(metrics) == 4  # structural category emits four metrics per table
    row_metric = next(metric for metric in metrics if metric.metric_name == "row_count")
    assert row_metric.metric_value_number == Decimal("15000")
    assert row_metric.metric_status == "ok"

    refresh_metric = next(metric for metric in metrics if metric.metric_name == "storage_size")
    assert refresh_metric.metric_value_number is not None
    assert refresh_metric.metric_status in {"ok", "missing_data"}