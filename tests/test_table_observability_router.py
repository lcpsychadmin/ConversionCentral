from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy.orm import Session

from app.models import (
    ConnectionTableSelection,
    DataQualityDataColumnCharacteristic,
    DataQualityDataTableCharacteristic,
    SystemConnection,
)
from app.services.table_observability import (
    TABLE_OBSERVABILITY_CATEGORY_KEYS,
    TableObservabilityRunService,
)


def _seed_connection(db_session: Session) -> ConnectionTableSelection:
    connection = SystemConnection(
        display_name="Primary",
        connection_string="jdbc://example",
        auth_method="username_password",
        active=True,
    )
    selection = ConnectionTableSelection(
        system_connection=connection,
        schema_name="analytics",
        table_name="orders",
        table_type="BASE TABLE",
        column_count=6,
        estimated_rows=12_000,
        last_seen_at=datetime.now(timezone.utc) - timedelta(hours=4),
    )
    table_stats = DataQualityDataTableCharacteristic(
        table_id="orders",
        schema_name=selection.schema_name,
        table_name=selection.table_name,
        record_count=13_500,
        column_count=6,
        data_point_count=81_000,
        latest_run_completed_at=datetime.now(timezone.utc) - timedelta(hours=1),
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
    return selection


def test_list_schedules_returns_all_categories(client) -> None:
    response = client.get("/table-observability/schedules")
    assert response.status_code == 200
    payload = response.json()
    keys = {item["categoryKey"] for item in payload}
    assert keys.issuperset(set(TABLE_OBSERVABILITY_CATEGORY_KEYS))
    assert all("cronExpression" in item for item in payload)


def test_update_schedule_and_fetch_runs_and_metrics(client, db_session: Session) -> None:
    # Ensure schedules exist
    response = client.get("/table-observability/schedules")
    assert response.status_code == 200
    schedules = response.json()
    schedule_id = schedules[0]["scheduleId"]

    update_payload = {
        "cronExpression": "15 3 * * *",
        "timezone": "America/New_York",
        "isActive": True,
    }
    update_response = client.put(f"/table-observability/schedules/{schedule_id}", json=update_payload)
    assert update_response.status_code == 200
    body = update_response.json()
    assert body["cronExpression"] == "15 3 * * *"
    assert body["timezone"] == "America/New_York"

    selection = _seed_connection(db_session)

    run_service = TableObservabilityRunService(lambda: db_session)
    run = run_service.run_for_category("structural", session=db_session)
    db_session.commit()

    runs_response = client.get("/table-observability/runs", params={"categoryKey": "structural", "limit": 5})
    assert runs_response.status_code == 200
    runs = runs_response.json()
    assert any(item["runId"] == str(run.id) for item in runs)

    run_detail = client.get(f"/table-observability/runs/{run.id}")
    assert run_detail.status_code == 200
    assert run_detail.json()["metricsCollected"] >= 1

    metrics_response = client.get("/table-observability/metrics", params={"runId": str(run.id)})
    assert metrics_response.status_code == 200
    metrics = metrics_response.json()
    assert metrics, "Expected metrics for run"
    row_metric = next(item for item in metrics if item["metricName"] == "row_count")
    assert Decimal(str(row_metric["metricValueNumber"])) == Decimal("13500")

    table_metrics = client.get(
        "/table-observability/metrics",
        params={"selectionId": str(selection.id), "categoryKey": "structural", "limit": 10},
    )
    assert table_metrics.status_code == 200
    assert table_metrics.json()