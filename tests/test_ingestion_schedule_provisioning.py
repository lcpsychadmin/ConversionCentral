from __future__ import annotations

from uuid import uuid4

import pytest

from app.models import ConnectionTableSelection, IngestionSchedule, System, SystemConnection
from app.schemas import IngestionLoadStrategy, SystemConnectionType


@pytest.fixture
def _base_setup(db_session):
    system = System(id=uuid4(), name="CRM", physical_name="crm")
    db_session.add(system)
    db_session.flush()

    connection = SystemConnection(
        id=uuid4(),
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
        id=uuid4(),
        system_connection_id=connection.id,
        schema_name="sales",
        table_name="orders",
        table_type="table",
    )
    db_session.add(selection)
    db_session.commit()

    return system, connection, selection


def test_create_schedule_triggers_provisioning(client, monkeypatch, _base_setup):
    system, connection, selection = _base_setup

    recorded: dict[str, str | None] = {}

    def _record(*, reason: str | None = None, wait: bool = False) -> None:
        recorded["reason"] = reason
        recorded["wait"] = wait

    monkeypatch.setattr(
        "app.routers.ingestion_schedule.trigger_data_quality_provisioning",
        _record,
    )

    payload = {
        "connection_table_selection_id": str(selection.id),
        "schedule_expression": "0 * * * *",
        "load_strategy": IngestionLoadStrategy.FULL.value,
        "batch_size": 1000,
        "timezone": "UTC",
    }

    response = client.post("/ingestion-schedules", json=payload)
    assert response.status_code == 201
    assert recorded["reason"] == "schedule-created"
    assert recorded["wait"] is False


def test_update_schedule_triggers_provisioning(client, db_session, monkeypatch, _base_setup):
    system, connection, selection = _base_setup

    schedule = IngestionSchedule(
        connection_table_selection_id=selection.id,
        schedule_expression="0 * * * *",
        timezone="UTC",
        load_strategy=IngestionLoadStrategy.FULL.value,
        batch_size=1000,
        is_active=True,
    )
    db_session.add(schedule)
    db_session.commit()

    recorded: dict[str, str | None] = {}

    def _record(*, reason: str | None = None, wait: bool = False) -> None:
        recorded["reason"] = reason
        recorded["wait"] = wait

    monkeypatch.setattr(
        "app.routers.ingestion_schedule.trigger_data_quality_provisioning",
        _record,
    )

    payload = {
        "schedule_expression": "*/5 * * * *",
    }

    response = client.put(f"/ingestion-schedules/{schedule.id}", json=payload)
    assert response.status_code == 200
    assert recorded["reason"] == "schedule-updated"
    assert recorded["wait"] is False


def test_delete_schedule_triggers_provisioning(client, db_session, monkeypatch, _base_setup):
    system, connection, selection = _base_setup

    schedule = IngestionSchedule(
        connection_table_selection_id=selection.id,
        schedule_expression="0 * * * *",
        timezone="UTC",
        load_strategy=IngestionLoadStrategy.FULL.value,
        batch_size=1000,
        is_active=True,
    )
    db_session.add(schedule)
    db_session.commit()

    recorded: dict[str, str | None] = {}

    def _record(*, reason: str | None = None, wait: bool = False) -> None:
        recorded["reason"] = reason
        recorded["wait"] = wait

    monkeypatch.setattr(
        "app.routers.ingestion_schedule.trigger_data_quality_provisioning",
        _record,
    )

    response = client.delete(f"/ingestion-schedules/{schedule.id}")
    assert response.status_code == 204
    assert recorded["reason"] == "schedule-deleted"
    assert recorded["wait"] is False
