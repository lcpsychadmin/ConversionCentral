from datetime import datetime, timezone
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.models import (
    ConnectionTableSelection,
    DataDefinition,
    DataDefinitionTable,
    DataObject,
    ProcessArea,
    Report,
    System,
    SystemConnection,
    Table,
)
from app.schemas import SystemConnectionType
from app.services.catalog_browser import CatalogTable, TablePreview


def _create_system(db: Session) -> System:
    system = System(
        id=uuid4(),
        name="Source System",
        physical_name="SRC_SYS",
        status="active",
    )
    db.add(system)
    db.flush()
    return system


def _create_connection(db: Session, system: System) -> SystemConnection:
    connection = SystemConnection(
        id=uuid4(),
        system_id=system.id,
        connection_type=SystemConnectionType.JDBC.value,
        connection_string="jdbc:postgresql://localhost:5432/example",
        auth_method="username_password",
        active=True,
    )
    db.add(connection)
    db.flush()
    return connection


def test_browse_catalog_merges_existing_selection(
    client: TestClient,
    db_session: Session,
    monkeypatch,
) -> None:
    system = _create_system(db_session)
    connection = _create_connection(db_session, system)

    existing_selection = ConnectionTableSelection(
        system_connection_id=connection.id,
        schema_name="public",
        table_name="customers",
        table_type="table",
        column_count=4,
    estimated_rows=50,
    last_seen_at=datetime.now(timezone.utc),
    )
    stale_selection = ConnectionTableSelection(
        system_connection_id=connection.id,
        schema_name="legacy",
        table_name="archived_orders",
        table_type="table",
        column_count=3,
    estimated_rows=10,
    last_seen_at=datetime.now(timezone.utc),
    )
    db_session.add_all([existing_selection, stale_selection])
    db_session.commit()

    def fake_fetch(connection_type: SystemConnectionType, connection_string: str):  # type: ignore[override]
        assert connection_type is SystemConnectionType.JDBC
        assert connection_string == connection.connection_string
        return [
            CatalogTable(
                schema_name="public",
                table_name="customers",
                table_type="table",
                column_count=5,
                estimated_rows=120,
            ),
            CatalogTable(
                schema_name="analytics",
                table_name="orders_view",
                table_type="view",
                column_count=6,
                estimated_rows=None,
            ),
        ]

    monkeypatch.setattr(
        "app.routers.system_connection.fetch_connection_catalog",
        fake_fetch,
    )

    response = client.get(f"/system-connections/{connection.id}/catalog")
    assert response.status_code == 200

    payload = response.json()
    assert payload == [
        {
            "schema_name": "analytics",
            "table_name": "orders_view",
            "table_type": "view",
            "column_count": 6,
            "estimated_rows": None,
            "selected": False,
            "available": True,
            "selection_id": None,
        },
        {
            "schema_name": "public",
            "table_name": "customers",
            "table_type": "table",
            "column_count": 5,
            "estimated_rows": 120,
            "selected": True,
            "available": True,
            "selection_id": str(existing_selection.id),
        },
        {
            "schema_name": "legacy",
            "table_name": "archived_orders",
            "table_type": "table",
            "column_count": 3,
            "estimated_rows": 10,
            "selected": True,
            "available": False,
            "selection_id": str(stale_selection.id),
        },
    ]

    db_session.refresh(existing_selection)
    assert existing_selection.column_count == 5
    assert existing_selection.estimated_rows == 120
    assert existing_selection.table_type == "table"
    assert existing_selection.last_seen_at is not None


def test_update_catalog_selection_persists_changes(
    client: TestClient,
    db_session: Session,
) -> None:
    system = _create_system(db_session)
    connection = _create_connection(db_session, system)
    db_session.commit()

    payload = {
        "selected_tables": [
            {
                "schema_name": "public",
                "table_name": "customers",
                "table_type": "table",
                "column_count": 5,
                "estimated_rows": 120,
            },
            {
                "schema_name": "sales",
                "table_name": "orders",
                "table_type": "table",
                "column_count": 8,
                "estimated_rows": 240,
            },
        ]
    }

    response = client.put(
        f"/system-connections/{connection.id}/catalog/selection",
        json=payload,
    )
    assert response.status_code == 204

    db_session.refresh(connection)
    assert len(connection.catalog_selections) == 2

    second_payload = {
        "selected_tables": [
            {
                "schema_name": "sales",
                "table_name": "orders",
                "table_type": "table",
                "column_count": 10,
                "estimated_rows": 300,
            }
        ]
    }

    response = client.put(
        f"/system-connections/{connection.id}/catalog/selection",
        json=second_payload,
    )
    assert response.status_code == 204

    db_session.refresh(connection)
    assert len(connection.catalog_selections) == 1
    selection = connection.catalog_selections[0]
    assert selection.schema_name == "sales"
    assert selection.table_name == "orders"
    assert selection.column_count == 10
    assert selection.estimated_rows == 300


def test_unselect_catalog_table_triggers_cascade(
    client: TestClient,
    db_session: Session,
    monkeypatch,
) -> None:
    process_area = ProcessArea(
        id=uuid4(),
        name="Finance",
        status="active",
    )
    data_object = DataObject(
        id=uuid4(),
        process_area_id=process_area.id,
        name="Customer Data",
        status="active",
    )

    system = _create_system(db_session)
    connection = _create_connection(db_session, system)

    table = Table(
        id=uuid4(),
        system_id=system.id,
        name="Customers",
        physical_name="customers",
        schema_name="public",
        status="active",
    )

    data_definition = DataDefinition(
        id=uuid4(),
        data_object_id=data_object.id,
        system_id=system.id,
        description=None,
    )
    definition_table = DataDefinitionTable(
        id=uuid4(),
        data_definition_id=data_definition.id,
        table_id=table.id,
        is_construction=False,
    )

    report_definition = {
        "tables": [
            {
                "tableId": str(table.id),
                "label": "Customers",
                "physicalName": "customers",
                "schemaName": "public",
                "alias": None,
                "position": {},
            }
        ],
        "joins": [],
        "columns": [],
        "criteriaRowCount": 0,
        "groupingEnabled": False,
    }
    report = Report(
        id=uuid4(),
        name="Customers by Region",
        status="draft",
        definition=report_definition,
        data_object_id=data_object.id,
        process_area_id=process_area.id,
    )

    selection = ConnectionTableSelection(
        system_connection_id=connection.id,
        schema_name="public",
        table_name="customers",
        table_type="table",
        column_count=5,
    estimated_rows=120,
    last_seen_at=datetime.now(timezone.utc),
    )

    db_session.add_all([
        process_area,
        data_object,
        table,
        data_definition,
        definition_table,
        report,
        selection,
    ])
    db_session.commit()

    dropped_tables: list[tuple[str | None, str]] = []

    class FakeStorage:
        def __init__(self) -> None:
            pass

        def drop_table(self, schema: str | None, table_name: str) -> None:
            dropped_tables.append((schema, table_name))

    monkeypatch.setattr(
        "app.services.connection_catalog_cleanup.DatabricksIngestionStorage",
        lambda: FakeStorage(),
    )

    response = client.put(
        f"/system-connections/{connection.id}/catalog/selection",
        json={"selected_tables": []},
    )
    assert response.status_code == 204

    db_session.expire_all()

    assert db_session.get(ConnectionTableSelection, selection.id) is None
    assert db_session.get(Table, table.id) is None
    assert db_session.get(DataDefinition, data_definition.id) is None
    assert db_session.get(DataDefinitionTable, definition_table.id) is None
    assert db_session.get(Report, report.id) is None
    assert dropped_tables == [("source_system", "public_customers")]


def test_catalog_preview_returns_data(
    client: TestClient,
    db_session: Session,
    monkeypatch,
) -> None:
    system = _create_system(db_session)
    connection = _create_connection(db_session, system)
    db_session.commit()

    def fake_preview(
        connection_type,
        connection_string,
        schema_name,
        table_name,
        limit,
    ) -> TablePreview:
        assert connection_string == connection.connection_string
        assert schema_name == "public"
        assert table_name == "customers"
        assert limit == 100
        return TablePreview(columns=["id", "name"], rows=[{"id": 1, "name": "Alice"}])

    monkeypatch.setattr(
        "app.routers.system_connection.fetch_table_preview",
        fake_preview,
    )

    response = client.get(
        f"/system-connections/{connection.id}/catalog/preview",
        params={"schema_name": "public", "table_name": "customers"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "columns": ["id", "name"],
        "rows": [{"id": 1, "name": "Alice"}],
    }


def test_connection_delete_triggers_catalog_cleanup(
    client: TestClient,
    db_session: Session,
    monkeypatch,
) -> None:
    system = _create_system(db_session)
    connection = _create_connection(db_session, system)

    process_area = ProcessArea(id=uuid4(), name="Finance")
    data_object = DataObject(
        id=uuid4(),
        process_area_id=process_area.id,
        name="Sales Orders",
    )
    table = Table(
        id=uuid4(),
        system_id=system.id,
        name="VBAK",
        physical_name="VBAK",
        schema_name=None,
    )
    data_definition = DataDefinition(
        id=uuid4(),
        data_object_id=data_object.id,
        system_id=system.id,
        description=None,
    )
    definition_table = DataDefinitionTable(
        id=uuid4(),
        data_definition_id=data_definition.id,
        table_id=table.id,
        is_construction=False,
    )

    selection = ConnectionTableSelection(
        system_connection_id=connection.id,
        schema_name="",
        table_name="VBAK",
        table_type="table",
        column_count=5,
    estimated_rows=120,
    last_seen_at=datetime.now(timezone.utc),
    )

    db_session.add_all([
        process_area,
        data_object,
        table,
        data_definition,
        definition_table,
        selection,
    ])
    db_session.commit()

    dropped_tables: list[tuple[str | None, str]] = []

    class FakeStorage:
        def drop_table(self, schema: str | None, table_name: str) -> None:
            dropped_tables.append((schema, table_name))

    monkeypatch.setattr(
        "app.services.connection_catalog_cleanup.DatabricksIngestionStorage",
        lambda: FakeStorage(),
    )

    response = client.delete(f"/system-connections/{connection.id}")
    assert response.status_code == 204

    db_session.expire_all()

    assert db_session.get(SystemConnection, connection.id) is None
    assert db_session.get(ConnectionTableSelection, selection.id) is None
    assert db_session.get(Table, table.id) is None
    assert db_session.get(DataDefinition, data_definition.id) is None
    assert db_session.get(DataDefinitionTable, definition_table.id) is None
    assert dropped_tables == [("source_system", "default_vbak")]
