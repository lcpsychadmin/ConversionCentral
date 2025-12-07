from __future__ import annotations

from uuid import UUID, uuid4

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.models.entities import (
    ConstructedTable,
    DataDefinition,
    DataDefinitionTable,
    DataObject,
    ProcessArea,
    System,
    SystemConnection,
    Table,
    Workspace,
)


def _sample_definition() -> dict[str, object]:
    table_id = str(uuid4())
    field_id = str(uuid4())
    return {
        "tables": [
            {
                "tableId": table_id,
                "label": "Dummy Table",
                "physicalName": "dummy_table",
                "schemaName": None,
                "alias": None,
                "position": {"x": 0, "y": 0},
            }
        ],
        "joins": [],
        "columns": [
            {
                "order": 0,
                "fieldId": field_id,
                "fieldName": "Dummy Field",
                "fieldDescription": None,
                "tableId": table_id,
                "tableName": "Dummy Table",
                "show": True,
                "sort": "none",
                "aggregate": None,
                "criteria": [""],
            }
        ],
        "criteriaRowCount": 1,
        "groupingEnabled": False,
    }


def test_report_crud_and_publish_flow(client: TestClient, db_session: Session) -> None:
    workspace = Workspace(name="Insights Workspace", description="", is_default=True, is_active=True)
    db_session.add(workspace)
    db_session.commit()

    process_area_response = client.post(
        "/process-areas",
        json={"name": "Insights", "description": "", "status": "active"},
    )
    assert process_area_response.status_code == 201
    process_area_id = process_area_response.json()["id"]

    data_object_response = client.post(
        "/data-objects",
        json={
            "process_area_id": process_area_id,
            "name": "Revenue",
            "description": "",
            "status": "active",
            "system_ids": [],
        },
    )
    assert data_object_response.status_code == 201
    data_object_id = data_object_response.json()["id"]

    data_object = db_session.get(DataObject, UUID(data_object_id))
    assert data_object is not None
    data_object.workspace_id = workspace.id
    db_session.commit()

    create_payload = {
        "name": "Quarterly Sales",
        "description": "Initial draft",
        "definition": _sample_definition(),
        "workspaceId": str(workspace.id),
    }

    create_response = client.post("/reporting/reports", json=create_payload)
    assert create_response.status_code == 201
    created = create_response.json()
    assert created["status"] == "draft"
    assert created["publishedAt"] is None
    assert created["workspaceId"] == str(workspace.id)
    report_id = created["id"]

    list_response = client.get("/reporting/reports")
    assert list_response.status_code == 200
    listings = list_response.json()
    assert any(item["id"] == report_id for item in listings)

    draft_response = client.get("/reporting/reports", params={"status": "draft"})
    assert draft_response.status_code == 200
    draft_list = draft_response.json()
    assert draft_list and draft_list[0]["status"] == "draft"

    detail_response = client.get(f"/reporting/reports/{report_id}")
    assert detail_response.status_code == 200
    detail = detail_response.json()
    assert detail["name"] == "Quarterly Sales"
    assert detail["workspaceId"] == str(workspace.id)

    update_response = client.put(
        f"/reporting/reports/{report_id}",
        json={"description": "Updated narrative"},
    )
    assert update_response.status_code == 200
    updated = update_response.json()
    assert updated["description"] == "Updated narrative"

    publish_response = client.post(
        f"/reporting/reports/{report_id}/publish",
        json={
            "productTeamId": process_area_id,
            "dataObjectId": data_object_id,
            "workspaceId": str(workspace.id),
        },
    )
    assert publish_response.status_code == 200
    published = publish_response.json()
    assert published["status"] == "published"
    assert published["publishedAt"] is not None
    assert published["productTeamId"] == process_area_id
    assert published["dataObjectId"] == data_object_id
    assert published["workspaceId"] == str(workspace.id)

    published_listing = client.get("/reporting/reports", params={"status": "published"})
    assert published_listing.status_code == 200
    published_records = published_listing.json()
    assert any(item["id"] == report_id for item in published_records)

    delete_response = client.delete(f"/reporting/reports/{report_id}")
    assert delete_response.status_code == 204

    missing_response = client.get(f"/reporting/reports/{report_id}")
    assert missing_response.status_code == 404


def test_reporting_tables_filters_to_databricks(client: TestClient, db_session: Session) -> None:
    databricks_system = System(
        name="Databricks Source",
        physical_name="DB_SRC",
        status="active",
    )
    db_session.add(databricks_system)
    db_session.flush()

    databricks_table = Table(
        system_id=databricks_system.id,
        name="Fact Sales",
        physical_name="fact_sales",
        schema_name="analytics",
        status="active",
    )
    db_session.add(databricks_table)
    db_session.flush()

    databricks_connection = SystemConnection(
        connection_type="jdbc",
        connection_string="jdbc:databricks://workspace.cloud.databricks.com:443/default",
        auth_method="username_password",
        active=True,
        system=databricks_system,
    )
    db_session.add(databricks_connection)

    legacy_system = System(
        name="Legacy Warehouse",
        physical_name="LEGACY_WH",
        status="active",
    )
    db_session.add(legacy_system)
    db_session.flush()

    legacy_table = Table(
        system_id=legacy_system.id,
        name="Legacy Orders",
        physical_name="legacy_orders",
        schema_name="dbo",
        status="active",
    )
    db_session.add(legacy_table)
    db_session.flush()

    legacy_connection = SystemConnection(
        connection_type="jdbc",
        connection_string="jdbc:postgresql://localhost:5432/warehouse",
        auth_method="username_password",
        active=True,
        system=legacy_system,
    )
    db_session.add(legacy_connection)

    managed_system = System(
        name="Managed Source",
        physical_name="MANAGED_SRC",
        status="active",
    )
    db_session.add(managed_system)
    db_session.flush()

    process_area = ProcessArea(name="Analytics", description="", status="active")
    db_session.add(process_area)
    db_session.flush()

    data_object = DataObject(
        process_area_id=process_area.id,
        name="Revenue Model",
        description="",
        status="active",
    )
    db_session.add(data_object)
    db_session.flush()

    constructed_source_table = Table(
        system_id=managed_system.id,
        name="Constructed Revenue",
        physical_name="constructed_revenue",
        status="active",
    )
    db_session.add(constructed_source_table)
    db_session.flush()

    data_definition = DataDefinition(
        data_object_id=data_object.id,
        system_id=managed_system.id,
        description="",
    )
    db_session.add(data_definition)
    db_session.flush()

    definition_table = DataDefinitionTable(
        data_definition_id=data_definition.id,
        table_id=constructed_source_table.id,
        is_construction=True,
    )
    db_session.add(definition_table)
    db_session.flush()

    db_session.add(
        ConstructedTable(
            data_definition_id=data_definition.id,
            data_definition_table_id=definition_table.id,
            name="constructed_revenue",
            status="approved",
        )
    )

    db_session.commit()

    response = client.get("/reporting/tables")
    assert response.status_code == 200
    payload = response.json()
    returned_ids = {item["id"] for item in payload}

    assert str(databricks_table.id) in returned_ids
    assert str(constructed_source_table.id) in returned_ids
    assert str(legacy_table.id) not in returned_ids
