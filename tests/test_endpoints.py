from datetime import datetime
from http import HTTPStatus
from uuid import UUID, uuid4

from app.models import (
    ConstructedDataValidationRule,
    ConstructedField,
    ConstructedTable,
    User,
)


def test_dashboard_summary(client):
    response = client.get("/dashboard/summary")
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    expected_keys = {"projects", "releases", "validationIssues", "pendingApprovals"}
    assert expected_keys.issubset(data.keys())


def test_project_crud_flow(client):
    payload = {"name": "Project Alpha", "description": "Test project", "status": "planned"}
    response = client.post("/projects", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    project_id = data["id"]
    assert data["name"] == payload["name"]

    list_resp = client.get("/projects")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/projects/{project_id}",
        json={"name": "Project Alpha Updated", "status": "active"},
    )
    assert update_resp.status_code == HTTPStatus.OK
    assert update_resp.json()["name"] == "Project Alpha Updated"

    delete_resp = client.delete(f"/projects/{project_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT
    assert client.get("/projects").json() == []


def test_release_crud_flow(client):
    project_response = client.post(
        "/projects",
        json={"name": "Project Beta", "description": "", "status": "planned"},
    )
    project_id = project_response.json()["id"]

    payload = {
        "project_id": project_id,
        "name": "Release 1",
        "description": "First release",
        "status": "planned",
    }
    response = client.post("/releases", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    release_id = response.json()["id"]

    assert client.get("/releases").status_code == HTTPStatus.OK

    update_resp = client.put(
        f"/releases/{release_id}",
        json={"name": "Release 1A", "status": "active"},
    )
    assert update_resp.status_code == HTTPStatus.OK
    assert update_resp.json()["name"] == "Release 1A"

    delete_resp = client.delete(f"/releases/{release_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT
    assert client.get("/releases").json() == []


def test_mock_cycle_crud_flow(client):
    project_id = client.post(
        "/projects",
        json={"name": "Project Gamma", "description": "", "status": "planned"},
    ).json()["id"]
    release_id = client.post(
        "/releases",
        json={
            "project_id": project_id,
            "name": "Release Gamma",
            "description": "",
            "status": "planned",
        },
    ).json()["id"]

    payload = {
        "release_id": release_id,
        "name": "Mock Cycle 1",
        "description": "Dry run",
        "status": "planned",
    }
    response = client.post("/mock-cycles", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    mock_cycle_id = response.json()["id"]

    list_resp = client.get("/mock-cycles")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/mock-cycles/{mock_cycle_id}",
        json={"status": "executed"},
    )
    assert update_resp.status_code == HTTPStatus.OK
    assert update_resp.json()["status"] == "executed"

    delete_resp = client.delete(f"/mock-cycles/{mock_cycle_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def test_execution_context_crud_flow(client):
    project_id = client.post(
        "/projects",
        json={"name": "Project Delta", "description": "", "status": "planned"},
    ).json()["id"]
    release_id = client.post(
        "/releases",
        json={
            "project_id": project_id,
            "name": "Release Delta",
            "description": "",
            "status": "planned",
        },
    ).json()["id"]
    mock_cycle_id = client.post(
        "/mock-cycles",
        json={
            "release_id": release_id,
            "name": "Mock Cycle Delta",
            "description": "",
            "status": "planned",
        },
    ).json()["id"]

    payload = {
        "mock_cycle_id": mock_cycle_id,
        "name": "Execution Context 1",
        "description": "Baseline",
        "status": "planned",
    }
    response = client.post("/execution-contexts", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    execution_context_id = response.json()["id"]

    list_resp = client.get("/execution-contexts")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/execution-contexts/{execution_context_id}",
        json={"status": "completed"},
    )
    assert update_resp.status_code == HTTPStatus.OK
    assert update_resp.json()["status"] == "completed"

    delete_resp = client.delete(f"/execution-contexts/{execution_context_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def test_process_area_crud_flow(client):
    payload = {
        "name": "Finance",
        "description": "Finance processes",
        "status": "draft",
    }
    response = client.post("/process-areas", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    process_area_id = response.json()["id"]
    assert response.json()["name"] == "Finance"

    list_resp = client.get("/process-areas")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/process-areas/{process_area_id}",
        json={"status": "approved", "description": "Approved scope"},
    )
    assert update_resp.status_code == HTTPStatus.OK
    assert update_resp.json()["status"] == "approved"
    assert update_resp.json()["description"] == "Approved scope"

    delete_resp = client.delete(f"/process-areas/{process_area_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def test_data_object_crud_flow(client):
    process_area_id = client.post(
        "/process-areas",
        json={
            "name": "HR",
            "description": "Human resources",
            "status": "draft",
        },
    ).json()["id"]

    payload = {
        "process_area_id": process_area_id,
        "name": "Employees",
        "description": "Employee records",
        "status": "draft",
    }
    response = client.post("/data-objects", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    data_object_id = response.json()["id"]

    list_resp = client.get("/data-objects")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/data-objects/{data_object_id}",
        json={"status": "ready"},
    )
    assert update_resp.status_code == HTTPStatus.OK
    assert update_resp.json()["status"] == "ready"

    delete_resp = client.delete(f"/data-objects/{data_object_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def test_data_object_system_assignment_via_payload(client):
    process_area_id = client.post(
        "/process-areas",
        json={
            "name": "Analytics",
            "description": "Analytics domain",
            "status": "draft",
        },
    ).json()["id"]

    system_a = client.post(
        "/systems",
        json={
            "name": "Source CRM",
            "physical_name": "CRM_DB",
            "description": "Customer relationship platform",
            "status": "active",
        },
    ).json()["id"]
    system_b = client.post(
        "/systems",
        json={
            "name": "Analytics Warehouse",
            "physical_name": "ANALYTICS_WH",
            "description": "Reporting warehouse",
            "status": "active",
        },
    ).json()["id"]

    create_resp = client.post(
        "/data-objects",
        json={
            "process_area_id": process_area_id,
            "name": "Customer Facts",
            "description": "Customer-centric metrics",
            "status": "draft",
            "system_ids": [system_a, system_b],
        },
    )
    assert create_resp.status_code == HTTPStatus.CREATED
    data_object_id = create_resp.json()["id"]
    created_systems = {system["id"] for system in create_resp.json()["systems"]}
    assert created_systems == {system_a, system_b}

    update_resp = client.put(
        f"/data-objects/{data_object_id}",
        json={"system_ids": [system_b]},
    )
    assert update_resp.status_code == HTTPStatus.OK
    updated_systems = [system["id"] for system in update_resp.json()["systems"]]
    assert updated_systems == [system_b]

    list_resp = client.get("/data-objects")
    assert list_resp.status_code == HTTPStatus.OK
    results = {item["id"]: {system["id"] for system in item["systems"]} for item in list_resp.json()}
    assert results[data_object_id] == {system_b}

    invalid_resp = client.put(
        f"/data-objects/{data_object_id}",
        json={"system_ids": [system_a, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"]},
    )
    assert invalid_resp.status_code == HTTPStatus.NOT_FOUND


def test_data_definition_crud_flow(client):
    process_area_id = client.post(
        "/process-areas",
        json={
            "name": "Customer Operations",
            "description": "Handles customer data",
            "status": "draft",
        },
    ).json()["id"]

    system_id = client.post(
        "/systems",
        json={
            "name": "Source CRM",
            "physical_name": "SRC_CRM",
            "description": "CRM source",
            "status": "active",
        },
    ).json()["id"]

    data_object_id = client.post(
        "/data-objects",
        json={
            "process_area_id": process_area_id,
            "name": "Customer",
            "description": "Customer domain object",
            "status": "draft",
            "system_ids": [system_id],
        },
    ).json()["id"]

    table_id = client.post(
        "/tables",
        json={
            "system_id": system_id,
            "name": "Customer",
            "physical_name": "dbo.Customer",
            "schema_name": "dbo",
            "description": "Customer master table",
            "table_type": "BASE",
            "status": "active",
        },
    ).json()["id"]

    field_id = client.post(
        "/fields",
        json={
            "table_id": table_id,
            "name": "Customer Identifier",
            "description": "Primary customer key",
            "field_type": "uuid",
            "system_required": True,
            "business_process_required": True,
            "suppressed_field": False,
            "active": True,
            "security_classification": "internal",
            "data_validation": None,
            "reference_table": None,
            "grouping_tab": None,
        },
    ).json()["id"]

    create_resp = client.post(
        "/data-definitions",
        json={
            "data_object_id": data_object_id,
            "system_id": system_id,
            "description": "Customer definition",
            "tables": [
                {
                    "table_id": table_id,
                    "alias": "dim_customer",
                    "description": "Primary customer table",
                    "load_order": 1,
                    "fields": [
                        {
                            "field_id": field_id,
                            "notes": "Business key",
                        }
                    ],
                }
            ],
        },
    )
    assert create_resp.status_code == HTTPStatus.CREATED
    definition = create_resp.json()
    definition_id = definition["id"]
    assert definition["system"]["id"] == system_id
    assert len(definition["tables"]) == 1
    assert definition["tables"][0]["fields"][0]["field"]["id"] == field_id
    assert definition["tables"][0]["load_order"] == 1

    list_resp = client.get(
        f"/data-definitions?data_object_id={data_object_id}&system_id={system_id}"
    )
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/data-definitions/{definition_id}",
        json={
            "description": "Updated customer definition",
            "tables": [
                {
                    "table_id": table_id,
                    "alias": "customer_dim",
                    "description": "Updated description",
                    "load_order": 2,
                    "fields": [],
                }
            ],
        },
    )
    assert update_resp.status_code == HTTPStatus.OK
    updated = update_resp.json()
    assert updated["description"] == "Updated customer definition"
    assert updated["tables"][0]["alias"] == "customer_dim"
    assert updated["tables"][0]["fields"] == []
    assert updated["tables"][0]["load_order"] == 2

    delete_resp = client.delete(f"/data-definitions/{definition_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT

    after_delete = client.get(
        f"/data-definitions?data_object_id={data_object_id}&system_id={system_id}"
    )
    assert after_delete.status_code == HTTPStatus.OK
    assert after_delete.json() == []


def test_data_definition_requires_assignment(client):
    process_area_id = client.post(
        "/process-areas",
        json={
            "name": "Finance",
            "description": "Finance data",
            "status": "draft",
        },
    ).json()["id"]

    system_id = client.post(
        "/systems",
        json={
            "name": "Finance Warehouse",
            "physical_name": "FIN_WH",
            "description": "Finance warehouse",
            "status": "active",
        },
    ).json()["id"]

    data_object_id = client.post(
        "/data-objects",
        json={
            "process_area_id": process_area_id,
            "name": "Financials",
            "description": "Finance object",
            "status": "draft",
        },
    ).json()["id"]

    response = client.post(
        "/data-definitions",
        json={
            "data_object_id": data_object_id,
            "system_id": system_id,
            "tables": [],
        },
    )
    assert response.status_code == HTTPStatus.BAD_REQUEST

def test_release_data_object_linking(client):
    project_id = client.post(
        "/projects",
        json={"name": "Project Theta", "description": "", "status": "planned"},
    ).json()["id"]
    release_id = client.post(
        "/releases",
        json={
            "project_id": project_id,
            "name": "Release Theta",
            "description": "",
            "status": "planned",
        },
    ).json()["id"]
    process_area_id = client.post(
        "/process-areas",
        json={
            "name": "Operations",
            "description": "Operations area",
            "status": "draft",
        },
    ).json()["id"]
    data_object_id = client.post(
        "/data-objects",
        json={
            "process_area_id": process_area_id,
            "name": "Orders",
            "description": "Order facts",
            "status": "draft",
        },
    ).json()["id"]

    link_resp = client.post(
        "/release-data-objects",
        json={"release_id": release_id, "data_object_id": data_object_id},
    )
    assert link_resp.status_code == HTTPStatus.CREATED
    link_id = link_resp.json()["id"]

    list_resp = client.get(f"/release-data-objects?release_id={release_id}")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    duplicate_resp = client.post(
        "/release-data-objects",
        json={"release_id": release_id, "data_object_id": data_object_id},
    )
    assert duplicate_resp.status_code == HTTPStatus.CONFLICT

    delete_resp = client.delete(f"/release-data-objects/{link_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT

def test_system_crud_flow(client):
    payload = {
        "name": "ERP",
        "physical_name": "SAP_ERP",
        "description": "SAP system",
        "system_type": "SAP",
        "status": "active",
        "security_classification": "internal",
    }
    response = client.post("/systems", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    system_id = response.json()["id"]

    list_resp = client.get("/systems")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/systems/{system_id}",
        json={"description": "Updated SAP system"},
    )
    assert update_resp.status_code == HTTPStatus.OK
    assert update_resp.json()["description"] == "Updated SAP system"

    delete_resp = client.delete(f"/systems/{system_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def test_table_crud_flow(client):
    system_id = client.post(
        "/systems",
        json={
            "name": "CRM",
            "physical_name": "Dynamics",
            "description": "Dynamics 365",
            "status": "active",
        },
    ).json()["id"]

    payload = {
        "system_id": system_id,
        "name": "Customer",
        "physical_name": "dbo.Customer",
        "schema_name": "dbo",
        "description": "Customer master",
        "table_type": "BASE",
        "status": "active",
    }
    response = client.post("/tables", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    table_id = response.json()["id"]

    list_resp = client.get("/tables")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/tables/{table_id}",
        json={"description": "Customer master table"},
    )
    assert update_resp.status_code == HTTPStatus.OK
    assert update_resp.json()["description"] == "Customer master table"

    delete_resp = client.delete(f"/tables/{table_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def test_role_crud_flow(client):
    payload = {
        "name": "Data Steward",
        "description": "Responsible for data governance",
        "is_active": True,
    }
    response = client.post("/roles", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    role_id = response.json()["id"]

    list_resp = client.get("/roles")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/roles/{role_id}",
        json={"description": "Updated description", "is_active": False},
    )
    assert update_resp.status_code == HTTPStatus.OK
    body = update_resp.json()
    assert body["description"] == "Updated description"
    assert body["is_active"] is False

    delete_resp = client.delete(f"/roles/{role_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def test_user_crud_flow(client):
    payload = {
        "name": "Jane Analyst",
        "email": "jane.analyst@example.com",
        "status": "active",
    }
    response = client.post("/users", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    user_id = response.json()["id"]

    list_resp = client.get("/users")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/users/{user_id}",
        json={"status": "inactive", "email": "jane.updated@example.com"},
    )
    assert update_resp.status_code == HTTPStatus.OK
    body = update_resp.json()
    assert body["status"] == "inactive"
    assert body["email"] == "jane.updated@example.com"

    delete_resp = client.delete(f"/users/{user_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def test_field_crud_flow(client):
    system_id = client.post(
        "/systems",
        json={
            "name": "Finance",
            "physical_name": "SAP_FI",
            "description": "Finance system",
            "status": "active",
        },
    ).json()["id"]
    table_id = client.post(
        "/tables",
        json={
            "system_id": system_id,
            "name": "GL",
            "physical_name": "FI.GL",
            "schema_name": "FI",
            "description": "General ledger",
            "status": "active",
        },
    ).json()["id"]

    payload = {
        "table_id": table_id,
        "name": "GLAccount",
        "description": "General ledger account",
        "field_type": "VARCHAR",
        "field_length": 20,
        "decimal_places": None,
        "system_required": True,
        "business_process_required": False,
        "suppressed_field": False,
        "active": True,
        "security_classification": "internal",
        "data_validation": "Must exist in chart",
        "reference_table": "ChartOfAccounts",
    }
    response = client.post("/fields", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    field_id = response.json()["id"]

    list_resp = client.get("/fields")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/fields/{field_id}",
        json={"system_required": False, "field_length": 30},
    )
    assert update_resp.status_code == HTTPStatus.OK
    assert update_resp.json()["system_required"] is False
    assert update_resp.json()["field_length"] == 30

    delete_resp = client.delete(f"/fields/{field_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def test_data_object_system_crud_flow(client):
    project_id = client.post(
        "/projects",
        json={"name": "Project Omega", "description": "", "status": "planned"},
    ).json()["id"]
    process_area_id = client.post(
        "/process-areas",
        json={
            "project_id": project_id,
            "name": "Logistics",
            "description": "Logistics processes",
            "status": "draft",
        },
    ).json()["id"]
    data_object_id = client.post(
        "/data-objects",
        json={
            "process_area_id": process_area_id,
            "name": "Shipment",
            "description": "Shipment data",
            "status": "draft",
        },
    ).json()["id"]
    system_id = client.post(
        "/systems",
        json={
            "name": "Warehouse",
            "physical_name": "WMS",
            "description": "Warehouse system",
            "status": "active",
        },
    ).json()["id"]

    payload = {
        "data_object_id": data_object_id,
        "system_id": system_id,
        "relationship_type": "source",
        "description": "Source system for shipments",
    }
    response = client.post("/data-object-systems", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    link_id = response.json()["id"]

    list_resp = client.get("/data-object-systems")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/data-object-systems/{link_id}",
        json={"relationship_type": "target"},
    )
    assert update_resp.status_code == HTTPStatus.OK
    assert update_resp.json()["relationship_type"] == "target"

    delete_resp = client.delete(f"/data-object-systems/{link_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def test_data_definition_relationship_crud_flow(client):
    process_area_id = client.post(
        "/process-areas",
        json={"name": "Operations", "description": "Ops", "status": "draft"},
    ).json()["id"]

    system_id = client.post(
        "/systems",
        json={
            "name": "ERP",
            "physical_name": "erp_core",
            "description": "ERP Core",
            "status": "active",
        },
    ).json()["id"]

    data_object_id = client.post(
        "/data-objects",
        json={
            "process_area_id": process_area_id,
            "name": "Orders",
            "description": "Order data",
            "status": "draft",
            "system_ids": [system_id],
        },
    ).json()["id"]

    table_customer_id = client.post(
        "/tables",
        json={
            "system_id": system_id,
            "name": "Customers",
            "physical_name": "dbo.customers",
            "schema_name": "dbo",
            "status": "active",
        },
    ).json()["id"]

    table_order_id = client.post(
        "/tables",
        json={
            "system_id": system_id,
            "name": "Orders",
            "physical_name": "dbo.orders",
            "schema_name": "dbo",
            "status": "active",
        },
    ).json()["id"]

    customer_field_id = client.post(
        "/fields",
        json={
            "table_id": table_customer_id,
            "name": "CustomerId",
            "field_type": "uuid",
        },
    ).json()["id"]

    order_field_id = client.post(
        "/fields",
        json={
            "table_id": table_order_id,
            "name": "CustomerId",
            "field_type": "uuid",
        },
    ).json()["id"]

    definition_resp = client.post(
        "/data-definitions",
        json={
            "data_object_id": data_object_id,
            "system_id": system_id,
            "description": "Order relationships",
            "tables": [
                {
                    "table_id": table_customer_id,
                    "fields": [
                        {
                            "field_id": customer_field_id,
                        }
                    ],
                },
                {
                    "table_id": table_order_id,
                    "fields": [
                        {
                            "field_id": order_field_id,
                        }
                    ],
                },
            ],
        },
    )
    assert definition_resp.status_code == HTTPStatus.CREATED
    definition = definition_resp.json()
    definition_id = definition["id"]

    tables_by_source = {table["table_id"]: table for table in definition["tables"]}
    customer_definition_field_id = tables_by_source[table_customer_id]["fields"][0]["id"]
    order_definition_field_id = tables_by_source[table_order_id]["fields"][0]["id"]

    create_rel_resp = client.post(
        f"/data-definitions/{definition_id}/relationships",
        json={
            "primary_field_id": customer_definition_field_id,
            "foreign_field_id": order_definition_field_id,
            "relationship_type": "one_to_many",
            "notes": "A customer can have many orders",
        },
    )
    assert create_rel_resp.status_code == HTTPStatus.CREATED
    relationship = create_rel_resp.json()
    relationship_id = relationship["id"]
    assert relationship["relationship_type"] == "one_to_many"

    list_resp = client.get(f"/data-definitions/{definition_id}/relationships")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/data-definitions/{definition_id}/relationships/{relationship_id}",
        json={"relationship_type": "many_to_one", "notes": "Updated notes"},
    )
    assert update_resp.status_code == HTTPStatus.OK
    updated_rel = update_resp.json()
    assert updated_rel["relationship_type"] == "many_to_one"
    assert updated_rel["notes"] == "Updated notes"

    delete_resp = client.delete(
        f"/data-definitions/{definition_id}/relationships/{relationship_id}"
    )
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT

    assert client.get(f"/data-definitions/{definition_id}/relationships").json() == []


def test_process_area_role_assignment_crud_flow(client):
    project_id = client.post(
        "/projects",
        json={"name": "Project Security", "description": "", "status": "planned"},
    ).json()["id"]
    process_area_id = client.post(
        "/process-areas",
        json={
            "project_id": project_id,
            "name": "Compliance",
            "description": "Compliance processes",
            "status": "draft",
        },
    ).json()["id"]

    role_id = client.post(
        "/roles",
        json={
            "name": "Process Owner",
            "description": "Owns the product team",
        },
    ).json()["id"]
    secondary_role_id = client.post(
        "/roles",
        json={
            "name": "Data Custodian",
            "description": "Supports process owner",
        },
    ).json()["id"]

    user_id = client.post(
        "/users",
        json={
            "name": "Grace Owner",
            "email": "grace.owner@example.com",
        },
    ).json()["id"]
    grantor_id = client.post(
        "/users",
        json={
            "name": "Hank Admin",
            "email": "hank.admin@example.com",
        },
    ).json()["id"]

    payload = {
        "process_area_id": process_area_id,
        "user_id": user_id,
        "role_id": role_id,
        "granted_by": grantor_id,
        "notes": "Initial assignment",
    }
    response = client.post("/process-area-role-assignments", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    assignment_id = response.json()["id"]

    list_resp = client.get("/process-area-role-assignments")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/process-area-role-assignments/{assignment_id}",
        json={
            "role_id": secondary_role_id,
            "notes": "Reassigned for coverage",
        },
    )
    assert update_resp.status_code == HTTPStatus.OK
    body = update_resp.json()
    assert body["role_id"] == secondary_role_id
    assert body["notes"] == "Reassigned for coverage"

    delete_resp = client.delete(f"/process-area-role-assignments/{assignment_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def test_field_load_crud_flow(client):
    project_id = client.post(
        "/projects",
        json={"name": "Project Sigma", "description": "", "status": "planned"},
    ).json()["id"]
    release_id = client.post(
        "/releases",
        json={
            "project_id": project_id,
            "name": "Release Sigma",
            "description": "",
            "status": "planned",
        },
    ).json()["id"]
    system_id = client.post(
        "/systems",
        json={
            "name": "Analytics",
            "physical_name": "DW",
            "description": "Data warehouse",
            "status": "active",
        },
    ).json()["id"]
    table_id = client.post(
        "/tables",
        json={
            "system_id": system_id,
            "name": "FactSales",
            "physical_name": "dw.fact_sales",
            "schema_name": "dw",
            "description": "Sales fact",
            "status": "active",
        },
    ).json()["id"]
    field_id = client.post(
        "/fields",
        json={
            "table_id": table_id,
            "name": "SalesAmount",
            "description": "Total sales amount",
            "field_type": "NUMERIC",
            "field_length": 18,
            "decimal_places": 2,
            "system_required": True,
            "business_process_required": True,
            "security_classification": "internal",
        },
    ).json()["id"]

    payload = {
        "release_id": release_id,
        "field_id": field_id,
        "load_flag": True,
        "notes": "Include in initial load",
    }
    response = client.post("/field-loads", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    field_load_id = response.json()["id"]
    assert response.json()["load_flag"] is True

    list_resp = client.get("/field-loads")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/field-loads/{field_load_id}",
        json={"load_flag": False, "notes": "Exclude from delta"},
    )
    assert update_resp.status_code == HTTPStatus.OK
    body = update_resp.json()
    assert body["load_flag"] is False
    assert body["notes"] == "Exclude from delta"

    delete_resp = client.delete(f"/field-loads/{field_load_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def _create_release_with_project(client, project_name: str, release_name: str) -> str:
    project_id = client.post(
        "/projects",
        json={"name": project_name, "description": "", "status": "planned"},
    ).json()["id"]
    release_id = client.post(
        "/releases",
        json={
            "project_id": project_id,
            "name": release_name,
            "description": "",
            "status": "planned",
        },
    ).json()["id"]
    return release_id


def _create_field(client, system_name: str, table_name: str, field_name: str) -> str:
    system_id = client.post(
        "/systems",
        json={
            "name": system_name,
            "physical_name": system_name.replace(" ", "_")[:200],
            "description": system_name,
            "status": "active",
        },
    ).json()["id"]
    table_id = client.post(
        "/tables",
        json={
            "system_id": system_id,
            "name": table_name,
            "physical_name": f"{table_name.lower()}",
            "schema_name": "dbo",
            "status": "active",
        },
    ).json()["id"]
    field_id = client.post(
        "/fields",
        json={
            "table_id": table_id,
            "name": field_name,
            "description": f"{field_name} field",
            "field_type": "VARCHAR",
            "field_length": 255,
            "system_required": False,
            "business_process_required": False,
        },
    ).json()["id"]
    return field_id


def _create_execution_context(
    client,
    project_name: str,
    release_name: str,
    mock_cycle_name: str,
    execution_context_name: str,
) -> str:
    project_id = client.post(
        "/projects",
        json={"name": project_name, "description": "", "status": "planned"},
    ).json()["id"]
    release_id = client.post(
        "/releases",
        json={
            "project_id": project_id,
            "name": release_name,
            "description": "",
            "status": "planned",
        },
    ).json()["id"]
    mock_cycle_id = client.post(
        "/mock-cycles",
        json={
            "release_id": release_id,
            "name": mock_cycle_name,
            "description": "",
            "status": "planned",
        },
    ).json()["id"]
    execution_context_id = client.post(
        "/execution-contexts",
        json={
            "mock_cycle_id": mock_cycle_id,
            "name": execution_context_name,
            "description": "",
            "status": "planned",
        },
    ).json()["id"]
    return execution_context_id


def _create_user(db_session, name: str, email: str) -> str:
    user = User(name=name, email=email, status="active")
    db_session.add(user)
    db_session.commit()
    return str(user.id)


def _setup_construction_domain(client):
    suffix = uuid4().hex[:8]

    process_area_id = client.post(
        "/process-areas",
        json={"name": f"Construction Area {suffix}", "description": "", "status": "draft"},
    ).json()["id"]

    system_id = client.post(
        "/systems",
        json={
            "name": f"Construction System {suffix}",
            "physical_name": f"CON_SYS_{suffix}",
            "description": "",
            "system_type": "source",
            "status": "active",
        },
    ).json()["id"]

    client.post(
        "/system-connections",
        json={
            "system_id": system_id,
            "connection_type": "jdbc",
            "connection_string": "jdbc:mssql://localhost:1433/construction",
            "auth_method": "username_password",
            "active": True,
            "ingestion_enabled": True,
        },
    )

    data_object_id = client.post(
        "/data-objects",
        json={
            "process_area_id": process_area_id,
            "name": f"Construction Object {suffix}",
            "description": "",
            "status": "active",
            "system_ids": [system_id],
        },
    ).json()["id"]

    table_id = client.post(
        "/tables",
        json={
            "system_id": system_id,
            "name": f"Construction Table {suffix}",
            "physical_name": f"CON_TABLE_{suffix}",
            "schema_name": "dbo",
            "description": "",
            "table_type": "table",
            "status": "active",
        },
    ).json()["id"]

    field_one_name = f"ConstructFieldA{suffix}"
    field_two_name = f"ConstructFieldB{suffix}"

    field_one_id = client.post(
        "/fields",
        json={
            "table_id": table_id,
            "name": field_one_name,
            "description": "",
            "field_type": "VARCHAR",
            "field_length": 255,
            "system_required": True,
            "business_process_required": False,
        },
    ).json()["id"]

    field_two_id = client.post(
        "/fields",
        json={
            "table_id": table_id,
            "name": field_two_name,
            "description": "",
            "field_type": "INTEGER",
            "system_required": False,
            "business_process_required": False,
        },
    ).json()["id"]

    return {
        "process_area_id": process_area_id,
        "system_id": system_id,
        "data_object_id": data_object_id,
        "table_id": table_id,
        "field_one_id": field_one_id,
        "field_two_id": field_two_id,
        "field_one_name": field_one_name,
        "field_two_name": field_two_name,
    }


def test_data_definition_construction_sync_creates_constructed_table(client, db_session):
    setup = _setup_construction_domain(client)

    payload = {
        "data_object_id": setup["data_object_id"],
        "system_id": setup["system_id"],
        "description": "Construction definition",
        "tables": [
            {
                "table_id": setup["table_id"],
                "alias": "Constructed Table",
                "is_construction": True,
                "fields": [
                    {"field_id": setup["field_one_id"]},
                    {"field_id": setup["field_two_id"]},
                ],
            }
        ],
    }

    response = client.post("/data-definitions", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    definition = response.json()
    assert definition["tables"], "Expected constructed tables in response"
    table_entry = definition["tables"][0]
    assert table_entry["constructedTableId"], "Constructed table id should be returned"
    definition_table_id = UUID(table_entry["id"])
    definition_id = UUID(definition["id"])

    constructed_table = (
        db_session.query(ConstructedTable)
        .filter(ConstructedTable.data_definition_table_id == definition_table_id)
        .one()
    )

    assert constructed_table.status == "approved"
    assert constructed_table.data_definition_id == definition_id
    audit_field_names = {
        "Created By",
        "Created Date",
        "Modified By",
        "Modified Date",
    }
    constructed_field_names = {field.name for field in constructed_table.fields}
    expected_field_names = {
        setup["field_one_name"],
        setup["field_two_name"],
    } | audit_field_names
    assert constructed_field_names == expected_field_names

    response_field_names = {
        field_entry["field"]["name"] for field_entry in table_entry["fields"]
    }
    assert audit_field_names.issubset(response_field_names)


def test_data_definition_construction_sync_removes_constructed_table(client, db_session):
    setup = _setup_construction_domain(client)

    create_payload = {
        "data_object_id": setup["data_object_id"],
        "system_id": setup["system_id"],
        "description": "Construction definition",
        "tables": [
            {
                "table_id": setup["table_id"],
                "alias": "Constructed Table",
                "is_construction": True,
                "fields": [
                    {"field_id": setup["field_one_id"]},
                    {"field_id": setup["field_two_id"]},
                ],
            }
        ],
    }

    create_response = client.post("/data-definitions", json=create_payload)
    assert create_response.status_code == HTTPStatus.CREATED
    definition = create_response.json()

    update_payload = {
        "tables": [
            {
                "table_id": setup["table_id"],
                "alias": "Constructed Table",
                "is_construction": False,
                "fields": [
                    {"field_id": setup["field_one_id"]},
                    {"field_id": setup["field_two_id"]},
                ],
            }
        ]
    }

    update_response = client.put(f"/data-definitions/{definition['id']}", json=update_payload)
    assert update_response.status_code == HTTPStatus.OK

    definition_id = UUID(definition["id"])

    remaining = (
        db_session.query(ConstructedTable)
        .filter(ConstructedTable.data_definition_id == definition_id)
        .all()
    )
    assert remaining == []


def test_mapping_set_business_rules(client):
    release_id = _create_release_with_project(client, "Project Mapping", "Release Mapping")

    payload_v1 = {
        "release_id": release_id,
        "version": 1,
        "status": "draft",
        "notes": "Initial mapping set",
    }
    response_v1 = client.post("/mapping-sets", json=payload_v1)
    assert response_v1.status_code == HTTPStatus.CREATED
    mapping_set_id_v1 = response_v1.json()["id"]

    duplicate_version = client.post(
        "/mapping-sets",
        json={
            "release_id": release_id,
            "version": 1,
            "status": "draft",
        },
    )
    assert duplicate_version.status_code == HTTPStatus.BAD_REQUEST

    payload_v2 = {
        "release_id": release_id,
        "version": 2,
        "status": "draft",
    }
    response_v2 = client.post("/mapping-sets", json=payload_v2)
    assert response_v2.status_code == HTTPStatus.CREATED
    mapping_set_id_v2 = response_v2.json()["id"]

    activate_first = client.put(
        f"/mapping-sets/{mapping_set_id_v1}",
        json={"status": "active"},
    )
    assert activate_first.status_code == HTTPStatus.OK
    assert activate_first.json()["status"] == "active"

    activate_second = client.put(
        f"/mapping-sets/{mapping_set_id_v2}",
        json={"status": "active"},
    )
    assert activate_second.status_code == HTTPStatus.BAD_REQUEST

    delete_resp = client.delete(f"/mapping-sets/{mapping_set_id_v2}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT
    assert client.delete(f"/mapping-sets/{mapping_set_id_v1}").status_code == HTTPStatus.NO_CONTENT


def test_mapping_crud_flow(client):
    release_id = _create_release_with_project(client, "Project Mapping 2", "Release Mapping 2")

    mapping_set_id = client.post(
        "/mapping-sets",
        json={
            "release_id": release_id,
            "version": 1,
            "status": "draft",
            "notes": "For CRUD test",
        },
    ).json()["id"]

    source_field_id = _create_field(client, "Source System", "SourceTable", "SourceField")
    target_field_id = _create_field(client, "Target System", "TargetTable", "TargetField")

    payload = {
        "mapping_set_id": mapping_set_id,
        "source_field_id": source_field_id,
        "target_field_id": target_field_id,
        "transformation_rule": "UPPER(source)",
        "default_value": "",
        "notes": "Initial mapping",
        "status": "draft",
    }
    response = client.post("/mappings", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    mapping_id = response.json()["id"]

    list_resp = client.get("/mappings")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) >= 1

    update_resp = client.put(
        f"/mappings/{mapping_id}",
        json={"status": "approved", "notes": "Reviewed"},
    )
    assert update_resp.status_code == HTTPStatus.OK
    assert update_resp.json()["status"] == "approved"
    assert update_resp.json()["notes"] == "Reviewed"

    delete_resp = client.delete(f"/mappings/{mapping_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT

    assert client.delete(f"/mapping-sets/{mapping_set_id}").status_code == HTTPStatus.NO_CONTENT


def test_pre_load_validation_result_crud_flow(client):
    release_id = _create_release_with_project(
        client, "Project Validation", "Release Validation"
    )

    payload = {
        "release_id": release_id,
        "name": "Pre Validation",
        "status": "pending",
        "total_checks": 5,
        "passed_checks": 4,
        "failed_checks": 1,
    }
    response = client.post("/pre-load-validation-results", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    validation_result_id = response.json()["id"]

    list_resp = client.get("/pre-load-validation-results")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/pre-load-validation-results/{validation_result_id}",
        json={"status": "completed", "failed_checks": 0},
    )
    assert update_resp.status_code == HTTPStatus.OK
    body = update_resp.json()
    assert body["status"] == "completed"
    assert body["failed_checks"] == 0

    delete_resp = client.delete(f"/pre-load-validation-results/{validation_result_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def test_pre_load_validation_issue_crud_flow(client):
    release_id = _create_release_with_project(client, "Project Validation 2", "Release Validation 2")
    validation_result_id = client.post(
        "/pre-load-validation-results",
        json={
            "release_id": release_id,
            "name": "Pre Validation With Issues",
        },
    ).json()["id"]

    payload = {
        "validation_result_id": validation_result_id,
        "description": "Missing reference data",
        "severity": "high",
    }
    response = client.post("/pre-load-validation-issues", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    validation_issue_id = response.json()["id"]

    list_resp = client.get("/pre-load-validation-issues")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/pre-load-validation-issues/{validation_issue_id}",
        json={"status": "resolved", "resolution_notes": "Reloaded reference table"},
    )
    assert update_resp.status_code == HTTPStatus.OK
    body = update_resp.json()
    assert body["status"] == "resolved"
    assert body["resolution_notes"] == "Reloaded reference table"

    delete_resp = client.delete(f"/pre-load-validation-issues/{validation_issue_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def test_post_load_validation_result_crud_flow(client):
    release_id = _create_release_with_project(client, "Project Post Validation", "Release Post Validation")

    payload = {
        "release_id": release_id,
        "name": "Post Validation",
        "total_checks": 10,
        "passed_checks": 10,
    }
    response = client.post("/post-load-validation-results", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    validation_result_id = response.json()["id"]

    list_resp = client.get("/post-load-validation-results")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/post-load-validation-results/{validation_result_id}",
        json={"status": "completed", "notes": "All metrics passed"},
    )
    assert update_resp.status_code == HTTPStatus.OK
    body = update_resp.json()
    assert body["status"] == "completed"
    assert body["notes"] == "All metrics passed"

    delete_resp = client.delete(f"/post-load-validation-results/{validation_result_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def test_post_load_validation_issue_crud_flow(client):
    release_id = _create_release_with_project(client, "Project Post Validation 2", "Release Post Validation 2")
    validation_result_id = client.post(
        "/post-load-validation-results",
        json={
            "release_id": release_id,
            "name": "Post Validation With Issues",
        },
    ).json()["id"]

    payload = {
        "validation_result_id": validation_result_id,
        "description": "Record count mismatch",
        "severity": "medium",
        "record_identifier": "Batch-42",
    }
    response = client.post("/post-load-validation-issues", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    validation_issue_id = response.json()["id"]

    list_resp = client.get("/post-load-validation-issues")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/post-load-validation-issues/{validation_issue_id}",
        json={"status": "resolved", "resolution_notes": "Reconciled with source"},
    )
    assert update_resp.status_code == HTTPStatus.OK
    body = update_resp.json()
    assert body["status"] == "resolved"
    assert body["resolution_notes"] == "Reconciled with source"

    delete_resp = client.delete(f"/post-load-validation-issues/{validation_issue_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def test_pre_load_validation_approval_crud_flow(client, db_session):
    user_id = _create_user(db_session, "Alice Approver", "alice.approver@example.com")
    release_id = _create_release_with_project(client, "Project Approval", "Release Approval")
    result_id = client.post(
        "/pre-load-validation-results",
        json={
            "release_id": release_id,
            "name": "Pre Validation For Approval",
        },
    ).json()["id"]

    payload = {
        "pre_load_validation_result_id": result_id,
        "approver_id": user_id,
        "role": "approver",
        "decision": "approved",
        "comments": "Looks good",
    }
    response = client.post("/pre-load-validation-approvals", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    approval_id = response.json()["id"]

    list_resp = client.get("/pre-load-validation-approvals")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/pre-load-validation-approvals/{approval_id}",
        json={"decision": "rejected", "comments": "Needs fixes"},
    )
    assert update_resp.status_code == HTTPStatus.OK
    body = update_resp.json()
    assert body["decision"] == "rejected"
    assert body["comments"] == "Needs fixes"

    delete_resp = client.delete(f"/pre-load-validation-approvals/{approval_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def test_post_load_validation_approval_crud_flow(client, db_session):
    user_id = _create_user(db_session, "Bob Approver", "bob.approver@example.com")
    release_id = _create_release_with_project(client, "Project Approval 2", "Release Approval 2")
    result_id = client.post(
        "/post-load-validation-results",
        json={
            "release_id": release_id,
            "name": "Post Validation For Approval",
        },
    ).json()["id"]

    payload = {
        "post_load_validation_result_id": result_id,
        "approver_id": user_id,
        "role": "sme",
        "decision": "approved",
    }
    response = client.post("/post-load-validation-approvals", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    approval_id = response.json()["id"]

    list_resp = client.get("/post-load-validation-approvals")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/post-load-validation-approvals/{approval_id}",
        json={"role": "admin", "comments": "Signed off"},
    )
    assert update_resp.status_code == HTTPStatus.OK
    body = update_resp.json()
    assert body["role"] == "admin"
    assert body["comments"] == "Signed off"

    delete_resp = client.delete(f"/post-load-validation-approvals/{approval_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def test_execution_context_ready_for_load_requires_preload_approvals(client, db_session):
    project_id = client.post(
        "/projects",
        json={"name": "Project Ready", "description": "", "status": "planned"},
    ).json()["id"]
    release_id = client.post(
        "/releases",
        json={
            "project_id": project_id,
            "name": "Release Ready",
            "description": "",
            "status": "planned",
        },
    ).json()["id"]
    mock_cycle_id = client.post(
        "/mock-cycles",
        json={
            "release_id": release_id,
            "name": "Mock Ready",
            "description": "",
            "status": "planned",
        },
    ).json()["id"]
    execution_context_id = client.post(
        "/execution-contexts",
        json={
            "mock_cycle_id": mock_cycle_id,
            "name": "Execution Ready",
            "description": "",
            "status": "planned",
        },
    ).json()["id"]

    result_id = client.post(
        "/pre-load-validation-results",
        json={
            "release_id": release_id,
            "name": "Pre Validation Ready",
        },
    ).json()["id"]

    # Attempt transition without approvals
    ready_attempt = client.put(
        f"/execution-contexts/{execution_context_id}",
        json={"status": "ReadyForLoad"},
    )
    assert ready_attempt.status_code == HTTPStatus.BAD_REQUEST

    user_id = _create_user(db_session, "Carol Approver", "carol.approver@example.com")
    approval_id = client.post(
        "/pre-load-validation-approvals",
        json={
            "pre_load_validation_result_id": result_id,
            "approver_id": user_id,
            "role": "approver",
            "decision": "rejected",
        },
    ).json()["id"]

    # Decision rejected should still block
    rejected_attempt = client.put(
        f"/execution-contexts/{execution_context_id}",
        json={"status": "ReadyForLoad"},
    )
    assert rejected_attempt.status_code == HTTPStatus.BAD_REQUEST

    client.put(
        f"/pre-load-validation-approvals/{approval_id}",
        json={"decision": "approved"},
    )

    successful_attempt = client.put(
        f"/execution-contexts/{execution_context_id}",
        json={"status": "ReadyForLoad"},
    )
    assert successful_attempt.status_code == HTTPStatus.OK
    assert successful_attempt.json()["status"] == "ReadyForLoad"


def test_constructed_table_crud_flow_requires_approval(client, db_session):
    execution_context_id = _create_execution_context(
        client,
        "Project Constructed",
        "Release Constructed",
        "Mock Constructed",
        "Execution Constructed",
    )

    payload = {
        "execution_context_id": execution_context_id,
        "name": "Reference Geography",
        "description": "Supplemental geography mappings",
        "purpose": "Enable cross-region harmonization",
    }
    response = client.post("/constructed-tables", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    constructed_table_id = response.json()["id"]

    list_resp = client.get("/constructed-tables")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/constructed-tables/{constructed_table_id}",
        json={"purpose": "Updated purpose"},
    )
    assert update_resp.status_code == HTTPStatus.OK
    assert update_resp.json()["purpose"] == "Updated purpose"

    approval_attempt = client.put(
        f"/constructed-tables/{constructed_table_id}",
        json={"status": "approved"},
    )
    assert approval_attempt.status_code == HTTPStatus.BAD_REQUEST

    approver_id = _create_user(db_session, "Dana Steward", "dana.steward@example.com")
    approval_resp = client.post(
        "/constructed-table-approvals",
        json={
            "constructed_table_id": constructed_table_id,
            "approver_id": approver_id,
            "role": "data_steward",
            "decision": "approved",
            "comments": "Validated",
        },
    )
    assert approval_resp.status_code == HTTPStatus.CREATED

    final_resp = client.put(
        f"/constructed-tables/{constructed_table_id}",
        json={"status": "approved"},
    )
    assert final_resp.status_code == HTTPStatus.OK
    assert final_resp.json()["status"] == "approved"

    delete_resp = client.delete(f"/constructed-tables/{constructed_table_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def test_constructed_field_crud_flow(client):
    execution_context_id = _create_execution_context(
        client,
        "Project Constructed Field",
        "Release Constructed Field",
        "Mock Constructed Field",
        "Execution Constructed Field",
    )
    constructed_table_id = client.post(
        "/constructed-tables",
        json={
            "execution_context_id": execution_context_id,
            "name": "Exchange Rates",
            "description": "Currency conversion data",
        },
    ).json()["id"]

    payload = {
        "constructed_table_id": constructed_table_id,
        "name": "CurrencyCode",
        "data_type": "VARCHAR",
        "is_nullable": False,
        "description": "ISO currency code",
    }
    response = client.post("/constructed-fields", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    constructed_field_id = response.json()["id"]

    list_resp = client.get("/constructed-fields")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/constructed-fields/{constructed_field_id}",
        json={"is_nullable": True, "default_value": "USD"},
    )
    assert update_resp.status_code == HTTPStatus.OK
    body = update_resp.json()
    assert body["is_nullable"] is True
    assert body["default_value"] == "USD"

    delete_resp = client.delete(f"/constructed-fields/{constructed_field_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def test_constructed_data_flow_requires_table_approval(client, db_session):
    execution_context_id = _create_execution_context(
        client,
        "Project Constructed Data",
        "Release Constructed Data",
        "Mock Constructed Data",
        "Execution Constructed Data",
    )
    constructed_table_id = client.post(
        "/constructed-tables",
        json={
            "execution_context_id": execution_context_id,
            "name": "Country Overrides",
        },
    ).json()["id"]

    pre_approval_resp = client.post(
        "/constructed-data",
        json={
            "constructed_table_id": constructed_table_id,
            "row_identifier": "US",
            "payload": {"Country": "United States"},
        },
    )
    assert pre_approval_resp.status_code == HTTPStatus.BAD_REQUEST

    approver_id = _create_user(db_session, "Erin Owner", "erin.owner@example.com")
    client.post(
        "/constructed-table-approvals",
        json={
            "constructed_table_id": constructed_table_id,
            "approver_id": approver_id,
            "role": "business_owner",
            "decision": "approved",
        },
    )
    client.put(
        f"/constructed-tables/{constructed_table_id}",
        json={"status": "approved"},
    )

    create_resp = client.post(
        "/constructed-data",
        json={
            "constructed_table_id": constructed_table_id,
            "row_identifier": "US",
            "payload": {"Country": "United States", "ISO": "USA"},
        },
    )
    assert create_resp.status_code == HTTPStatus.CREATED
    constructed_data_id = create_resp.json()["id"]

    list_resp = client.get("/constructed-data")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/constructed-data/{constructed_data_id}",
        json={
            "payload": {"Country": "United States of America", "ISO": "USA"},
        },
    )
    assert update_resp.status_code == HTTPStatus.OK
    assert update_resp.json()["payload"]["Country"] == "United States of America"

    delete_resp = client.delete(f"/constructed-data/{constructed_data_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def test_constructed_data_batch_save_returns_rule_metadata(client, db_session):
    table = ConstructedTable(name="Validation Metadata", status="approved")
    db_session.add(table)
    db_session.flush()

    field = ConstructedField(
        constructed_table_id=table.id,
        name="Email",
        data_type="string",
        is_nullable=False,
        display_order=0,
    )
    table.fields.append(field)
    db_session.add(field)
    db_session.flush()

    rule = ConstructedDataValidationRule(
        constructed_table_id=table.id,
        field_id=field.id,
        name="Email must be present",
        description="Auto rule for email presence",
        rule_type="required",
        configuration={"fieldName": field.name},
        error_message="Email is required.",
        is_active=True,
    )
    table.validation_rules.append(rule)
    db_session.add(rule)
    db_session.flush()

    response = client.post(
        f"/constructed-data/{table.id}/batch-save",
        json={"rows": [{field.name: ""}], "validateOnly": True},
    )

    assert response.status_code == HTTPStatus.OK
    body = response.json()
    assert body["success"] is False
    assert body["rowsSaved"] == 0
    assert len(body["errors"]) == 1

    error = body["errors"][0]
    assert error["fieldName"] == field.name
    assert error["message"] == "Email is required."
    assert error["ruleName"] == "Email must be present"
    assert error["ruleType"] == "required"


def test_constructed_table_approval_crud_flow(client, db_session):
    execution_context_id = _create_execution_context(
        client,
        "Project Approval Constructed",
        "Release Approval Constructed",
        "Mock Approval Constructed",
        "Execution Approval Constructed",
    )
    constructed_table_id = client.post(
        "/constructed-tables",
        json={
            "execution_context_id": execution_context_id,
            "name": "Approvals Needed",
        },
    ).json()["id"]

    approver_id = _create_user(db_session, "Frank Lead", "frank.lead@example.com")

    create_resp = client.post(
        "/constructed-table-approvals",
        json={
            "constructed_table_id": constructed_table_id,
            "approver_id": approver_id,
            "role": "technical_lead",
            "decision": "rejected",
            "comments": "Need data dictionary",
        },
    )
    assert create_resp.status_code == HTTPStatus.CREATED
    approval_id = create_resp.json()["id"]

    list_resp = client.get("/constructed-table-approvals")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/constructed-table-approvals/{approval_id}",
        json={
            "decision": "approved",
            "comments": "Documentation provided",
        },
    )
    assert update_resp.status_code == HTTPStatus.OK
    body = update_resp.json()
    assert body["decision"] == "approved"
    assert body["comments"] == "Documentation provided"

    delete_resp = client.delete(f"/constructed-table-approvals/{approval_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def test_system_connection_crud_flow(client):
    system_id = client.post(
        "/systems",
        json={
            "name": "Integration Hub",
            "physical_name": "integration_hub",
            "description": "Central integration platform",
            "status": "active",
        },
    ).json()["id"]

    payload = {
        "system_id": system_id,
        "connection_type": "jdbc",
        "connection_string": "jdbc:postgresql://host:5432/db",
        "auth_method": "username_password",
        "notes": "Primary connection",
    }
    response = client.post("/system-connections", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    system_connection_id = response.json()["id"]

    list_resp = client.get("/system-connections")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/system-connections/{system_connection_id}",
        json={"auth_method": "oauth", "active": False},
    )
    assert update_resp.status_code == HTTPStatus.OK
    body = update_resp.json()
    assert body["auth_method"] == "oauth"
    assert body["active"] is False

    delete_resp = client.delete(f"/system-connections/{system_connection_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def test_system_connection_test_endpoint(client):
    response = client.post(
        "/system-connections/test",
        json={
            "connection_type": "jdbc",
            "connection_string": "jdbc:postgresql://invalid-host:5432/postgres",
        },
    )
    assert response.status_code == HTTPStatus.OK
    body = response.json()
    assert body["success"] is False
    assert "invalid-host" in body["message"]


def test_ingestion_job_crud_flow(client, monkeypatch):
    from app.routers import ingestion_job as ingestion_job_router

    run_state: dict[str, object] = {}

    def fake_ingest(self, system_connection, table, *, job, batch_size, replace):
        run_state["called"] = True
        run_state["batch_size"] = batch_size
        run_state["replace"] = replace
        run_state["system_connection_id"] = str(getattr(system_connection, "id", ""))
        assert job is not None
        job.started_at = job.started_at or datetime.utcnow()
        job.completed_at = datetime.utcnow()
        job.status = "completed"
        job.row_count = 42
        return job.row_count

    monkeypatch.setattr(
        ingestion_job_router.IngestionRunner,
        "ingest_table",
        fake_ingest,
    )

    execution_context_id = _create_execution_context(
        client,
        "Project Ingestion",
        "Release Ingestion",
        "Mock Cycle Ingestion",
        "Execution Context Ingestion",
    )

    system_id = client.post(
        "/systems",
        json={
            "name": "Source DB",
            "physical_name": "source_db",
            "description": "Source system",
            "status": "active",
        },
    ).json()["id"]
    table_id = client.post(
        "/tables",
        json={
            "system_id": system_id,
            "name": "Orders",
            "physical_name": "public.orders",
            "schema_name": "public",
            "status": "active",
        },
    ).json()["id"]

    connection_response = client.post(
        "/system-connections",
        json={
            "system_id": system_id,
            "connection_type": "jdbc",
            "connection_string": "jdbc:sqlserver://localhost:1433;databaseName=Source",
            "auth_method": "username_password",
            "active": True,
        },
    )
    assert connection_response.status_code == HTTPStatus.CREATED

    payload = {
        "execution_context_id": execution_context_id,
        "table_id": table_id,
        "status": "pending",
        "notes": "Initial ingestion",
    }
    response = client.post("/ingestion-jobs", json=payload)
    assert response.status_code == HTTPStatus.CREATED
    ingestion_job_id = response.json()["id"]

    run_resp = client.post(f"/ingestion-jobs/{ingestion_job_id}/run")
    assert run_resp.status_code == HTTPStatus.OK
    run_body = run_resp.json()
    assert run_body["status"] == "completed"
    assert run_body["row_count"] == 42
    assert run_state.get("called") is True
    assert run_state.get("batch_size") == 5_000
    assert run_state.get("replace") is False
    assert run_state.get("system_connection_id")

    list_resp = client.get("/ingestion-jobs")
    assert list_resp.status_code == HTTPStatus.OK
    assert len(list_resp.json()) == 1

    update_resp = client.put(
        f"/ingestion-jobs/{ingestion_job_id}",
        json={
            "status": "completed",
            "row_count": 12345,
            "notes": "Loaded successfully",
        },
    )
    assert update_resp.status_code == HTTPStatus.OK
    body = update_resp.json()
    assert body["status"] == "completed"
    assert body["row_count"] == 12345

    delete_resp = client.delete(f"/ingestion-jobs/{ingestion_job_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT
