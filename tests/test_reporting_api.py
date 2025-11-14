from __future__ import annotations

from uuid import uuid4

from fastapi.testclient import TestClient


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


def test_report_crud_and_publish_flow(client: TestClient) -> None:
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

    create_payload = {
        "name": "Quarterly Sales",
        "description": "Initial draft",
        "definition": _sample_definition(),
    }

    create_response = client.post("/reporting/reports", json=create_payload)
    assert create_response.status_code == 201
    created = create_response.json()
    assert created["status"] == "draft"
    assert created["publishedAt"] is None
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
        },
    )
    assert publish_response.status_code == 200
    published = publish_response.json()
    assert published["status"] == "published"
    assert published["publishedAt"] is not None
    assert published["productTeamId"] == process_area_id
    assert published["dataObjectId"] == data_object_id

    published_listing = client.get("/reporting/reports", params={"status": "published"})
    assert published_listing.status_code == 200
    published_records = published_listing.json()
    assert any(item["id"] == report_id for item in published_records)

    delete_response = client.delete(f"/reporting/reports/{report_id}")
    assert delete_response.status_code == 204

    missing_response = client.get(f"/reporting/reports/{report_id}")
    assert missing_response.status_code == 404
