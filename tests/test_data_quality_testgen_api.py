from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from app.services.data_quality_keys import (
    build_connection_id,
    build_project_key,
    build_table_group_id,
    build_table_id,
)
from app.models.entities import (
    ConnectionTableSelection,
    DataDefinition,
    DataDefinitionTable,
    DataObject,
    ProcessArea,
    System,
    SystemConnection,
    Table,
)

from app.main import app
from app.routers import data_quality_testgen as dq_router


PROJECT_KEY = build_project_key("system-alpha", "object-beta")
CONNECTION_ID = build_connection_id("conn-1", "object-beta")
TABLE_GROUP_ID = build_table_group_id("conn-1", "object-beta")
TABLE_ID = build_table_id("tbl-1", "object-beta")


class StubTestGenClient:
    def __init__(self) -> None:
        self.closed = False
        self.calls: list[tuple] = []
        self.projects: list[dict[str, object]] = []
        self.connections: list[dict[str, object]] = []
        self.table_groups: list[dict[str, object]] = []
        self.table_group_overview: list[dict[str, object]] = []
        self.tables: list[dict[str, object]] = []
        self.profile_runs: list[dict[str, object]] = []
        self.profile_run_overview: list[dict[str, object]] = []
        self.test_runs: list[dict[str, object]] = []
        self.alerts: list[dict[str, object]] = []
        self.profile_run_anomaly_counts_map: dict[str, dict[str, int]] = {}
        self.profile_run_anomalies_map: dict[str, list[dict[str, object]]] = {}
        self.profile_run_id = "profile-run-1"
        self.test_run_id = "test-run-1"
        self.alert_id = "alert-1"
        self.last_alert_record = None
        self.last_profile_completion = None
        self.last_acknowledgement = None
        self.last_test_results = None
        self.last_test_completion = None
        self.tests: list[dict[str, object]] = []
        self.column_profiles: dict[tuple[str, str, str | None, str | None], dict[str, object]] = {}

    def list_projects(self):
        self.calls.append(("list_projects",))
        return self.projects

    def list_connections(self, project_key: str):
        self.calls.append(("list_connections", project_key))
        return self.connections

    def list_table_groups(self, connection_id: str):
        self.calls.append(("list_table_groups", connection_id))
        return self.table_groups

    def list_tables(self, table_group_id: str):
        self.calls.append(("list_tables", table_group_id))
        return self.tables

    def list_profile_runs_overview(self, *, table_group_id: str | None = None, limit: int = 50):
        self.calls.append(("list_profile_runs_overview", table_group_id, limit))
        rows = self.profile_run_overview
        if table_group_id:
            rows = [row for row in rows if row.get("table_group_id") == table_group_id]
        return rows[:limit]

    def list_table_groups_with_connections(self):
        self.calls.append(("list_table_groups_with_connections",))
        return self.table_group_overview

    def profile_run_anomaly_counts(self, profile_run_ids):
        self.calls.append(("profile_run_anomaly_counts", tuple(profile_run_ids)))
        return {
            run_id: self.profile_run_anomaly_counts_map.get(run_id, {})
            for run_id in profile_run_ids
        }

    def list_suite_tests(self, test_suite_key: str):
        self.calls.append(("list_suite_tests", test_suite_key))
        return [test for test in self.tests if test.get("test_suite_key") == test_suite_key]

    def get_test(self, test_id: str):
        self.calls.append(("get_test", test_id))
        for test in self.tests:
            if test.get("test_id") == test_id:
                return test
        return None

    def create_test(self, *, table_group_id: str, test_suite_key: str, name: str, rule_type: str, definition):
        self.calls.append(("create_test", table_group_id, test_suite_key, name, rule_type, definition))
        test_id = f"test-{len(self.tests) + 1}"
        record = {
            "test_id": test_id,
            "table_group_id": table_group_id,
            "test_suite_key": test_suite_key,
            "name": name,
            "rule_type": rule_type,
            "definition": definition,
            "created_at": None,
            "updated_at": None,
        }
        self.tests.append(record)
        return test_id

    def update_test(self, test_id: str, **updates):
        self.calls.append(("update_test", test_id, updates))
        for test in self.tests:
            if test.get("test_id") == test_id:
                test.update(updates)
                return

    def delete_test(self, test_id: str):  # noqa: D401 - simple pass-through
        self.calls.append(("delete_test", test_id))
        self.tests = [test for test in self.tests if test.get("test_id") != test_id]

    def recent_profile_runs(self, table_group_id: str, *, limit: int):
        self.calls.append(("recent_profile_runs", table_group_id, limit))
        return self.profile_runs

    def recent_test_runs(self, project_key: str, *, limit: int):
        self.calls.append(("recent_test_runs", project_key, limit))
        return self.test_runs

    def recent_alerts(self, *, limit: int, include_acknowledged: bool):
        self.calls.append(("recent_alerts", limit, include_acknowledged))
        return self.alerts

    def column_profile(
        self,
        table_group_id: str,
        *,
        column_name: str,
        table_name: str | None = None,
        physical_name: str | None = None,
    ):
        self.calls.append(("column_profile", table_group_id, column_name, table_name, physical_name))
        key = (table_group_id, column_name, table_name, physical_name)
        return self.column_profiles.get(key)

    def create_alert(self, record):
        self.calls.append(("create_alert", record))
        self.last_alert_record = record
        return self.alert_id

    def acknowledge_alert(self, alert_id: str, *, acknowledged: bool = True, acknowledged_by=None, acknowledged_at=None):
        self.calls.append(("acknowledge_alert", alert_id, acknowledged, acknowledged_by, acknowledged_at))
        self.last_acknowledgement = {
            "alert_id": alert_id,
            "acknowledged": acknowledged,
            "acknowledged_by": acknowledged_by,
            "acknowledged_at": acknowledged_at,
        }

    def delete_alert(self, alert_id: str):
        self.calls.append(("delete_alert", alert_id))

    def start_profile_run(self, table_group_id: str, *, status: str, started_at=None):
        self.calls.append(("start_profile_run", table_group_id, status, started_at))
        return self.profile_run_id

    def complete_profile_run(self, profile_run_id: str, *, status: str, row_count=None, anomaly_count=None, anomalies=()):
        self.calls.append(("complete_profile_run", profile_run_id, status, row_count, anomaly_count, anomalies))
        self.last_profile_completion = {
            "profile_run_id": profile_run_id,
            "status": status,
            "row_count": row_count,
            "anomaly_count": anomaly_count,
            "anomalies": list(anomalies),
        }
        self.profile_run_anomalies_map[profile_run_id] = list(anomalies)

    def start_test_run(self, *, project_key: str, test_suite_key=None, total_tests=None, trigger_source=None, status: str, started_at=None):
        self.calls.append(("start_test_run", project_key, test_suite_key, total_tests, trigger_source, status, started_at))
        return self.test_run_id

    def record_test_results(self, test_run_id: str, results):
        self.calls.append(("record_test_results", test_run_id, tuple(results)))
        self.last_test_results = {"test_run_id": test_run_id, "results": list(results)}

    def complete_test_run(self, test_run_id: str, *, status: str, failed_tests=None, duration_ms=None):
        self.calls.append(("complete_test_run", test_run_id, status, failed_tests, duration_ms))
        self.last_test_completion = {
            "test_run_id": test_run_id,
            "status": status,
            "failed_tests": failed_tests,
            "duration_ms": duration_ms,
        }

    def list_profile_run_anomalies(self, profile_run_id: str):
        self.calls.append(("list_profile_run_anomalies", profile_run_id))
        return self.profile_run_anomalies_map.get(profile_run_id, [])

    def close(self) -> None:
        self.closed = True


@contextmanager
def override_client(stub: StubTestGenClient):
    def _override():  # pragma: no cover - FastAPI handles execution
        try:
            yield stub
        finally:
            stub.close()

    app.dependency_overrides[dq_router.get_testgen_client] = _override
    try:
        yield
    finally:
        app.dependency_overrides.pop(dq_router.get_testgen_client, None)


def test_list_projects_endpoint(client):
    stub = StubTestGenClient()
    stub.projects = [
        {
            "project_key": PROJECT_KEY,
            "name": "Alpha",
            "description": None,
            "sql_flavor": None,
        }
    ]

    with override_client(stub):
        response = client.get("/data-quality/testgen/projects")

    assert response.status_code == 200
    assert response.json() == stub.projects
    assert ("list_projects",) in stub.calls
    assert stub.closed is True


def test_list_connections_and_tables(client):
    stub = StubTestGenClient()
    stub.connections = [
        {
            "connection_id": CONNECTION_ID,
            "project_key": PROJECT_KEY,
            "system_id": None,
            "name": "Primary",
            "catalog": None,
            "schema_name": None,
            "http_path": None,
            "managed_credentials_ref": None,
            "is_active": None,
        }
    ]
    stub.table_groups = [
        {
            "table_group_id": TABLE_GROUP_ID,
            "connection_id": CONNECTION_ID,
            "name": "Core",
            "description": None,
            "profiling_include_mask": None,
            "profiling_exclude_mask": None,
        }
    ]
    stub.tables = [
        {
            "table_id": TABLE_ID,
            "table_group_id": TABLE_GROUP_ID,
            "schema_name": None,
            "table_name": "orders",
            "source_table_id": None,
        }
    ]

    with override_client(stub):
        resp_connections = client.get(f"/data-quality/testgen/projects/{PROJECT_KEY}/connections")
        resp_groups = client.get(f"/data-quality/testgen/connections/{CONNECTION_ID}/table-groups")
        resp_tables = client.get(f"/data-quality/testgen/table-groups/{TABLE_GROUP_ID}/tables")

    assert resp_connections.status_code == 200
    assert resp_connections.json() == stub.connections
    assert resp_groups.json() == stub.table_groups
    assert resp_tables.json() == stub.tables
    assert ("list_connections", PROJECT_KEY) in stub.calls
    assert ("list_table_groups", CONNECTION_ID) in stub.calls
    assert ("list_tables", TABLE_GROUP_ID) in stub.calls


def test_recent_queries_apply_limits(client):
    stub = StubTestGenClient()
    stub.profile_runs = [{"profile_run_id": "p1", "table_group_id": TABLE_GROUP_ID, "status": "completed"}]
    stub.test_runs = [{"test_run_id": "t1", "project_key": PROJECT_KEY, "status": "completed"}]

    stub.profile_run_overview = [
        {
            "profile_run_id": "p1",
            "table_group_id": TABLE_GROUP_ID,
            "status": "completed",
            "started_at": None,
            "completed_at": None,
            "row_count": None,
            "anomaly_count": None,
            "table_group_name": "default",
            "connection_id": CONNECTION_ID,
            "connection_name": "Primary",
            "catalog": None,
            "schema_name": None,
        }
    ]
    stub.profile_run_anomaly_counts_map = {"p1": {"definite": 2}}
    stub.table_group_overview = [
        {
            "table_group_id": TABLE_GROUP_ID,
            "table_group_name": "default",
            "connection_id": CONNECTION_ID,
            "connection_name": "Primary",
            "catalog": None,
            "schema_name": None,
        }
    ]

    with override_client(stub):
        profile_resp = client.get(f"/data-quality/testgen/table-groups/{TABLE_GROUP_ID}/profile-runs?limit=5")
        test_resp = client.get(f"/data-quality/testgen/projects/{PROJECT_KEY}/test-runs?limit=3")
        overview_resp = client.get("/data-quality/profile-runs")

    assert profile_resp.status_code == 200
    assert test_resp.status_code == 200
    assert ("recent_profile_runs", TABLE_GROUP_ID, 5) in stub.calls
    assert ("recent_test_runs", PROJECT_KEY, 3) in stub.calls
    assert overview_resp.status_code == 200
    payload = overview_resp.json()
    assert payload["runs"][0]["profileRunId"] == "p1"
    assert payload["runs"][0]["anomaliesBySeverity"] == {"definite": 2}
    assert ("list_profile_runs_overview", None, 50) in stub.calls
    assert ("profile_run_anomaly_counts", ("p1",)) in stub.calls
    assert ("list_table_groups_with_connections",) in stub.calls


def test_column_profile_endpoint(client):
    stub = StubTestGenClient()
    stub.column_profiles[(TABLE_GROUP_ID, "total", "orders", None)] = {
        "table_group_id": TABLE_GROUP_ID,
        "profile_run_id": "run-1",
        "status": "completed",
        "column_name": "total",
        "metrics": [],
        "top_values": [],
        "histogram": [],
        "anomalies": [],
    }

    with override_client(stub):
        response = client.get(
            f"/data-quality/testgen/table-groups/{TABLE_GROUP_ID}/column-profile",
            params={"columnName": "total", "tableName": "orders"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["column_name"] == "total"
    assert ("column_profile", TABLE_GROUP_ID, "total", "orders", None) in stub.calls


def test_column_profile_not_found(client):
    stub = StubTestGenClient()

    with override_client(stub):
        response = client.get(
            f"/data-quality/testgen/table-groups/{TABLE_GROUP_ID}/column-profile",
            params={"columnName": "missing"},
        )

    assert response.status_code == 404


def test_column_profile_requires_column_name(client):
    stub = StubTestGenClient()

    with override_client(stub):
        response = client.get(
            f"/data-quality/testgen/table-groups/{TABLE_GROUP_ID}/column-profile",
            params={"columnName": ""},
        )

    assert response.status_code == 400


def test_alert_lifecycle_endpoints(client):
    stub = StubTestGenClient()
    stub.alerts = [
        {
            "alert_id": "a1",
            "source_type": "profile_run",
            "source_ref": "run",
            "severity": "high",
            "title": "Issue",
            "details": "desc",
            "acknowledged": False,
            "acknowledged_by": None,
            "acknowledged_at": None,
            "created_at": None,
        }
    ]

    with override_client(stub):
        list_resp = client.get("/data-quality/testgen/alerts?limit=5&include_acknowledged=true")
        create_resp = client.post(
            "/data-quality/testgen/alerts",
            json={
                "source_type": "profile_run",
                "source_ref": "run-1",
                "severity": "critical",
                "title": "Anomaly",
                "details": "Null spike",
            },
        )
        ack_resp = client.post("/data-quality/testgen/alerts/alert-1/acknowledge", json={})
        delete_resp = client.delete("/data-quality/testgen/alerts/alert-1")

    assert list_resp.status_code == 200
    assert list_resp.json() == stub.alerts
    assert ("recent_alerts", 5, True) in stub.calls
    assert create_resp.status_code == 201
    assert create_resp.json() == {"alert_id": "alert-1"}
    assert stub.last_alert_record is not None
    assert ack_resp.status_code == 200
    assert stub.last_acknowledgement == {
        "alert_id": "alert-1",
        "acknowledged": True,
        "acknowledged_by": None,
        "acknowledged_at": None,
    }
    assert delete_resp.status_code == 204
    assert ("delete_alert", "alert-1") in stub.calls


def test_profile_run_flow(client):
    stub = StubTestGenClient()

    with override_client(stub):
        start_resp = client.post(
            "/data-quality/testgen/profile-runs",
            json={
                "table_group_id": TABLE_GROUP_ID,
            },
        )
        complete_resp = client.post(
            "/data-quality/testgen/profile-runs/profile-run-1/complete",
            json={
                "status": "completed",
                "row_count": 120,
                "anomalies": [
                    {
                        "table_name": "orders",
                        "anomaly_type": "null_density",
                        "severity": "high",
                        "description": "Nulls increased",
                        "detected_at": datetime(2025, 11, 16, tzinfo=timezone.utc).isoformat(),
                    }
                ],
            },
        )

    assert start_resp.status_code == 201
    assert start_resp.json() == {"profile_run_id": "profile-run-1"}
    assert complete_resp.status_code == 204
    completion = stub.last_profile_completion
    assert completion is not None
    assert completion["profile_run_id"] == "profile-run-1"
    assert completion["row_count"] == 120
    assert len(completion["anomalies"]) == 1
    anomaly = completion["anomalies"][0]
    assert anomaly.table_name == "orders"
    assert anomaly.anomaly_type == "null_density"


def test_profile_run_anomalies_endpoint(client):
    stub = StubTestGenClient()
    stub.profile_run_anomalies_map = {
        "profile-run-99": [
            {
                "table_name": "orders",
                "column_name": "status",
                "anomaly_type": "value_drift",
                "severity": "likely",
                "description": "Status mix changed",
                "detected_at": datetime(2025, 11, 17, tzinfo=timezone.utc).isoformat(),
            }
        ]
    }

    with override_client(stub):
        response = client.get("/data-quality/profile-runs/profile-run-99/anomalies")

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["column_name"] == "status"
    assert ("list_profile_run_anomalies", "profile-run-99") in stub.calls


def test_test_run_flow(client):
    stub = StubTestGenClient()

    with override_client(stub):
        start_resp = client.post(
            "/data-quality/testgen/test-runs",
            json={
                "project_key": PROJECT_KEY,
                "total_tests": 5,
            },
        )
        results_resp = client.post(
            "/data-quality/testgen/test-runs/test-run-1/results",
            json={
                "results": [
                    {
                        "test_id": "test-1",
                        "result_status": "failed",
                        "message": "Threshold exceeded",
                    }
                ]
            },
        )
        complete_resp = client.post(
            "/data-quality/testgen/test-runs/test-run-1/complete",
            json={"status": "completed", "failed_tests": 1, "duration_ms": 1200},
        )

    assert start_resp.status_code == 201
    assert start_resp.json() == {"test_run_id": "test-run-1"}
    assert results_resp.status_code == 202
    test_results = stub.last_test_results
    assert test_results is not None
    assert test_results["test_run_id"] == "test-run-1"
    assert len(test_results["results"]) == 1
    assert test_results["results"][0].test_id == "test-1"
    assert complete_resp.status_code == 204
    completion = stub.last_test_completion
    assert completion == {
        "test_run_id": "test-run-1",
        "status": "completed",
        "failed_tests": 1,
        "duration_ms": 1200,
    }
    assert stub.closed is True


def test_list_suite_tests_endpoint(client):
    stub = StubTestGenClient()
    suite_key = "suite-1"
    definition_payload = {
        "dataDefinitionTableId": str(uuid.uuid4()),
        "tableGroupId": TABLE_GROUP_ID,
        "tableId": TABLE_ID,
        "schemaName": "analytics",
        "tableName": "orders",
        "physicalName": "raw.orders",
        "columnName": "order_id",
        "parameters": {"mode": "not_null"},
    }
    stub.tests = [
        {
            "test_id": "test-1",
            "table_group_id": TABLE_GROUP_ID,
            "test_suite_key": suite_key,
            "name": "Order ID Required",
            "rule_type": "not_null",
            "definition": json.dumps(definition_payload),
            "created_at": None,
            "updated_at": None,
        }
    ]

    with override_client(stub):
        response = client.get(f"/data-quality/testgen/test-suites/{suite_key}/tests")

    assert response.status_code == 200
    assert response.json() == [
        {
            "testId": "test-1",
            "testSuiteKey": suite_key,
            "tableGroupId": TABLE_GROUP_ID,
            "tableId": TABLE_ID,
            "dataDefinitionTableId": definition_payload["dataDefinitionTableId"],
            "schemaName": "analytics",
            "tableName": "orders",
            "physicalName": "raw.orders",
            "columnName": "order_id",
            "ruleType": "not_null",
            "name": "Order ID Required",
            "definition": definition_payload,
            "createdAt": None,
            "updatedAt": None,
        }
    ]
    assert ("list_suite_tests", suite_key) in stub.calls


def _seed_data_quality_context(db_session):
    product_team = ProcessArea(name="Product A", description="Team", status="active")
    system = System(name="Billing", physical_name="billing", description="Billing system")
    data_object = DataObject(name="Invoice", description="Invoices", status="active", process_area=product_team)
    definition = DataDefinition(data_object=data_object, system=system, description="Primary definition")
    table = Table(
        system=system,
        name="invoices",
        physical_name="raw.invoices",
        schema_name="analytics",
        description="Invoices table",
        table_type="base",
    )
    definition_table = DataDefinitionTable(
        data_definition=definition,
        table=table,
        alias="Invoices",
        description="Invoices table",
        load_order=1,
    )

    connection = SystemConnection(
        system=system,
        connection_type="jdbc",
        connection_string="jdbc://example",
        auth_method="username_password",
        active=True,
        ingestion_enabled=True,
    )
    selection = ConnectionTableSelection(
        system_connection=connection,
        schema_name="analytics",
        table_name="raw.invoices",
    )

    db_session.add_all([
        product_team,
        system,
        data_object,
        definition,
        table,
        definition_table,
        connection,
        selection,
    ])
    db_session.commit()

    return {
        "data_definition_table_id": definition_table.id,
        "connection": connection,
        "selection": selection,
        "data_object_id": data_object.id,
    }


def test_create_suite_test_endpoint(client, db_session):
    context = _seed_data_quality_context(db_session)
    stub = StubTestGenClient()
    suite_key = "suite-create"

    payload = {
        "name": "Invoices must have IDs",
        "ruleType": "not_null",
        "dataDefinitionTableId": str(context["data_definition_table_id"]),
        "columnName": "invoice_id",
        "definition": {"mode": "not_null"},
    }

    with override_client(stub):
        response = client.post(f"/data-quality/testgen/test-suites/{suite_key}/tests", json=payload)

    assert response.status_code == 201
    body = response.json()
    expected_group = build_table_group_id(context["connection"].id, context["data_object_id"])
    assert body["tableGroupId"] == expected_group
    assert body["columnName"] == "invoice_id"
    assert body["definition"]["parameters"] == {"mode": "not_null"}
    create_call = next(call for call in stub.calls if call[0] == "create_test")
    _, table_group_id, called_suite_key, _, _, definition_payload = create_call
    assert table_group_id == expected_group
    assert called_suite_key == suite_key
    assert definition_payload["parameters"] == {"mode": "not_null"}


def test_update_suite_test_endpoint(client, db_session):
    context = _seed_data_quality_context(db_session)
    stub = StubTestGenClient()
    suite_key = "suite-update"
    initial_definition = {
        "dataDefinitionTableId": str(context["data_definition_table_id"]),
        "tableGroupId": build_table_group_id(context["connection"].id, context["data_object_id"]),
        "tableId": build_table_id(context["selection"].id, context["data_object_id"]),
        "schemaName": "analytics",
        "tableName": "invoices",
        "physicalName": "raw.invoices",
        "columnName": "invoice_id",
        "parameters": {"mode": "not_null"},
    }
    stub.tests = [
        {
            "test_id": "test-1",
            "table_group_id": initial_definition["tableGroupId"],
            "test_suite_key": suite_key,
            "name": "Invoices must have IDs",
            "rule_type": "not_null",
            "definition": initial_definition,
            "created_at": None,
            "updated_at": None,
        }
    ]

    payload = {
        "columnName": "invoice_number",
        "definition": {"mode": "not_null", "threshold": 0.05},
    }

    with override_client(stub):
        response = client.put("/data-quality/testgen/tests/test-1", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["columnName"] == "invoice_number"
    assert body["definition"]["parameters"] == {"mode": "not_null", "threshold": 0.05}
    update_call = next(call for call in stub.calls if call[0] == "update_test")
    assert update_call[1] == "test-1"
    assert update_call[2]["definition"]["parameters"] == {"mode": "not_null", "threshold": 0.05}


def test_delete_suite_test_endpoint(client):
    stub = StubTestGenClient()
    stub.tests = [
        {
            "test_id": "test-1",
            "table_group_id": TABLE_GROUP_ID,
            "test_suite_key": "suite-1",
            "name": "Invoices must have IDs",
            "rule_type": "not_null",
            "definition": {},
        }
    ]

    with override_client(stub):
        response = client.delete("/data-quality/testgen/tests/test-1")

    assert response.status_code == 204
    assert ("delete_test", "test-1") in stub.calls
    assert not any(test.get("test_id") == "test-1" for test in stub.tests)


def test_missing_schema_returns_service_unavailable(monkeypatch, client):
    class DummyParams:
        data_quality_schema = ""

    monkeypatch.setattr(dq_router, "get_ingestion_connection_params", lambda: DummyParams())
    response = client.get("/data-quality/testgen/projects")
    assert response.status_code == 503