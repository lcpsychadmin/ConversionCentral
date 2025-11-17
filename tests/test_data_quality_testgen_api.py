from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone

from app.main import app
from app.routers import data_quality_testgen as dq_router


class StubTestGenClient:
    def __init__(self) -> None:
        self.closed = False
        self.calls: list[tuple] = []
        self.projects: list[dict[str, object]] = []
        self.connections: list[dict[str, object]] = []
        self.table_groups: list[dict[str, object]] = []
        self.tables: list[dict[str, object]] = []
        self.profile_runs: list[dict[str, object]] = []
        self.test_runs: list[dict[str, object]] = []
        self.alerts: list[dict[str, object]] = []
        self.profile_run_id = "profile-run-1"
        self.test_run_id = "test-run-1"
        self.alert_id = "alert-1"
        self.last_alert_record = None
        self.last_profile_completion = None
        self.last_acknowledgement = None
        self.last_test_results = None
        self.last_test_completion = None

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

    def recent_profile_runs(self, table_group_id: str, *, limit: int):
        self.calls.append(("recent_profile_runs", table_group_id, limit))
        return self.profile_runs

    def recent_test_runs(self, project_key: str, *, limit: int):
        self.calls.append(("recent_test_runs", project_key, limit))
        return self.test_runs

    def recent_alerts(self, *, limit: int, include_acknowledged: bool):
        self.calls.append(("recent_alerts", limit, include_acknowledged))
        return self.alerts

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

    def start_profile_run(self, table_group_id: str, *, status: str, started_at=None, payload_path=None):
        self.calls.append(("start_profile_run", table_group_id, status, started_at, payload_path))
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
            "project_key": "system:alpha",
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
            "connection_id": "conn-1",
            "project_key": "system:alpha",
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
            "table_group_id": "group-1",
            "connection_id": "conn-1",
            "name": "Core",
            "description": None,
            "profiling_include_mask": None,
            "profiling_exclude_mask": None,
        }
    ]
    stub.tables = [
        {
            "table_id": "tbl-1",
            "table_group_id": "group-1",
            "schema_name": None,
            "table_name": "orders",
            "source_table_id": None,
        }
    ]

    with override_client(stub):
        resp_connections = client.get("/data-quality/testgen/projects/system:alpha/connections")
        resp_groups = client.get("/data-quality/testgen/connections/conn-1/table-groups")
        resp_tables = client.get("/data-quality/testgen/table-groups/group-1/tables")

    assert resp_connections.status_code == 200
    assert resp_connections.json() == stub.connections
    assert resp_groups.json() == stub.table_groups
    assert resp_tables.json() == stub.tables
    assert ("list_connections", "system:alpha") in stub.calls
    assert ("list_table_groups", "conn-1") in stub.calls
    assert ("list_tables", "group-1") in stub.calls


def test_recent_queries_apply_limits(client):
    stub = StubTestGenClient()
    stub.profile_runs = [{"profile_run_id": "p1", "table_group_id": "group", "status": "completed"}]
    stub.test_runs = [{"test_run_id": "t1", "project_key": "system", "status": "completed"}]

    with override_client(stub):
        profile_resp = client.get("/data-quality/testgen/table-groups/group/profile-runs?limit=5")
        test_resp = client.get("/data-quality/testgen/projects/system/test-runs?limit=3")

    assert profile_resp.status_code == 200
    assert test_resp.status_code == 200
    assert ("recent_profile_runs", "group", 5) in stub.calls
    assert ("recent_test_runs", "system", 3) in stub.calls


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
                "table_group_id": "group-1",
                "payload_path": "dbfs:/runs/profile.json",
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


def test_test_run_flow(client):
    stub = StubTestGenClient()

    with override_client(stub):
        start_resp = client.post(
            "/data-quality/testgen/test-runs",
            json={
                "project_key": "system:alpha",
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


def test_missing_schema_returns_service_unavailable(monkeypatch, client):
    class DummyParams:
        data_quality_schema = ""

    monkeypatch.setattr(dq_router, "get_ingestion_connection_params", lambda: DummyParams())
    response = client.get("/data-quality/testgen/projects")
    assert response.status_code == 503