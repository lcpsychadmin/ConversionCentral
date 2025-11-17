from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from app.services.data_quality_testgen import (
    AlertRecord,
    ProfileAnomaly,
    TestGenClient,
    TestGenClientError,
    TestResultRecord,
)
from app.services import data_quality_testgen
from app.services.databricks_sql import DatabricksConnectionParams


class DummyResult:
    def __init__(self, records: list[dict[str, Any]] | None = None) -> None:
        self._records = records or []

    def mappings(self) -> "DummyResult":
        return self

    def __iter__(self):
        return iter(self._records)


class DummyConnection:
    def __init__(self, executed: list[tuple[str, dict[str, Any]]], results_queue: list[list[dict[str, Any]]]) -> None:
        self._executed = executed
        self._results_queue = results_queue

    def __enter__(self) -> "DummyConnection":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        return None

    def execute(self, clause, params: dict[str, Any] | None = None):
        text = clause.text if hasattr(clause, "text") else str(clause)
        self._executed.append((text.strip(), params or {}))
        records = self._results_queue.pop(0) if self._results_queue else []
        return DummyResult(records)

    def close(self) -> None:
        return None


class DummyTransaction:
    def __init__(self, executed: list[tuple[str, dict[str, Any]]], results_queue: list[list[dict[str, Any]]]) -> None:
        self._connection = DummyConnection(executed, results_queue)

    def __enter__(self) -> DummyConnection:
        return self._connection

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        return None


class DummyEngine:
    def __init__(
        self,
        executed: list[tuple[str, dict[str, Any]]],
        should_fail: bool = False,
        results_sequence: list[list[dict[str, Any]]] | None = None,
    ) -> None:
        self._executed = executed
        self._should_fail = should_fail
        self._results_sequence = results_sequence or []

    def begin(self):
        if self._should_fail:
            raise RuntimeError("engine failure")
        return DummyTransaction(self._executed, self._results_sequence)

    def connect(self) -> DummyConnection:
        if self._should_fail:
            raise RuntimeError("engine failure")
        return DummyConnection(self._executed, self._results_sequence)

    def dispose(self) -> None:
        return None


@pytest.fixture()
def sample_params() -> DatabricksConnectionParams:
    return DatabricksConnectionParams(
        workspace_host="adb",
        http_path="/sql/path",
        access_token="token",
        catalog="sandbox",
        data_quality_schema="dq",
    )


def _install_dummy_engine(monkeypatch, executed, should_fail: bool = False, results_sequence=None) -> None:
    dummy = DummyEngine(executed, should_fail=should_fail, results_sequence=results_sequence)
    monkeypatch.setattr(data_quality_testgen, "create_engine", lambda url, **kwargs: dummy)


def test_start_profile_run_inserts_rows(monkeypatch, sample_params):
    executed: list[tuple[str, dict[str, Any]]] = []
    _install_dummy_engine(monkeypatch, executed)

    client = TestGenClient(sample_params, schema="dq")
    run_id = client.start_profile_run("group:abc", payload_path="dbfs:/runs/profile.json")
    client.close()

    assert run_id
    assert executed
    statement, params = executed[0]
    assert "INSERT INTO `sandbox`.`dq`.`dq_profiles`" in statement
    assert params["table_group_id"] == "group:abc"
    assert params["payload_path"] == "dbfs:/runs/profile.json"


def test_complete_profile_run_updates_and_records_anomalies(monkeypatch, sample_params):
    executed: list[tuple[str, dict[str, Any]]] = []
    _install_dummy_engine(monkeypatch, executed)

    client = TestGenClient(sample_params, schema="dq")
    anomalies = [
        ProfileAnomaly(
            table_name="orders",
            column_name="order_id",
            anomaly_type="null_density",
            severity="high",
            description="Null ratio exceeded threshold",
            detected_at=datetime(2025, 11, 16, tzinfo=timezone.utc),
        )
    ]

    client.complete_profile_run(
        "run-1",
        status="completed",
        row_count=123,
        anomalies=anomalies,
    )
    client.close()

    assert len(executed) == 2
    update_statement, update_params = executed[0]
    insert_statement, insert_params = executed[1]

    assert "UPDATE `sandbox`.`dq`.`dq_profiles`" in update_statement
    assert update_params["status"] == "completed"
    assert update_params["row_count"] == 123
    assert update_params["anomaly_count"] == 1

    assert "INSERT INTO `sandbox`.`dq`.`dq_profile_anomalies`" in insert_statement
    assert insert_params["profile_run_id"] == "run-1"
    assert insert_params["table_name"] == "orders"
    assert insert_params["column_name"] == "order_id"


def test_list_projects_returns_records(monkeypatch, sample_params):
    executed: list[tuple[str, dict[str, Any]]] = []
    results = [
        [
            {
                "project_key": "system:abc",
                "name": "System",
                "description": "desc",
                "sql_flavor": "databricks-sql",
            }
        ]
    ]
    _install_dummy_engine(monkeypatch, executed, results_sequence=results)

    client = TestGenClient(sample_params, schema="dq")
    rows = client.list_projects()
    client.close()

    assert rows[0]["project_key"] == "system:abc"
    statement, _ = executed[0]
    assert "SELECT project_key" in statement


def test_list_connections_filters_by_project(monkeypatch, sample_params):
    executed: list[tuple[str, dict[str, Any]]] = []
    results = [[{"connection_id": "conn-1"}]]
    _install_dummy_engine(monkeypatch, executed, results_sequence=results)

    client = TestGenClient(sample_params, schema="dq")
    rows = client.list_connections("system:abc")
    client.close()

    assert rows[0]["connection_id"] == "conn-1"
    statement, params = executed[0]
    assert "WHERE project_key = :project_key" in statement
    assert params["project_key"] == "system:abc"


def test_list_table_groups_filters_by_connection(monkeypatch, sample_params):
    executed: list[tuple[str, dict[str, Any]]] = []
    results = [[{"table_group_id": "group-1"}]]
    _install_dummy_engine(monkeypatch, executed, results_sequence=results)

    client = TestGenClient(sample_params, schema="dq")
    rows = client.list_table_groups("conn-1")
    client.close()

    assert rows[0]["table_group_id"] == "group-1"
    statement, params = executed[0]
    assert "WHERE connection_id = :connection_id" in statement
    assert params["connection_id"] == "conn-1"


def test_list_tables_filters_by_table_group(monkeypatch, sample_params):
    executed: list[tuple[str, dict[str, Any]]] = []
    results = [[{"table_id": "tbl-1"}]]
    _install_dummy_engine(monkeypatch, executed, results_sequence=results)

    client = TestGenClient(sample_params, schema="dq")
    rows = client.list_tables("group-1")
    client.close()

    assert rows[0]["table_id"] == "tbl-1"
    statement, params = executed[0]
    assert "WHERE table_group_id = :table_group_id" in statement
    assert params["table_group_id"] == "group-1"


def test_recent_alerts_excludes_acknowledged_by_default(monkeypatch, sample_params):
    executed: list[tuple[str, dict[str, Any]]] = []
    results = [[{"alert_id": "a1", "acknowledged": False}]]
    _install_dummy_engine(monkeypatch, executed, results_sequence=results)

    client = TestGenClient(sample_params, schema="dq")
    rows = client.recent_alerts(limit=10)
    client.close()

    assert rows[0]["alert_id"] == "a1"
    statement, params = executed[0]
    assert "WHERE acknowledged = false" in statement
    assert params["limit"] == 10


def test_recent_alerts_can_include_acknowledged(monkeypatch, sample_params):
    executed: list[tuple[str, dict[str, Any]]] = []
    results = [[{"alert_id": "a1", "acknowledged": True}]]
    _install_dummy_engine(monkeypatch, executed, results_sequence=results)

    client = TestGenClient(sample_params, schema="dq")
    rows = client.recent_alerts(limit=5, include_acknowledged=True)
    client.close()

    assert rows[0]["acknowledged"] is True
    statement, params = executed[0]
    assert "WHERE acknowledged = false" not in statement
    assert params["limit"] == 5


def test_acknowledge_and_delete_alert(monkeypatch, sample_params):
    executed: list[tuple[str, dict[str, Any]]] = []
    _install_dummy_engine(monkeypatch, executed)

    client = TestGenClient(sample_params, schema="dq")
    client.acknowledge_alert("alert-1", acknowledged_by="tester")
    client.delete_alert("alert-1")
    client.close()

    update_stmt, update_params = executed[0]
    delete_stmt, delete_params = executed[1]

    assert "UPDATE `sandbox`.`dq`.`dq_alerts`" in update_stmt
    assert update_params["acknowledged"] is True
    assert update_params["acknowledged_by"] == "tester"

    assert "DELETE FROM `sandbox`.`dq`.`dq_alerts`" in delete_stmt
    assert delete_params["alert_id"] == "alert-1"


def test_start_and_complete_test_run(monkeypatch, sample_params):
    executed: list[tuple[str, dict[str, Any]]] = []
    _install_dummy_engine(monkeypatch, executed)

    client = TestGenClient(sample_params, schema="dq")
    test_run_id = client.start_test_run(project_key="system:abc", test_suite_key="suite", total_tests=5)

    results = [
        TestResultRecord(
            test_id="test-1",
            table_name="orders",
            column_name="total",
            result_status="failed",
            expected_value="<=100",
            actual_value="150",
            message="Row count exceeded threshold",
            detected_at=datetime(2025, 11, 16, tzinfo=timezone.utc),
        )
    ]
    client.record_test_results(test_run_id, results)
    client.complete_test_run(test_run_id, status="completed", failed_tests=1, duration_ms=2300)
    client.close()

    assert len(executed) == 3
    insert_run_stmt, insert_run_params = executed[0]
    insert_result_stmt, insert_result_params = executed[1]
    update_stmt, update_params = executed[2]

    assert "INSERT INTO `sandbox`.`dq`.`dq_test_runs`" in insert_run_stmt
    assert insert_run_params["project_key"] == "system:abc"
    assert insert_run_params["total_tests"] == 5

    assert "INSERT INTO `sandbox`.`dq`.`dq_test_results`" in insert_result_stmt
    assert insert_result_params["test_id"] == "test-1"
    assert insert_result_params["result_status"] == "failed"

    assert "UPDATE `sandbox`.`dq`.`dq_test_runs`" in update_stmt
    assert update_params["failed_tests"] == 1
    assert update_params["duration_ms"] == 2300


def test_recent_runs_helpers_apply_limits(monkeypatch, sample_params):
    executed: list[tuple[str, dict[str, Any]]] = []
    results = [[{"profile_run_id": "p1"}], [{"test_run_id": "t1"}]]
    _install_dummy_engine(monkeypatch, executed, results_sequence=results)

    client = TestGenClient(sample_params, schema="dq")
    profiles = client.recent_profile_runs("group-1", limit=7)
    tests = client.recent_test_runs("system:abc", limit=3)
    client.close()

    assert profiles[0]["profile_run_id"] == "p1"
    assert tests[0]["test_run_id"] == "t1"

    profile_stmt, profile_params = executed[0]
    test_stmt, test_params = executed[1]

    assert "LIMIT :limit" in profile_stmt
    assert profile_params["limit"] == 7
    assert profile_params["table_group_id"] == "group-1"

    assert "LIMIT :limit" in test_stmt
    assert test_params["limit"] == 3
    assert test_params["project_key"] == "system:abc"


def test_create_alert(monkeypatch, sample_params):
    executed: list[tuple[str, dict[str, Any]]] = []
    _install_dummy_engine(monkeypatch, executed)

    client = TestGenClient(sample_params, schema="dq")
    alert_id = client.create_alert(
        AlertRecord(
            source_type="profile_run",
            source_ref="run-1",
            severity="critical",
            title="Null density spike",
            details="More than 10% nulls detected",
        )
    )
    client.close()

    assert alert_id
    statement, params = executed[0]
    assert "INSERT INTO `sandbox`.`dq`.`dq_alerts`" in statement
    assert params["source_ref"] == "run-1"
    assert params["severity"] == "critical"


def test_record_test_results_is_noop_when_empty(monkeypatch, sample_params):
    executed: list[tuple[str, dict[str, Any]]] = []
    _install_dummy_engine(monkeypatch, executed)

    client = TestGenClient(sample_params, schema="dq")
    client.record_test_results("run-1", [])
    client.close()

    assert executed == []


def test_execute_wraps_sqlalchemy_errors(monkeypatch, sample_params):
    executed: list[tuple[str, dict[str, Any]]] = []
    _install_dummy_engine(monkeypatch, executed, should_fail=True)

    client = TestGenClient(sample_params, schema="dq")
    with pytest.raises(TestGenClientError):
        client.start_profile_run("group:1")
    client.close()
