from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from sqlalchemy.exc import SQLAlchemyError

from app.services.data_quality_keys import (
    build_connection_id,
    build_project_key,
    build_table_group_id,
    build_table_id,
)
from app.services.data_quality_testgen import (
    AlertRecord,
    ProfileAnomaly,
    TestGenClient,
    TestGenClientError,
    TestResultRecord,
)
from app.services import data_quality_testgen
from app.services.databricks_sql import DatabricksConnectionParams


PROJECT_KEY = build_project_key("system-alpha", "object-beta")
CONNECTION_ID = build_connection_id("conn-alpha", "object-beta")
TABLE_GROUP_ID = build_table_group_id("conn-alpha", "object-beta")
TABLE_ID = build_table_id("tbl-alpha", "object-beta")


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
        self._results_sequence = list(results_sequence or [])

    def connect(self) -> DummyConnection:
        if self._should_fail:
            raise SQLAlchemyError("simulated failure")
        return DummyConnection(self._executed, self._results_sequence)

    def begin(self) -> DummyTransaction:
        if self._should_fail:
            raise SQLAlchemyError("simulated failure")
        return DummyTransaction(self._executed, self._results_sequence)

    def dispose(self) -> None:
        return None


def _install_dummy_engine(
    monkeypatch,
    executed: list[tuple[str, dict[str, Any]]],
    *,
    results_sequence: list[list[dict[str, Any]]] | None = None,
    should_fail: bool = False,
):
    engine = DummyEngine(executed, should_fail=should_fail, results_sequence=results_sequence or [])

    def fake_create_engine(*_, **__):
        return engine

    monkeypatch.setattr(data_quality_testgen, "create_engine", fake_create_engine)
    return engine


@pytest.fixture(name="sample_params")
def fixture_sample_params():
    return DatabricksConnectionParams(
        workspace_host="adb-unit-test",
        http_path="/sql/1.0/warehouses/test",
        access_token="token",
        catalog="sandbox",
        schema_name="default",
    )



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
                "project_key": PROJECT_KEY,
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

    assert rows[0]["project_key"] == PROJECT_KEY
    statement, _ = executed[0]
    assert "SELECT project_key" in statement


def test_list_connections_filters_by_project(monkeypatch, sample_params):
    executed: list[tuple[str, dict[str, Any]]] = []
    results = [[{"connection_id": CONNECTION_ID}]]
    _install_dummy_engine(monkeypatch, executed, results_sequence=results)

    client = TestGenClient(sample_params, schema="dq")
    rows = client.list_connections(PROJECT_KEY)
    client.close()

    assert rows[0]["connection_id"] == CONNECTION_ID
    statement, params = executed[0]
    assert "WHERE project_key = :project_key" in statement
    assert params["project_key"] == PROJECT_KEY


def test_list_table_groups_filters_by_connection(monkeypatch, sample_params):
    executed: list[tuple[str, dict[str, Any]]] = []
    results = [[{"table_group_id": TABLE_GROUP_ID}]]
    _install_dummy_engine(monkeypatch, executed, results_sequence=results)

    client = TestGenClient(sample_params, schema="dq")
    rows = client.list_table_groups(CONNECTION_ID)
    client.close()

    assert rows[0]["table_group_id"] == TABLE_GROUP_ID
    statement, params = executed[0]
    assert "WHERE connection_id = :connection_id" in statement
    assert params["connection_id"] == CONNECTION_ID


def test_list_tables_filters_by_table_group(monkeypatch, sample_params):
    executed: list[tuple[str, dict[str, Any]]] = []
    results = [[{"table_id": TABLE_ID}]]
    _install_dummy_engine(monkeypatch, executed, results_sequence=results)

    client = TestGenClient(sample_params, schema="dq")
    rows = client.list_tables(TABLE_GROUP_ID)
    client.close()

    assert rows[0]["table_id"] == TABLE_ID
    statement, params = executed[0]
    assert "WHERE table_group_id = :table_group_id" in statement
    assert params["table_group_id"] == TABLE_GROUP_ID


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
    test_run_id = client.start_test_run(project_key=PROJECT_KEY, test_suite_key="suite", total_tests=5)

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
    assert insert_run_params["project_key"] == PROJECT_KEY
    assert insert_run_params["total_tests"] == 5

    assert "INSERT INTO `sandbox`.`dq`.`dq_test_results`" in insert_result_stmt
    assert insert_result_params["test_id"] == "test-1"
    assert insert_result_params["result_status"] == "failed"

    assert "UPDATE `sandbox`.`dq`.`dq_test_runs`" in update_stmt
    assert update_params["failed_tests"] == 1
    assert update_params["duration_ms"] == 2300


def test_recent_runs_helpers_apply_limits(monkeypatch, sample_params):
    executed: list[tuple[str, dict[str, Any]]] = []
    results = [[], [], [], [], [{"profile_run_id": "p1"}], [{"test_run_id": "t1"}]]
    _install_dummy_engine(monkeypatch, executed, results_sequence=results)

    client = TestGenClient(sample_params, schema="dq")
    profiles = client.recent_profile_runs(TABLE_GROUP_ID, limit=7)
    tests = client.recent_test_runs(PROJECT_KEY, limit=3)
    client.close()

    assert profiles[0]["profile_run_id"] == "p1"
    assert tests[0]["test_run_id"] == "t1"

    profile_stmt, profile_params = next(
        ((stmt, params) for stmt, params in executed if "SELECT profile_run_id" in stmt)
    )
    test_stmt, test_params = next(
        ((stmt, params) for stmt, params in executed if "SELECT test_run_id" in stmt)
    )

    assert "LIMIT :limit" in profile_stmt
    assert profile_params["limit"] == 7
    assert profile_params["table_group_id"] == TABLE_GROUP_ID

    assert "LIMIT :limit" in test_stmt
    assert test_params["limit"] == 3
    assert test_params["project_key"] == PROJECT_KEY


def test_column_profile_prefers_detail_tables_when_available(monkeypatch, sample_params):
    executed: list[tuple[str, dict[str, Any]]] = []
    profile_row = {
        "profile_run_id": "run-2",
        "table_group_id": TABLE_GROUP_ID,
        "status": "completed",
        "started_at": datetime(2025, 11, 16, tzinfo=timezone.utc),
        "completed_at": datetime(2025, 11, 16, tzinfo=timezone.utc),
        "row_count": 250,
        "anomaly_count": 0,
    }
    column_row = {
        "profile_run_id": "run-2",
        "table_name": "analytics.orders",
        "schema_name": "analytics",
        "column_name": "total",
        "data_type": "DECIMAL(10,2)",
        "row_count": 250,
        "null_count": 10,
        "distinct_count": 15,
        "min_value": "1",
        "max_value": "999",
        "avg_value": 42.5,
        "median_value": 21.0,
        "p95_value": 200.0,
        "generated_at": datetime(2025, 11, 16, tzinfo=timezone.utc),
    }
    value_rows = [
        {
            "profile_run_id": "run-2",
            "column_name": "total",
            "value": "alpha",
            "frequency": 5,
            "relative_freq": 0.25,
            "rank": 1,
        },
        {
            "profile_run_id": "run-2",
            "column_name": "total",
            "bucket_label": "0-10",
            "bucket_lower_bound": 0.0,
            "bucket_upper_bound": 10.0,
            "frequency": 3,
        },
    ]
    anomaly_row = {
        "table_name": "analytics.orders",
        "column_name": "total",
        "anomaly_type": "null_density",
        "severity": "high",
        "description": "Null ratio increased",
        "detected_at": datetime(2025, 11, 16, tzinfo=timezone.utc),
    }

    results_sequence = [
        [profile_row],  # latest run
        [],
        [],
        [],
        [],
        [column_row],
        value_rows,
        [anomaly_row],
    ]
    _install_dummy_engine(monkeypatch, executed, results_sequence=results_sequence)

    class StubSettings:
        testgen_profile_table_reads_enabled = True

    client = TestGenClient(sample_params, schema="dq", settings=StubSettings())
    profile = client.column_profile(TABLE_GROUP_ID, column_name="total", table_name="analytics.orders")
    client.close()

    assert profile
    assert profile["table_name"] == "analytics.orders"
    metrics = {entry["key"]: entry for entry in profile["metrics"]}
    assert metrics["min_value"]["value"] == "1"
    assert profile["top_values"][0]["percentage"] == 25.0
    assert profile["histogram"][0]["label"] == "0-10"

def test_export_profiling_payload_builds_tables(monkeypatch, sample_params):
    client = TestGenClient(sample_params, schema="dq")
    summary_row = {
        "profile_run_id": "run-1",
        "table_group_id": TABLE_GROUP_ID,
        "status": "completed",
        "started_at": datetime(2025, 11, 16, tzinfo=timezone.utc),
        "completed_at": datetime(2025, 11, 16, 1, tzinfo=timezone.utc),
        "row_count": 250,
        "anomaly_count": 2,
        "databricks_run_id": "db-7",
    }
    table_catalog = {
        "by_id": {
            TABLE_ID: {
                "table_id": TABLE_ID,
                "schema_name": "analytics",
                "table_name": "orders",
            }
        },
        "by_name": {},
    }
    result_rows = [
        {
            "table_id": TABLE_ID,
            "column_id": "col-1",
            "schema_name": "analytics",
            "table_name": "orders",
            "column_name": "total",
            "data_type": "DECIMAL(10,2)",
            "general_type": "numeric",
            "record_count": 250,
            "null_count": 5,
            "distinct_count": 15,
            "min_value": "1",
            "max_value": "999",
            "avg_value": 42.5,
            "stddev_value": 1.5,
            "percentiles_json": "{\"p95\": 200}",
            "top_values_json": "",
            "metrics_json": "{\"dq_dimension\": \"accuracy\"}",
        }
    ]
    value_rows = [
        {
            "schema_name": "analytics",
            "table_name": "orders",
            "column_name": "total",
            "value": "widget",
            "frequency": 3,
            "relative_freq": 0.3,
            "rank": 1,
        },
        {
            "schema_name": "analytics",
            "table_name": "orders",
            "column_name": "total",
            "bucket_label": "0-10",
            "bucket_lower_bound": 0.0,
            "bucket_upper_bound": 10.0,
            "frequency": 5,
        },
    ]
    anomaly_rows = [
        {
            "table_id": TABLE_ID,
            "column_id": "col-1",
            "table_name": "orders",
            "column_name": "total",
            "anomaly_type_id": "null_density",
            "severity": "high",
            "detail": "Null ratio",
            "detected_at": datetime(2025, 11, 16, tzinfo=timezone.utc),
        },
        {
            "table_id": TABLE_ID,
            "column_id": None,
            "table_name": "orders",
            "column_name": None,
            "anomaly_type_id": "table_notice",
            "severity": "low",
            "detail": "table anomaly",
        },
    ]

    monkeypatch.setattr(
        TestGenClient,
        "_fetch_profile_run_record",
        lambda self, run_id, table_group_id=None: summary_row if run_id == "run-1" else None,
    )
    monkeypatch.setattr(TestGenClient, "_latest_completed_profile_run", lambda self, tg: summary_row)
    monkeypatch.setattr(TestGenClient, "_fetch_table_catalog", lambda self, tg: table_catalog)
    monkeypatch.setattr(TestGenClient, "_fetch_profile_result_rows", lambda self, run_id: result_rows)
    monkeypatch.setattr(TestGenClient, "_fetch_profile_value_rows", lambda self, run_id: value_rows)
    monkeypatch.setattr(TestGenClient, "_fetch_profile_anomaly_rows", lambda self, run_id: anomaly_rows)

    payload = client.export_profiling_payload(TABLE_GROUP_ID, profile_run_id="run-1")

    assert payload is not None
    assert payload["summary"]["status"] == "completed"
    assert payload["summary"]["row_count"] == 250
    assert payload["tables"][0]["table_id"] == TABLE_ID
    column_entry = payload["tables"][0]["columns"][0]
    assert column_entry["column_name"] == "total"
    assert column_entry["top_values"][0]["value"] == "widget"
    assert column_entry["histogram"][0]["label"] == "0-10"
    assert column_entry["anomalies"][0]["anomaly_type_id"] == "null_density"
    assert payload["tables"][0]["anomalies"][0]["anomaly_type_id"] == "table_notice"


def test_export_profiling_payload_returns_none_without_results(monkeypatch, sample_params):
    client = TestGenClient(sample_params, schema="dq")
    summary_row = {
        "profile_run_id": "run-1",
        "table_group_id": TABLE_GROUP_ID,
        "status": "completed",
    }

    monkeypatch.setattr(
        TestGenClient,
        "_fetch_profile_run_record",
        lambda self, run_id, table_group_id=None: summary_row,
    )
    monkeypatch.setattr(TestGenClient, "_fetch_profile_result_rows", lambda self, run_id: [])
    monkeypatch.setattr(TestGenClient, "_fetch_table_catalog", lambda self, tg: {"by_id": {}, "by_name": {}})

    payload = client.export_profiling_payload(TABLE_GROUP_ID, profile_run_id="run-1")

    assert payload is None


def test_column_profile_sql_path_can_be_disabled(monkeypatch, sample_params):
    executed: list[tuple[str, dict[str, Any]]] = []
    profile_row = {
        "profile_run_id": "run-flag",
        "table_group_id": TABLE_GROUP_ID,
        "status": "completed",
        "started_at": datetime(2025, 11, 16, tzinfo=timezone.utc),
        "completed_at": datetime(2025, 11, 16, tzinfo=timezone.utc),
        "row_count": 99,
        "anomaly_count": 0,
    }
    results_sequence = [[profile_row]]
    _install_dummy_engine(monkeypatch, executed, results_sequence=results_sequence)

    class StubSettings:
        testgen_profile_table_reads_enabled = False

    client = TestGenClient(sample_params, schema="dq", settings=StubSettings())
    profile = client.column_profile(TABLE_GROUP_ID, column_name="total", table_name="orders")
    client.close()

    assert profile is None
    assert not any("dq_profile_columns" in stmt for stmt, _ in executed)



def test_column_profile_returns_none_when_missing_run(monkeypatch, sample_params):
    executed: list[tuple[str, dict[str, Any]]] = []
    results_sequence: list[list[dict[str, Any]]] = [[]]
    _install_dummy_engine(monkeypatch, executed, results_sequence=results_sequence)

    client = TestGenClient(sample_params, schema="dq")
    profile = client.column_profile(TABLE_GROUP_ID, column_name="total", table_name="orders")
    client.close()

    assert profile is None


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
        client.start_profile_run(TABLE_GROUP_ID)
    client.close()
