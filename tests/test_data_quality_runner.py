from __future__ import annotations

from datetime import datetime, timezone

from app.services.data_quality_keys import build_project_key, build_table_group_id

from app.services.data_quality_executor import DataQualityRunExecutor
from app.services.data_quality_notifications import RunNotification
from app.services.data_quality_runner import queue_validation_run
from app.services.databricks_sql import DatabricksConnectionParams


def _build_params(**overrides) -> DatabricksConnectionParams:
    base = dict(
        workspace_host="adb-1234",
        http_path="/sql/warehouses/abc",
        access_token="token",
        catalog="workspace",
        schema_name="default",
        constructed_schema="constructed",
        data_quality_schema="dq",
        data_quality_auto_manage_tables=True,
    )
    base.update(overrides)
    return DatabricksConnectionParams(**base)


PROJECT_KEY = build_project_key("workspace-123")
TEST_SUITE_KEY = build_table_group_id("conn-456", "object-456")


def test_queue_validation_run_invokes_executor(monkeypatch):
    calls: dict[str, object] = {}

    class DummyExecutor:
        def execute(self, project_key, **kwargs):
            calls["project_key"] = project_key
            calls["kwargs"] = kwargs
            return "run-123"

    monkeypatch.setattr(
        "app.services.data_quality_runner.DataQualityRunExecutor",
        lambda: DummyExecutor(),
    )

    run_id = queue_validation_run(
        PROJECT_KEY,
        test_suite_key=TEST_SUITE_KEY,
        trigger_source="ingestion-run:abc",
        status="running",
        wait=True,
    )

    assert run_id == "run-123"
    assert calls["project_key"] == PROJECT_KEY
    assert calls["kwargs"] == {
        "test_suite_key": TEST_SUITE_KEY,
        "trigger_source": "ingestion-run:abc",
        "start_status": "running",
    }


def test_data_quality_run_executor_records_results():
    params = _build_params()
    recorded: dict[str, object] = {}
    notifications: list[RunNotification] = []

    class DummyClient:
        def __init__(self, resolved_params, schema: str) -> None:
            assert resolved_params is params
            assert schema == "dq"

        def start_test_run(self, **kwargs):
            recorded["start"] = kwargs
            return "run-1"

        def record_test_results(self, run_id, results):
            recorded["record"] = (run_id, list(results))

        def complete_test_run(self, run_id, **kwargs):
            recorded["complete"] = (run_id, kwargs)

        def close(self):
            recorded["closed"] = True

    payload = {
        "status": "passed",
        "duration_ms": 1250,
        "total_tests": 2,
        "failed_tests": 1,
        "results": [
            {
                "test_id": "test-1",
                "table_name": "dim_customer",
                "column_name": "email",
                "status": "failed",
                "expected_value": "not null",
                "actual_value": "null",
                "message": "Email should not be null",
                "detected_at": datetime.now(timezone.utc).isoformat(),
            },
            {
                "test_id": "test-2",
                "table_name": "dim_customer",
                "column_name": "id",
                "status": "passed",
                "message": "",
                "detected_at": None,
            },
        ],
    }

    executor = DataQualityRunExecutor(
        params_resolver=lambda: params,
        client_factory=lambda *_args: DummyClient(*_args),
        command_runner=lambda *_args: payload,
        notification_service=_capture_notifications(notifications),
    )

    run_id = executor.execute(
        PROJECT_KEY,
        test_suite_key=TEST_SUITE_KEY,
        trigger_source="ingestion-run:abc",
    )

    assert run_id == "run-1"
    assert recorded["start"]["project_key"] == PROJECT_KEY
    assert recorded["start"]["test_suite_key"] == TEST_SUITE_KEY
    assert recorded["record"][0] == "run-1"
    assert len(recorded["record"][1]) == 2
    assert recorded["complete"] == (
        "run-1",
        {"status": "completed", "failed_tests": 1, "duration_ms": 1250},
    )
    assert recorded.get("closed") is True
    assert len(notifications) == 1
    note = notifications[0]
    assert note.run_id == "run-1"
    assert note.failed_tests == 1
    assert len(list(note.results)) == 2


def test_data_quality_run_executor_skips_without_schema():
    params = _build_params(data_quality_schema=None)
    notifications: list[RunNotification] = []
    executor = DataQualityRunExecutor(
        params_resolver=lambda: params,
        client_factory=lambda *_args: (_ for _ in ()).throw(RuntimeError("should not initialize")),
        command_runner=lambda *_args: {},
        notification_service=_capture_notifications(notifications),
    )

    run_id = executor.execute(PROJECT_KEY)
    assert run_id is None
    assert notifications == []


def test_data_quality_run_executor_marks_failure():
    params = _build_params()
    recorded: dict[str, object] = {}
    notifications: list[RunNotification] = []

    class DummyClient:
        def __init__(self, *_args) -> None:
            pass

        def start_test_run(self, **_kwargs):
            return "run-fail"

        def complete_test_run(self, run_id, **kwargs):
            recorded.setdefault("complete", []).append((run_id, kwargs))

        def close(self):
            recorded["closed"] = True

    def _raise_cli(*_args):
        raise RuntimeError("CLI failed")

    executor = DataQualityRunExecutor(
        params_resolver=lambda: params,
        client_factory=lambda *_args: DummyClient(*_args),
        command_runner=_raise_cli,
        notification_service=_capture_notifications(notifications),
    )

    run_id = executor.execute(PROJECT_KEY)
    assert run_id == "run-fail"
    assert recorded["complete"][-1][1]["status"] == "failed"
    assert recorded.get("closed") is True
    assert len(notifications) == 1
    assert notifications[0].run_id == "run-fail"


def test_queue_validation_run_handles_missing_configuration(monkeypatch):
    def _raise() -> DatabricksConnectionParams:
        raise RuntimeError("databricks config missing")

    monkeypatch.setattr(
        "app.services.data_quality_executor.get_ingestion_connection_params",
        _raise,
    )

    executor = DataQualityRunExecutor(command_runner=lambda *_args: {})
    assert executor.execute(PROJECT_KEY) is None


def _capture_notifications(store: list[RunNotification]):
    class _Service:
        def send(self, payload: RunNotification) -> None:
            store.append(payload)

    return _Service()
