from __future__ import annotations

import base64
import gzip
import json
from types import SimpleNamespace
from typing import Any, Mapping

import pytest

from app.services.data_quality_profiling import (
    CALLBACK_URL_PLACEHOLDER,
    DataQualityProfilingService,
    ProfilingConfigurationError,
    ProfilingJobError,
    ProfilingTargetNotFound,
)
from app.services.databricks_jobs import DatabricksJobsError, DatabricksRunHandle
from app.services.databricks_sql import DatabricksConnectionParams
from app.services.data_quality_testgen import TestGenClientError


@pytest.fixture(autouse=True)
def _stub_workspace_client(monkeypatch: pytest.MonkeyPatch):
    class StubWorkspaceClient:
        def __init__(self, *, config, request_func=None):  # type: ignore[unused-arg]
            self.config = config

        def get_workspace_status(self, path: str):  # pragma: no cover - simple test stub
            return {"object_type": "NOTEBOOK", "path": path}

        def list_directory(self, path: str):  # pragma: no cover - simple test stub
            return []

    monkeypatch.setattr(
        "app.services.data_quality_profiling.DatabricksWorkspaceClient",
        StubWorkspaceClient,
    )


def _build_group_row(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "table_group_id": "group:conn:object",
        "connection_id": "conn:123:456",
        "name": "Finance Tables",
        "connection_name": "Finance Connection",
        "catalog": "main",
        "schema_name": "finance",
        "http_path": "/sql/1.0/warehouses/abc",
        "project_key": "system:123:object:456",
        "system_id": "123",
        "profiling_job_id": None,
        "is_active": True,
    }
    base.update(overrides)
    return base


def _build_settings(**overrides: Any) -> SimpleNamespace:
    defaults = {
        "databricks_profile_notebook_path": "/Repos/profiling",
        "databricks_profile_existing_cluster_id": "cluster-1",
        "databricks_profile_policy_id": None,
        "databricks_profile_job_name_prefix": "DQ - ",
        "databricks_profile_callback_url": "https://callback/api",
        "databricks_profile_callback_token": "token-123",
        "databricks_host": "adb-test.cloud.databricks.com",
        "databricks_token": "dapi-test",
        "databricks_spark_compute": "classic",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _decode_inline_payload(blob: str) -> Any:
    assert blob.startswith("base64:"), "Inline payload should include base64 prefix"
    encoded = blob.split(":", 1)[1]
    raw = gzip.decompress(base64.b64decode(encoded)).decode("utf-8")
    return json.loads(raw)


class DummyTestGenClient:
    def __init__(self, row: dict[str, Any] | None):
        self.schema = "dq_metadata"
        self._row = row
        self.start_calls: list[dict[str, Any]] = []
        self.job_updates: list[tuple[str, str]] = []
        self.profile_updates: list[dict[str, Any]] = []
        self.completed: list[dict[str, Any]] = []

    def get_table_group_details(self, table_group_id: str) -> dict[str, Any] | None:
        return self._row

    def start_profile_run(self, table_group_id: str, *, status: str = "running", started_at=None) -> str:  # noqa: ARG002
        run_id = f"profile-{len(self.start_calls) + 1}"
        self.start_calls.append(
            {
                "table_group_id": table_group_id,
                "status": status,
            }
        )
        return run_id

    def update_table_group_profiling_job(self, table_group_id: str, profiling_job_id: str | None) -> None:
        self.job_updates.append((table_group_id, profiling_job_id or ""))

    def update_profile_run_databricks_run(
        self,
        profile_run_id: str,
        *,
        databricks_run_id: str | None,
    ) -> None:
        self.profile_updates.append(
            {
                "profile_run_id": profile_run_id,
                "databricks_run_id": databricks_run_id,
            }
        )

    def complete_profile_run(self, profile_run_id: str, *, status: str, row_count=None, anomaly_count=None, anomalies=None) -> None:  # noqa: ARG002
        self.completed.append({"profile_run_id": profile_run_id, "status": status})


class PayloadExportingClient(DummyTestGenClient):
    def __init__(self, row: dict[str, Any] | None, payload: Any = None, *, should_raise: bool = False):
        super().__init__(row)
        self._payload = payload
        self._should_raise = should_raise
        self.export_calls: list[tuple[str, str | None]] = []

    def export_profiling_payload(self, table_group_id: str, profile_run_id: str | None = None):
        self.export_calls.append((table_group_id, profile_run_id))
        if self._should_raise:
            raise TestGenClientError("payload boom")
        return self._payload


class DummyJobsClient:
    def __init__(self, *, run_error: Exception | None = None):
        self.created_jobs: list[Mapping[str, Any]] = []
        self.updated_jobs: list[dict[str, Any]] = []
        self.run_requests: list[dict[str, Any]] = []
        self._job_id_sequence = 100
        self._run_error = run_error

    def create_job(self, job_settings: Mapping[str, Any]):
        self.created_jobs.append(dict(job_settings))
        self._job_id_sequence += 1
        return {"job_id": self._job_id_sequence}

    def update_job(self, job_id: int, new_settings: Mapping[str, Any]):
        self.updated_jobs.append({"job_id": job_id, "settings": dict(new_settings)})

    def run_now(self, job_id: int, *, notebook_params: Mapping[str, Any], idempotency_token: str):
        if self._run_error is not None:
            raise self._run_error
        self.run_requests.append(
            {
                "job_id": job_id,
                "notebook_params": dict(notebook_params),
                "idempotency_token": idempotency_token,
            }
        )
        return DatabricksRunHandle(run_id=901)


class DummyWorkspaceClient:
    def __init__(self, *, statuses: Mapping[str, Mapping[str, Any] | None], listings: Mapping[str, list[Mapping[str, Any]]]):
        self._statuses = statuses
        self._listings = listings
        self.status_calls: list[str] = []
        self.list_calls: list[str] = []

    def get_workspace_status(self, path: str) -> Mapping[str, Any] | None:  # pragma: no cover - simple data lookup
        self.status_calls.append(path)
        return self._statuses.get(path)

    def list_directory(self, path: str) -> list[Mapping[str, Any]]:  # pragma: no cover - simple data lookup
        self.list_calls.append(path)
        return list(self._listings.get(path, []))


def test_service_creates_job_and_launches_run():
    row = _build_group_row()
    client = DummyTestGenClient(row)
    jobs = DummyJobsClient()
    service = DataQualityProfilingService(client, jobs_client=jobs, settings=_build_settings())

    result = service.start_profile_for_table_group(row["table_group_id"])

    assert result.profile_run_id == "profile-1"
    assert jobs.created_jobs
    assert not jobs.updated_jobs
    assert client.job_updates == [(row["table_group_id"], str(result.job_id))]

    first_update = client.profile_updates[0]
    assert first_update["databricks_run_id"] == "901"
    assert jobs.run_requests[0]["notebook_params"]["callback_token"] == "token-123"


def test_callback_template_injects_run_specific_url():
    row = _build_group_row()
    client = DummyTestGenClient(row)
    jobs = DummyJobsClient()
    settings = _build_settings(databricks_profile_callback_url=None)
    template = f"https://api.example.com/profile-runs/{CALLBACK_URL_PLACEHOLDER}/complete"

    service = DataQualityProfilingService(client, jobs_client=jobs, settings=settings)

    result = service.start_profile_for_table_group(
        row["table_group_id"],
        callback_url_template=template,
    )

    params = jobs.run_requests[0]["notebook_params"]
    assert params["callback_url"] == template.replace(CALLBACK_URL_PLACEHOLDER, result.profile_run_id)


def test_callback_template_without_placeholder_appends_completion_suffix():
    row = _build_group_row()
    client = DummyTestGenClient(row)
    jobs = DummyJobsClient()
    settings = _build_settings(databricks_profile_callback_url=None)
    base_url = "https://api.example.com/data-quality/testgen/profile-runs"

    service = DataQualityProfilingService(client, jobs_client=jobs, settings=settings)

    result = service.start_profile_for_table_group(
        row["table_group_id"],
        callback_url_template=base_url,
    )

    params = jobs.run_requests[0]["notebook_params"]
    assert params["callback_url"].endswith(f"/{result.profile_run_id}/complete")


def test_inline_payload_included_when_exporter_available():
    row = _build_group_row()
    payload = {"tables": [{"table_name": "orders"}], "summary": {"status": "completed"}}
    client = PayloadExportingClient(row, payload=payload)
    jobs = DummyJobsClient()
    service = DataQualityProfilingService(client, jobs_client=jobs, settings=_build_settings())

    service.start_profile_for_table_group(row["table_group_id"])

    params = jobs.run_requests[0]["notebook_params"]
    assert "profiling_payload_inline" in params
    decoded = _decode_inline_payload(params["profiling_payload_inline"])
    assert decoded == payload
    assert client.export_calls[0] == (row["table_group_id"], "profile-1")


def test_inline_payload_skipped_when_exporter_missing():
    row = _build_group_row()
    client = DummyTestGenClient(row)
    jobs = DummyJobsClient()
    service = DataQualityProfilingService(client, jobs_client=jobs, settings=_build_settings())

    service.start_profile_for_table_group(row["table_group_id"])

    params = jobs.run_requests[0]["notebook_params"]
    assert "profiling_payload_inline" not in params


def test_inline_payload_export_failure_is_ignored():
    row = _build_group_row()
    client = PayloadExportingClient(row, payload=None, should_raise=True)
    jobs = DummyJobsClient()
    service = DataQualityProfilingService(client, jobs_client=jobs, settings=_build_settings())

    service.start_profile_for_table_group(row["table_group_id"])

    params = jobs.run_requests[0]["notebook_params"]
    assert "profiling_payload_inline" not in params
def test_service_reuses_existing_job():
    row = _build_group_row(profiling_job_id="77")
    client = DummyTestGenClient(row)
    jobs = DummyJobsClient()
    service = DataQualityProfilingService(client, jobs_client=jobs, settings=_build_settings())

    result = service.start_profile_for_table_group(row["table_group_id"])

    assert not jobs.created_jobs
    assert jobs.updated_jobs[0]["job_id"] == 77
    assert client.job_updates == [(row["table_group_id"], "77")]
    assert result.job_id == 77


def test_missing_table_group_raises_not_found():
    client = DummyTestGenClient(row=None)
    jobs = DummyJobsClient()
    service = DataQualityProfilingService(client, jobs_client=jobs, settings=_build_settings())

    with pytest.raises(ProfilingTargetNotFound):
        service.start_profile_for_table_group("group:missing")

    assert not client.start_calls


def test_notebook_path_falls_back_to_stored_setting(monkeypatch: pytest.MonkeyPatch):
    row = _build_group_row()
    client = DummyTestGenClient(row)
    jobs = DummyJobsClient()
    settings = _build_settings(databricks_profile_notebook_path="  ")
    fallback_path = "/Repos/fallback/profile"

    def fake_params():
        return SimpleNamespace(
            profiling_notebook_path=fallback_path,
            workspace_host="adb-fallback",
            access_token="token-fallback",
        )

    monkeypatch.setattr(
        "app.services.data_quality_profiling.get_ingestion_connection_params",
        fake_params,
    )

    service = DataQualityProfilingService(client, jobs_client=jobs, settings=settings)

    service.start_profile_for_table_group(row["table_group_id"])

    assert jobs.created_jobs
    created_task = jobs.created_jobs[0]["tasks"][0]
    assert created_task["notebook_task"]["notebook_path"] == fallback_path


def test_directory_notebook_path_selects_preferred_notebook():
    row = _build_group_row()
    client = DummyTestGenClient(row)
    jobs = DummyJobsClient()
    directory_path = "/Repos/profiling-folder"
    workspace = DummyWorkspaceClient(
        statuses={directory_path: {"object_type": "DIRECTORY"}},
        listings={
            directory_path: [
                {"path": f"{directory_path}/misc", "object_type": "NOTEBOOK"},
                {"path": f"{directory_path}/profiling", "object_type": "NOTEBOOK"},
            ]
        },
    )
    service = DataQualityProfilingService(
        client,
        jobs_client=jobs,
        workspace_client=workspace,
        settings=_build_settings(databricks_profile_notebook_path=directory_path),
    )

    service.start_profile_for_table_group(row["table_group_id"])

    notebook_path = jobs.created_jobs[0]["tasks"][0]["notebook_task"]["notebook_path"]
    assert notebook_path == f"{directory_path}/profiling"


def test_directory_notebook_path_without_match_uses_first_available():
    row = _build_group_row()
    client = DummyTestGenClient(row)
    jobs = DummyJobsClient()
    directory_path = "/Repos/profiling-folder"
    workspace = DummyWorkspaceClient(
        statuses={directory_path: {"object_type": "DIRECTORY"}},
        listings={
            directory_path: [
                {"path": f"{directory_path}/B", "object_type": "NOTEBOOK"},
                {"path": f"{directory_path}/A", "object_type": "NOTEBOOK"},
            ]
        },
    )
    service = DataQualityProfilingService(
        client,
        jobs_client=jobs,
        workspace_client=workspace,
        settings=_build_settings(databricks_profile_notebook_path=directory_path),
    )

    service.start_profile_for_table_group(row["table_group_id"])

    notebook_path = jobs.created_jobs[0]["tasks"][0]["notebook_task"]["notebook_path"]
    assert notebook_path == f"{directory_path}/A"


def test_directory_without_notebooks_raises_configuration_error():
    row = _build_group_row()
    client = DummyTestGenClient(row)
    jobs = DummyJobsClient()
    directory_path = "/Repos/profiling-folder"
    workspace = DummyWorkspaceClient(
        statuses={directory_path: {"object_type": "DIRECTORY"}},
        listings={directory_path: []},
    )
    service = DataQualityProfilingService(
        client,
        jobs_client=jobs,
        workspace_client=workspace,
        settings=_build_settings(databricks_profile_notebook_path=directory_path),
    )

    with pytest.raises(ProfilingConfigurationError):
        service.start_profile_for_table_group(row["table_group_id"])

    assert not jobs.created_jobs


def test_cluster_policy_falls_back_to_stored_setting(monkeypatch: pytest.MonkeyPatch):
    row = _build_group_row()
    client = DummyTestGenClient(row)
    jobs = DummyJobsClient()
    settings = _build_settings(databricks_profile_existing_cluster_id="", databricks_profile_policy_id=None)

    def fake_params():
        return SimpleNamespace(
            profiling_policy_id="policy-123",
            workspace_host="adb-fallback",
            access_token="token-fallback",
        )

    monkeypatch.setattr(
        "app.services.data_quality_profiling.get_ingestion_connection_params",
        fake_params,
    )

    service = DataQualityProfilingService(client, jobs_client=jobs, settings=settings)

    service.start_profile_for_table_group(row["table_group_id"])

    created_task = jobs.created_jobs[0]["tasks"][0]
    assert created_task["new_cluster"]["policy_id"] == "policy-123"


def test_serverless_compute_uses_workspace_compute(monkeypatch: pytest.MonkeyPatch):
    row = _build_group_row()
    client = DummyTestGenClient(row)
    jobs = DummyJobsClient()
    settings = _build_settings(databricks_profile_existing_cluster_id="", databricks_profile_policy_id=None)

    params = DatabricksConnectionParams(
        workspace_host="adb-test",
        http_path="/sql/1.0/warehouses/abc",
        access_token="token",
        catalog="workspace",
        schema_name="default",
        spark_compute="serverless",
        warehouse_name="Serverless 2X-Small",
    )

    monkeypatch.setattr(
        "app.services.data_quality_profiling.get_ingestion_connection_params",
        lambda: params,
    )

    service = DataQualityProfilingService(client, jobs_client=jobs, settings=settings)

    service.start_profile_for_table_group(row["table_group_id"])

    task = jobs.created_jobs[0]["tasks"][0]
    assert "compute" in task
    assert task["compute"]["compute_type"] == "SERVERLESS"
    assert task["compute"]["workspace_compute"]["workspace_compute_name"] == "Serverless 2X-Small"


def test_missing_cluster_configuration_falls_back_to_serverless(monkeypatch: pytest.MonkeyPatch):
    row = _build_group_row()
    client = DummyTestGenClient(row)
    jobs = DummyJobsClient()
    settings = _build_settings(databricks_profile_existing_cluster_id="", databricks_profile_policy_id=None)

    params = DatabricksConnectionParams(
        workspace_host="adb-test",
        http_path="/sql/1.0/warehouses/abc",
        access_token="token",
        catalog="workspace",
        schema_name="default",
        warehouse_name="Serverless Small",
    )

    monkeypatch.setattr(
        "app.services.data_quality_profiling.get_ingestion_connection_params",
        lambda: params,
    )

    service = DataQualityProfilingService(client, jobs_client=jobs, settings=settings)

    service.start_profile_for_table_group(row["table_group_id"])

    task = jobs.created_jobs[0]["tasks"][0]
    assert task["compute"]["compute_type"] == "SERVERLESS"


def test_job_retries_with_serverless_when_workspace_requires():
    row = _build_group_row()
    client = DummyTestGenClient(row)

    class FailingJobsClient(DummyJobsClient):
        def __init__(self):
            super().__init__()
            self._should_fail = True

        def create_job(self, job_settings: Mapping[str, Any]):  # type: ignore[override]
            if self._should_fail:
                self._should_fail = False
                raise DatabricksJobsError("Only serverless compute is supported")
            return super().create_job(job_settings)

    jobs = FailingJobsClient()
    settings = _build_settings(databricks_profile_existing_cluster_id="", databricks_profile_policy_id="policy-xyz")

    service = DataQualityProfilingService(client, jobs_client=jobs, settings=settings)

    service.start_profile_for_table_group(row["table_group_id"])

    assert jobs.created_jobs
    task = jobs.created_jobs[-1]["tasks"][0]
    assert task.get("compute", {}).get("compute_type") == "SERVERLESS"


def test_missing_notebook_configuration_aborts_before_run_registration(monkeypatch: pytest.MonkeyPatch):
    row = _build_group_row()
    client = DummyTestGenClient(row)
    jobs = DummyJobsClient()
    settings = _build_settings(databricks_profile_notebook_path="")

    def fake_params():
        return SimpleNamespace(
            profiling_notebook_path=None,
            profiling_policy_id=None,
        )

    monkeypatch.setattr(
        "app.services.data_quality_profiling.get_ingestion_connection_params",
        fake_params,
    )

    service = DataQualityProfilingService(client, jobs_client=jobs, settings=settings)

    with pytest.raises(ProfilingConfigurationError):
        service.start_profile_for_table_group(row["table_group_id"])

    assert not client.start_calls


def test_run_submission_failure_marks_profile_as_failed():
    row = _build_group_row()
    client = DummyTestGenClient(row)
    jobs = DummyJobsClient(run_error=DatabricksJobsError("boom"))
    service = DataQualityProfilingService(client, jobs_client=jobs, settings=_build_settings())

    with pytest.raises(ProfilingJobError):
        service.start_profile_for_table_group(row["table_group_id"])

    assert client.completed and client.completed[0]["status"] == "failed"
    assert len(client.profile_updates) == 0


def test_jobs_client_uses_ingestion_params_when_settings_missing(monkeypatch: pytest.MonkeyPatch):
    row = _build_group_row()
    client = DummyTestGenClient(row)
    settings = _build_settings(databricks_host="", databricks_token="")
    recorded: dict[str, str] = {}

    def fake_params():
        return SimpleNamespace(workspace_host="adb-fallback", access_token="token-fallback")

    monkeypatch.setattr(
        "app.services.data_quality_profiling.get_ingestion_connection_params",
        fake_params,
    )

    class RecordingJobsClient(DummyJobsClient):
        def __init__(self, *, config, request_func=None):  # type: ignore[override]
            super().__init__()
            recorded["host"] = config.host
            recorded["token"] = config.token

    monkeypatch.setattr(
        "app.services.data_quality_profiling.DatabricksJobsClient",
        RecordingJobsClient,
    )

    service = DataQualityProfilingService(client, settings=settings)

    service.start_profile_for_table_group(row["table_group_id"])

    assert recorded == {"host": "adb-fallback", "token": "token-fallback"}


def test_missing_host_configuration_raises(monkeypatch: pytest.MonkeyPatch):
    row = _build_group_row()
    client = DummyTestGenClient(row)
    settings = _build_settings(databricks_host="", databricks_token="")

    def fail_params():
        raise RuntimeError("no settings")

    monkeypatch.setattr(
        "app.services.data_quality_profiling.get_ingestion_connection_params",
        fail_params,
    )

    service = DataQualityProfilingService(client, settings=settings)

    with pytest.raises(ProfilingConfigurationError):
        service.start_profile_for_table_group(row["table_group_id"])