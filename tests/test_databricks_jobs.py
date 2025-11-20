from __future__ import annotations

from typing import Any, Mapping

import pytest

from app.services.databricks_jobs import (
    DatabricksJobsClient,
    DatabricksJobsConfig,
    DatabricksJobsError,
    DatabricksRunHandle,
)


class DummyResponse:
    def __init__(self, status_code: int = 200, *, payload: Mapping[str, Any] | None = None, text: str | None = None, reason: str | None = None):
        self.status_code = status_code
        self._payload = payload
        self.text = text or ""
        self.reason = reason or ""

    def json(self) -> Mapping[str, Any] | None:
        if self._payload is None:
            raise ValueError("no json payload")
        return self._payload


def _build_client(record: list[dict[str, Any]], *, response: DummyResponse) -> DatabricksJobsClient:
    def _request(method: str, url: str, headers: dict[str, str], json_payload, params, timeout: int):
        record.append(
            {
                "method": method,
                "url": url,
                "headers": headers,
                "json": json_payload,
                "params": params,
                "timeout": timeout,
            }
        )
        return response

    config = DatabricksJobsConfig(host="adb-123.cloud.databricks.com", token="secret", timeout_seconds=15)
    return DatabricksJobsClient(config=config, request_func=_request)


def test_client_requires_host_and_token():
    with pytest.raises(ValueError):
        DatabricksJobsClient(config=DatabricksJobsConfig(host="", token="secret"))

    with pytest.raises(ValueError):
        DatabricksJobsClient(config=DatabricksJobsConfig(host="adb", token=""))


def test_create_job_posts_payload_and_returns_response():
    calls: list[dict[str, Any]] = []
    response = DummyResponse(payload={"job_id": 987})
    client = _build_client(calls, response=response)

    payload = {"name": "profiling-job", "tasks": []}
    result = client.create_job(payload)

    assert result == {"job_id": 987}
    assert len(calls) == 1
    assert calls[0]["method"] == "POST"
    assert calls[0]["url"].endswith("/api/2.1/jobs/create")
    assert calls[0]["json"] == payload
    assert calls[0]["timeout"] == 15


def test_run_now_returns_handle_with_state():
    calls: list[dict[str, Any]] = []
    response = DummyResponse(payload={"run_id": 777, "number_in_job": 3, "state": {"life_cycle_state": "PENDING"}})
    client = _build_client(calls, response=response)

    handle = client.run_now(321, notebook_params={"tableGroupId": "tg-1"}, idempotency_token="abc")

    assert isinstance(handle, DatabricksRunHandle)
    assert handle.run_id == 777
    assert handle.number_in_job == 3
    assert handle.state == {"life_cycle_state": "PENDING"}

    assert calls[0]["json"]["job_id"] == 321
    assert calls[0]["json"]["notebook_params"] == {"tableGroupId": "tg-1"}
    assert calls[0]["json"]["idempotency_token"] == "abc"


def test_request_errors_raise_databricks_error():
    calls: list[dict[str, Any]] = []
    response = DummyResponse(status_code=400, payload={"error_code": "INVALID", "message": "Bad request"}, text="bad")
    client = _build_client(calls, response=response)

    with pytest.raises(DatabricksJobsError) as exc:
        client.get_run(1)

    assert "400" in str(exc.value)
    assert calls[0]["params"] == {"run_id": 1}