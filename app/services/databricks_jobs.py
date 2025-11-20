from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping

from app.config import get_settings

RequestFunc = Callable[[str, str, dict[str, str], dict[str, Any] | None, dict[str, Any] | None, int], Any]


class DatabricksJobsError(RuntimeError):
    """Raised when Databricks Jobs API calls fail."""


@dataclass(frozen=True)
class DatabricksJobsConfig:
    host: str
    token: str
    timeout_seconds: int = 30


@dataclass(frozen=True)
class DatabricksRunHandle:
    run_id: int
    number_in_job: int | None = None
    state: Mapping[str, Any] | None = None


class DatabricksJobsClient:
    """Thin wrapper around the Databricks Jobs REST API."""

    def __init__(
        self,
        *,
        config: DatabricksJobsConfig | None = None,
        request_func: RequestFunc | None = None,
    ) -> None:
        if config is None:
            settings = get_settings()
            host = (settings.databricks_host or "").strip()
            token = (settings.databricks_token or "").strip()
            config = DatabricksJobsConfig(host=host, token=token)

        host = (config.host or "").strip()
        token = (config.token or "").strip()
        if not host:
            raise ValueError("Databricks host is required to initialize DatabricksJobsClient.")
        if not token:
            raise ValueError("Databricks access token is required to initialize DatabricksJobsClient.")

        base = host if host.startswith("http") else f"https://{host}"
        self._base_url = base.rstrip("/")
        self._token = token
        self._timeout = max(1, config.timeout_seconds)
        self._request_func = request_func

    def create_job(self, job_settings: Mapping[str, Any]) -> Mapping[str, Any]:
        payload = dict(job_settings)
        return self._request("POST", "/api/2.1/jobs/create", json_payload=payload)  # type: ignore[return-value]

    def update_job(self, job_id: int, new_settings: Mapping[str, Any]) -> Mapping[str, Any]:
        payload = {"job_id": job_id, "new_settings": dict(new_settings)}
        return self._request("POST", "/api/2.1/jobs/update", json_payload=payload)  # type: ignore[return-value]

    def delete_job(self, job_id: int) -> None:
        self._request("POST", "/api/2.1/jobs/delete", json_payload={"job_id": job_id}, expected_statuses=(200, 204))

    def run_now(
        self,
        job_id: int,
        *,
        notebook_params: Mapping[str, Any] | None = None,
        python_params: Iterable[str] | None = None,
        idempotency_token: str | None = None,
    ) -> DatabricksRunHandle:
        payload: dict[str, Any] = {"job_id": job_id}
        if notebook_params:
            payload["notebook_params"] = dict(notebook_params)
        if python_params:
            payload["python_params"] = list(python_params)
        if idempotency_token:
            payload["idempotency_token"] = idempotency_token
        response = self._request("POST", "/api/2.1/jobs/run-now", json_payload=payload)
        return self._parse_run_response(response)

    def submit_run(self, run_definition: Mapping[str, Any]) -> DatabricksRunHandle:
        response = self._request("POST", "/api/2.1/jobs/runs/submit", json_payload=dict(run_definition))
        return self._parse_run_response(response)

    def get_run(self, run_id: int) -> Mapping[str, Any]:
        params = {"run_id": run_id}
        return self._request("GET", "/api/2.1/jobs/runs/get", params=params)  # type: ignore[return-value]

    def list_runs(self, *, job_id: int | None = None, limit: int = 25) -> Mapping[str, Any]:
        params: dict[str, Any] = {"limit": limit}
        if job_id is not None:
            params["job_id"] = job_id
        return self._request("GET", "/api/2.1/jobs/runs/list", params=params)  # type: ignore[return-value]

    def cancel_run(self, run_id: int) -> None:
        self._request("POST", "/api/2.1/jobs/runs/cancel", json_payload={"run_id": run_id}, expected_statuses=(200, 202, 204))

    def _parse_run_response(self, payload: Mapping[str, Any] | None) -> DatabricksRunHandle:
        if not isinstance(payload, Mapping):
            raise DatabricksJobsError("Databricks API response is missing run metadata.")
        run_id = payload.get("run_id")
        if not isinstance(run_id, int):
            raise DatabricksJobsError("Databricks API response did not include a numeric run_id.")
        number_in_job = payload.get("number_in_job")
        state = payload.get("state")
        return DatabricksRunHandle(
            run_id=run_id,
            number_in_job=number_in_job if isinstance(number_in_job, int) else None,
            state=state if isinstance(state, Mapping) else None,
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_payload: Mapping[str, Any] | None = None,
        params: Mapping[str, Any] | None = None,
        expected_statuses: tuple[int, ...] = (200,),
    ) -> Mapping[str, Any] | None:
        url = f"{self._base_url}{path}"
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }
        try:
            response = self._dispatch_request(method, url, headers, json_payload, params)
        except DatabricksJobsError:
            raise
        except Exception as exc:  # pragma: no cover - defensive networking
            raise DatabricksJobsError(f"Databricks request to {path} failed: {exc}") from exc

        if response.status_code not in expected_statuses:
            detail = self._extract_detail(response)
            raise DatabricksJobsError(
                f"Databricks API call {method} {path} failed with {response.status_code}: {detail}"
            )

        if response.status_code == 204:
            return None

        try:
            return response.json()
        except ValueError:  # pragma: no cover - non-json responses
            return None

    def _dispatch_request(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        json_payload: Mapping[str, Any] | None,
        params: Mapping[str, Any] | None,
    ):
        if self._request_func is not None:
            return self._request_func(method, url, headers, json_payload and dict(json_payload), params and dict(params), self._timeout)

        try:
            import requests
        except ModuleNotFoundError as exc:  # pragma: no cover - dependency missing
            raise DatabricksJobsError(
                "The 'requests' package is required to call the Databricks Jobs API."
            ) from exc

        return requests.request(
            method,
            url,
            headers=headers,
            json=json_payload,
            params=params,
            timeout=self._timeout,
        )

    @staticmethod
    def _extract_detail(response: Any) -> str:
        try:
            payload = response.json()
        except Exception:  # pragma: no cover - fallback to text
            payload = None

        if isinstance(payload, Mapping):
            message = payload.get("message") or payload.get("error") or payload.get("error_code")
            if message:
                return str(message)
        text = getattr(response, "text", None)
        if text:
            return str(text).strip()
        reason = getattr(response, "reason", None)
        if reason:
            return str(reason)
        return "unknown error"
