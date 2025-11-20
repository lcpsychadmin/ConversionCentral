from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import Settings, get_settings
from app.models import DatabricksClusterPolicy, DatabricksSqlSetting

RequestFunc = Callable[[str, str, dict[str, str], Mapping[str, Any] | None, Mapping[str, Any] | None, int], Any]


class DatabricksWorkspaceClientError(RuntimeError):
    """Raised when Databricks workspace API calls fail."""


# Backwards compatibility alias for older imports referring to policy-specific errors.
DatabricksPolicyClientError = DatabricksWorkspaceClientError


class DatabricksPolicySyncError(RuntimeError):
    """Raised when cluster policy synchronization fails."""


@dataclass(frozen=True)
class DatabricksWorkspaceConfig:
    host: str
    token: str
    timeout_seconds: int = 30


class DatabricksWorkspaceClient:
    """Minimal Databricks workspace client for cluster policy operations."""

    def __init__(
        self,
        *,
        config: DatabricksWorkspaceConfig,
        request_func: RequestFunc | None = None,
    ) -> None:
        host = (config.host or "").strip()
        token = (config.token or "").strip()
        if not host:
            raise ValueError("Databricks host is required to initialize DatabricksWorkspaceClient.")
        if not token:
            raise ValueError("Databricks access token is required to initialize DatabricksWorkspaceClient.")
        base = host if host.startswith("http") else f"https://{host}"
        self._base_url = base.rstrip("/")
        self._token = token
        self._timeout = max(1, config.timeout_seconds)
        self._request_func = request_func

    def list_cluster_policies(self) -> list[Mapping[str, Any]]:
        response = self._request("GET", "/api/2.0/policies/clusters/list")
        if not isinstance(response, Mapping):
            return []
        policies = response.get("policies")
        if not isinstance(policies, Sequence):
            return []

        results: list[Mapping[str, Any]] = []
        for item in policies:
            if isinstance(item, Mapping):
                results.append(item)
        return results

    def get_workspace_status(self, path: str) -> Mapping[str, Any] | None:
        params = {"path": path}
        payload = self._request(
            "GET",
            "/api/2.0/workspace/get-status",
            params=params,
            expected_statuses=(200, 404),
        )
        if not isinstance(payload, Mapping):
            return None
        if str(payload.get("error_code")) == "RESOURCE_DOES_NOT_EXIST":
            return None
        return payload

    def list_directory(self, path: str) -> list[Mapping[str, Any]]:
        params = {"path": path}
        response = self._request(
            "GET",
            "/api/2.0/workspace/list",
            params=params,
            expected_statuses=(200, 404),
        )
        if not isinstance(response, Mapping):
            return []
        if str(response.get("error_code")) == "RESOURCE_DOES_NOT_EXIST":
            return []
        objects = response.get("objects")
        if not isinstance(objects, Sequence):
            return []
        results: list[Mapping[str, Any]] = []
        for item in objects:
            if isinstance(item, Mapping):
                results.append(item)
        return results

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
        except DatabricksWorkspaceClientError:
            raise
        except Exception as exc:  # pragma: no cover - defensive networking
            raise DatabricksWorkspaceClientError(f"Databricks request to {path} failed: {exc}") from exc

        if response.status_code not in expected_statuses:
            detail = self._extract_detail(response)
            raise DatabricksWorkspaceClientError(
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
            raise DatabricksWorkspaceClientError(
                "The 'requests' package is required to call the Databricks workspace API."
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


def sync_cluster_policies(
    session: Session,
    *,
    settings: Settings | None = None,
    client: DatabricksWorkspaceClient | None = None,
) -> list[DatabricksClusterPolicy]:
    """Fetch cluster policies from Databricks and upsert them for the active setting."""

    settings = settings or get_settings()
    setting = _get_active_setting(session)
    if setting is None:
        raise DatabricksPolicySyncError("Databricks SQL settings are not configured. Configure the warehouse before syncing policies.")

    host = (setting.workspace_host or settings.databricks_host or "").strip()
    token = (setting.access_token or settings.databricks_token or "").strip()
    if not host:
        raise DatabricksPolicySyncError("Databricks workspace host is missing. Update the Databricks settings before syncing policies.")
    if not token:
        raise DatabricksPolicySyncError("Databricks access token is missing. Update the Databricks settings before syncing policies.")

    if client is None:
        config = DatabricksWorkspaceConfig(host=host, token=token)
        client = DatabricksWorkspaceClient(config=config)

    try:
        payloads = client.list_cluster_policies()
    except (ValueError, DatabricksPolicyClientError) as exc:
        raise DatabricksPolicySyncError(str(exc)) from exc

    _upsert_policies(session, setting, payloads)
    session.flush()

    stmt = (
        select(DatabricksClusterPolicy)
        .where(
            DatabricksClusterPolicy.setting_id == setting.id,
            DatabricksClusterPolicy.is_active.is_(True),
        )
        .order_by(DatabricksClusterPolicy.name.asc())
    )
    return list(session.execute(stmt).scalars())


def _upsert_policies(
    session: Session,
    setting: DatabricksSqlSetting,
    payloads: Sequence[Mapping[str, Any]],
) -> None:
    existing = {
        policy.policy_id: policy
        for policy in session.execute(
            select(DatabricksClusterPolicy).where(DatabricksClusterPolicy.setting_id == setting.id)
        ).scalars()
    }

    now = datetime.now(timezone.utc)
    seen: set[str] = set()
    for payload in payloads:
        policy_id = _coerce_str(payload.get("policy_id"))
        name = _coerce_str(payload.get("name"))
        if not policy_id or not name:
            continue
        description = _coerce_str(payload.get("description"))
        definition = _parse_definition(payload.get("definition"))

        record = existing.get(policy_id)
        if record is None:
            record = DatabricksClusterPolicy(
                setting_id=setting.id,
                policy_id=policy_id,
                name=name,
                description=description,
                definition=definition,
                synced_at=now,
                is_active=True,
            )
            session.add(record)
        else:
            record.name = name
            record.description = description
            record.definition = definition
            record.synced_at = now
            record.is_active = True
        seen.add(policy_id)

    for policy_id, record in existing.items():
        if policy_id not in seen:
            record.is_active = False
            record.synced_at = now


def _get_active_setting(session: Session) -> DatabricksSqlSetting | None:
    stmt = (
        select(DatabricksSqlSetting)
        .where(DatabricksSqlSetting.is_active.is_(True))
        .order_by(DatabricksSqlSetting.updated_at.desc())
        .limit(1)
    )
    return session.execute(stmt).scalars().first()


def _coerce_str(value: Any) -> str | None:
    if isinstance(value, str):
        trimmed = value.strip()
        return trimmed or None
    return None


def _parse_definition(raw: Any) -> dict[str, Any] | None:
    if raw is None:
        return None
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, str):
        payload = raw.strip()
        if not payload:
            return None
        try:
            parsed = json.loads(payload)
        except ValueError:
            return {"raw": payload}
        if isinstance(parsed, Mapping):
            return dict(parsed)
        return {"value": parsed}
    return None
