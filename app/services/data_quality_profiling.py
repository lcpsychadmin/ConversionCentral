from __future__ import annotations

import base64
import gzip
import json
import logging
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from app.config import Settings, get_settings
from app.ingestion.engine import get_ingestion_connection_params
from app.services.databricks_jobs import DatabricksJobsClient, DatabricksJobsConfig, DatabricksJobsError
from app.services.databricks_policies import (
    DatabricksWorkspaceClient,
    DatabricksWorkspaceClientError,
    DatabricksWorkspaceConfig,
)
from app.services.data_quality_keys import parse_connection_id
from app.services.data_quality_testgen import TestGenClient, TestGenClientError
from app.services.databricks_sql import DatabricksConnectionParams

logger = logging.getLogger(__name__)

NOTEBOOK_CANDIDATE_BASENAMES: tuple[str, ...] = (
    "profiling",
    "data_quality_profiling",
    "profile",
)

CALLBACK_URL_PLACEHOLDER = "__PROFILE_RUN_ID__"
INLINE_PAYLOAD_MAX_BYTES = 512 * 1024  # 512 KiB safety guard for widget payloads


class ProfilingServiceError(RuntimeError):
    """Base error for profiling orchestration failures."""


class ProfilingConfigurationError(ProfilingServiceError):
    """Raised when profiling cannot run due to missing configuration."""


class ProfilingTargetNotFound(ProfilingServiceError):
    """Raised when a requested table group cannot be located."""


class ProfilingJobError(ProfilingServiceError):
    """Raised when Databricks job provisioning or execution fails."""


class ProfilingConcurrencyLimitError(ProfilingJobError):
    """Raised when Databricks job concurrency limits prevent new runs."""


@dataclass(frozen=True)
class ProfilingTarget:
    table_group_id: str
    table_group_name: str | None
    connection_id: str
    connection_name: str | None
    catalog: str | None
    schema_name: str | None
    http_path: str | None
    project_key: str | None
    system_id: str | None
    profiling_job_id: int | None
    is_active: bool


@dataclass(frozen=True)
class ProfilingLaunchResult:
    table_group_id: str
    profile_run_id: str
    job_id: int
    databricks_run_id: int


@dataclass(frozen=True)
class PreparedProfileRun:
    target: ProfilingTarget
    profile_run_id: str
    callback_url: str | None


class DataQualityProfilingService:
    """Coordinates Databricks-managed profiling runs for TestGen table groups."""

    def __init__(
        self,
        client: TestGenClient,
        *,
        jobs_client: DatabricksJobsClient | None = None,
        workspace_client: DatabricksWorkspaceClient | None = None,
        settings: Settings | None = None,
    ) -> None:
        self._client = client
        self._settings = settings or get_settings()
        self._jobs_client = jobs_client
        self._workspace_client: DatabricksWorkspaceClient | None = workspace_client
        self._ingestion_params: DatabricksConnectionParams | None = None
        self._ingestion_params_loaded = False
        self._compute_mode: str | None = None

    def start_profile_for_table_group(
        self,
        table_group_id: str,
        *,
        callback_url_template: str | None = None,
    ) -> ProfilingLaunchResult:
        prepared = self.prepare_profile_run(
            table_group_id,
            callback_url_template=callback_url_template,
        )
        return self.launch_prepared_profile_run(prepared)

    def prepare_profile_run(
        self,
        table_group_id: str,
        *,
        callback_url_template: str | None = None,
    ) -> PreparedProfileRun:
        target = self._load_target(table_group_id)
        notebook_path = self._resolve_profile_notebook_path()
        if not notebook_path:
            raise ProfilingConfigurationError(
                "Databricks profiling notebook path is not configured. Update the Databricks settings before running profiling."
            )
        profile_run_id = self._client.start_profile_run(table_group_id)
        callback_url = self._build_callback_url(callback_url_template, profile_run_id)

        return PreparedProfileRun(
            target=target,
            profile_run_id=profile_run_id,
            callback_url=callback_url,
        )

    def launch_prepared_profile_run(self, prepared: PreparedProfileRun) -> ProfilingLaunchResult:
        job_id = self._ensure_job(prepared.target)

        try:
            run_handle = self._launch_run(
                prepared.target,
                job_id,
                prepared.profile_run_id,
                prepared.callback_url,
            )
            self._client.update_profile_run_databricks_run(
                prepared.profile_run_id,
                databricks_run_id=str(run_handle.run_id),
            )
        except ProfilingServiceError:
            self._mark_run_failed(prepared.profile_run_id)
            raise
        except DatabricksJobsError as exc:
            self._mark_run_failed(prepared.profile_run_id)
            raise self._translate_jobs_error(exc) from exc
        except TestGenClientError as exc:
            self._mark_run_failed(prepared.profile_run_id)
            raise ProfilingJobError(str(exc)) from exc

        return ProfilingLaunchResult(
            table_group_id=prepared.target.table_group_id,
            profile_run_id=prepared.profile_run_id,
            job_id=job_id,
            databricks_run_id=run_handle.run_id,
        )

    def _load_target(self, table_group_id: str) -> ProfilingTarget:
        details = self._client.get_table_group_details(table_group_id)
        if not details:
            raise ProfilingTargetNotFound(f"Table group '{table_group_id}' was not found.")

        is_active = bool(details.get("is_active", True))
        if not is_active:
            raise ProfilingConfigurationError("Cannot profile a table group whose connection is inactive.")

        profiling_job_id = self._coerce_int(details.get("profiling_job_id"))

        return ProfilingTarget(
            table_group_id=details.get("table_group_id", table_group_id),
            table_group_name=details.get("name"),
            connection_id=details.get("connection_id", ""),
            connection_name=details.get("connection_name"),
            catalog=details.get("catalog"),
            schema_name=details.get("schema_name"),
            http_path=details.get("http_path"),
            project_key=details.get("project_key"),
            system_id=details.get("system_id"),
            profiling_job_id=profiling_job_id,
            is_active=is_active,
        )

    def _ensure_job(self, target: ProfilingTarget) -> int:
        job_settings = self._build_job_settings(target)
        jobs_client = self._get_jobs_client()

        try:
            if target.profiling_job_id is None:
                response = jobs_client.create_job(job_settings)
                job_id = self._extract_job_id(response)
            else:
                job_id = target.profiling_job_id
                jobs_client.update_job(job_id, job_settings)
        except DatabricksJobsError as exc:
            job_id = self._retry_job_with_serverless(target, jobs_client, exc)

        self._client.update_table_group_profiling_job(target.table_group_id, str(job_id))
        return job_id

    def _launch_run(
        self,
        target: ProfilingTarget,
        job_id: int,
        profile_run_id: str,
        callback_url: str | None,
    ):
        params = self._build_notebook_params(
            target,
            profile_run_id,
            callback_url_override=callback_url,
        )
        return self._get_jobs_client().run_now(
            job_id,
            notebook_params=params,
            idempotency_token=profile_run_id,
        )

    def _build_job_settings(self, target: ProfilingTarget) -> Mapping[str, Any]:
        notebook_path = self._resolve_profile_notebook_path()
        if not notebook_path:
            raise ProfilingConfigurationError("Databricks profiling notebook path is not configured.")

        job_name_prefix = self._settings.databricks_profile_job_name_prefix or "ConversionCentral Profiling - "
        job_name = f"{job_name_prefix}{target.table_group_name or target.table_group_id}"

        cluster_payload = self._build_cluster_payload()

        task: dict[str, Any] = {
            "task_key": "profile",
            "notebook_task": {
                "notebook_path": notebook_path,
            },
        }
        task.update(cluster_payload)

        connection_uuid, data_object_uuid = parse_connection_id(target.connection_id)
        tags: dict[str, str] = {
            "app": "conversion_central",
            "component": "data_quality_profiling",
            "table_group_id": target.table_group_id,
        }
        if connection_uuid:
            tags["connection_uuid"] = connection_uuid
        if data_object_uuid:
            tags["data_object_uuid"] = data_object_uuid

        return {
            "name": job_name,
            "max_concurrent_runs": 1,
            "tags": tags,
            "tasks": [task],
        }

    def _build_cluster_payload(self) -> dict[str, Any]:
        if self._should_use_serverless_compute():
            payload = self._build_serverless_compute_payload()
            logger.info(
                "Profiling job will use serverless compute (workspace_compute=%s)",
                payload.get("compute", {}).get("workspace_compute"),
            )
            return payload

        classic_payload = self._build_classic_cluster_payload()
        if classic_payload:
            logger.info(
                "Profiling job will use classic compute (existing_cluster_id=%s, policy_id=%s)",
                classic_payload.get("existing_cluster_id"),
                classic_payload.get("new_cluster", {}).get("policy_id") if isinstance(classic_payload.get("new_cluster"), Mapping) else None,
            )
            return classic_payload

        logger.info(
            "Profiling cluster configuration not found; using serverless compute payload instead.",
        )
        self._force_serverless_compute()
        return self._build_serverless_compute_payload()

    def _build_classic_cluster_payload(self) -> dict[str, Any] | None:
        existing_cluster_id, policy_id = self._resolve_cluster_config()

        if existing_cluster_id:
            return {"existing_cluster_id": existing_cluster_id}
        if policy_id:
            return {"new_cluster": {"policy_id": policy_id}}
        return None

    def _build_serverless_compute_payload(self) -> dict[str, Any]:
        workspace_compute = self._resolve_workspace_compute_name()
        compute: dict[str, Any] = {"compute": {"compute_type": "SERVERLESS"}}
        if workspace_compute:
            compute["compute"]["workspace_compute"] = {"workspace_compute_name": workspace_compute}
        return compute

    def _build_notebook_params(
        self,
        target: ProfilingTarget,
        profile_run_id: str,
        *,
        callback_url_override: str | None = None,
    ) -> dict[str, str]:
        params: dict[str, str] = {
            "table_group_id": target.table_group_id,
            "profile_run_id": profile_run_id,
            "data_quality_schema": self._client.schema,
        }

        optional_pairs = {
            "connection_id": target.connection_id,
            "connection_name": target.connection_name,
            "catalog": target.catalog,
            "schema_name": target.schema_name,
            "http_path": target.http_path,
            "project_key": target.project_key,
        }
        for key, value in optional_pairs.items():
            if value:
                params[key] = str(value)

        callback_url = (callback_url_override or self._settings.databricks_profile_callback_url or "").strip()
        if callback_url:
            params["callback_url"] = callback_url
        callback_token = (self._settings.databricks_profile_callback_token or "").strip()
        if callback_token:
            params["callback_token"] = callback_token

        inline_payload = self._build_inline_payload_blob(target, profile_run_id)
        if inline_payload:
            params["profiling_payload_inline"] = inline_payload

        return params

    def _build_inline_payload_blob(self, target: ProfilingTarget, profile_run_id: str) -> str | None:
        payload = self._export_inline_payload(target, profile_run_id)
        if payload is None:
            return None
        try:
            serialized = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        except (TypeError, ValueError) as exc:
            logger.warning(
                "Profiling payload for table group %s could not be serialized: %s",
                target.table_group_id,
                exc,
            )
            return None

        try:
            compressed = gzip.compress(serialized)
        except OSError as exc:  # pragma: no cover - gzip failures are rare
            logger.warning("Unable to compress profiling payload for %s: %s", target.table_group_id, exc)
            return None

        encoded = base64.b64encode(compressed).decode("ascii")
        blob = f"base64:{encoded}"
        if len(blob) > INLINE_PAYLOAD_MAX_BYTES:
            logger.warning(
                "Profiling payload for %s exceeds inline limit (%s bytes); omitting inline payload.",
                target.table_group_id,
                INLINE_PAYLOAD_MAX_BYTES,
            )
            return None
        return blob

    def _export_inline_payload(self, target: ProfilingTarget, profile_run_id: str) -> Any:
        exporter = getattr(self._client, "export_profiling_payload", None)
        if not callable(exporter):
            return None

        try:
            try:
                return exporter(target.table_group_id, profile_run_id=profile_run_id)
            except TypeError:
                return exporter(target.table_group_id)
        except TestGenClientError as exc:
            logger.warning(
                "Profiling payload export failed for table group %s: %s",
                target.table_group_id,
                exc,
            )
            return None
        except Exception as exc:  # pragma: no cover - defensive guard
            logger.warning(
                "Unexpected error while exporting profiling payload for %s: %s",
                target.table_group_id,
                exc,
            )
            return None

    def _build_callback_url(self, template: str | None, profile_run_id: str) -> str | None:
        candidate = (template or "").strip()
        if not candidate:
            return None
        if CALLBACK_URL_PLACEHOLDER in candidate:
            return candidate.replace(CALLBACK_URL_PLACEHOLDER, profile_run_id)
        normalized = candidate.rstrip("/")
        if normalized.endswith("/complete"):
            return normalized
        return f"{normalized}/{profile_run_id}/complete"


    def _mark_run_failed(self, profile_run_id: str) -> None:
        try:
            self._client.complete_profile_run(profile_run_id, status="failed")
        except TestGenClientError as exc:  # pragma: no cover - defensive logging
            logger.warning("Failed to mark profile run %s as failed: %s", profile_run_id, exc)

    @staticmethod
    def _translate_jobs_error(exc: DatabricksJobsError) -> ProfilingJobError:
        message = str(exc)
        lowered = message.lower()
        if "active runs" in lowered and ("limit" in lowered or "429" in lowered):
            friendly = (
                "Another profiling run is already in progress for this job; wait for the current run to finish "
                "or increase the Databricks job concurrency limit."
            )
            return ProfilingConcurrencyLimitError(friendly)
        if "request timeout" in lowered or "timed out" in lowered:
            return ProfilingJobError("Databricks did not respond in time; please retry the profiling run.")
        return ProfilingJobError(message)

    def _get_jobs_client(self) -> DatabricksJobsClient:
        if self._jobs_client is None:
            config = self._build_jobs_config()
            try:
                self._jobs_client = DatabricksJobsClient(config=config)
            except ValueError as exc:  # Raised when host/token missing
                raise ProfilingConfigurationError(str(exc)) from exc
        return self._jobs_client

    def _build_jobs_config(self) -> DatabricksJobsConfig:
        host, token = self._resolve_workspace_credentials()
        if not host or not token:
            raise ProfilingConfigurationError(
                "Databricks workspace host and token are not configured. Update the Databricks settings before running profiling."
            )

        return DatabricksJobsConfig(host=host, token=token)

    def _resolve_profile_notebook_path(self) -> str | None:
        notebook_path = self._get_configured_notebook_path()
        if not notebook_path:
            return None
        if notebook_path.startswith("/"):
            return self._expand_workspace_notebook_path(notebook_path)
        return notebook_path

    def _resolve_cluster_config(self) -> tuple[str | None, str | None]:
        existing_cluster_id = (self._settings.databricks_profile_existing_cluster_id or "").strip() or None
        policy_id = (self._settings.databricks_profile_policy_id or "").strip() or None
        if not policy_id:
            params = self._get_ingestion_params()
            if params:
                candidate = getattr(params, "profiling_policy_id", None)
                if isinstance(candidate, str):
                    candidate = candidate.strip()
                if candidate:
                    policy_id = candidate
        return existing_cluster_id, policy_id

    def _get_ingestion_params(self) -> DatabricksConnectionParams | None:
        if not self._ingestion_params_loaded:
            try:
                self._ingestion_params = get_ingestion_connection_params()
            except RuntimeError:
                self._ingestion_params = None
            self._ingestion_params_loaded = True
        return self._ingestion_params

    def _resolve_workspace_credentials(self) -> tuple[str, str]:
        host = (self._settings.databricks_host or "").strip()
        token = (self._settings.databricks_token or "").strip()
        if host and token:
            return host, token

        params = self._get_ingestion_params()
        if params:
            if not host:
                host = (getattr(params, "workspace_host", "") or "").strip()
            if not token:
                token = (getattr(params, "access_token", "") or "").strip()
        return host, token

    def _should_use_serverless_compute(self) -> bool:
        return self._resolve_compute_mode() == "serverless"

    def _retry_job_with_serverless(
        self,
        target: ProfilingTarget,
        jobs_client: DatabricksJobsClient,
        original_error: DatabricksJobsError,
    ) -> int:
        if not self._should_force_serverless_retry(original_error):
            raise original_error

        logger.info(
            "Databricks workspace rejected classic clusters for profiling; recreating job %s with serverless compute",
            target.table_group_id,
        )
        self._force_serverless_compute()
        if target.profiling_job_id is not None:
            with suppress(DatabricksJobsError):
                jobs_client.delete_job(target.profiling_job_id)

        logger.info(
            "Submitting serverless profiling job definition for %s",
            target.table_group_id,
        )
        response = jobs_client.create_job(self._build_job_settings(target))
        return self._extract_job_id(response)

    def _force_serverless_compute(self) -> None:
        self._compute_mode = "serverless"

    def _should_force_serverless_retry(self, exc: DatabricksJobsError) -> bool:
        if self._should_use_serverless_compute():
            return False

        message = str(exc).lower()
        trigger_phrases = {
            "only serverless compute is supported",
            "serverless compute is supported",
        }
        return any(phrase in message for phrase in trigger_phrases)

    def _resolve_compute_mode(self) -> str:
        if self._compute_mode:
            return self._compute_mode

        mode: str | None = None
        params = self._get_ingestion_params()
        if params:
            spark_setting = getattr(params, "spark_compute", None)
            if isinstance(spark_setting, str):
                candidate = spark_setting.strip().lower()
                if candidate:
                    mode = candidate
        if not mode and isinstance(self._settings.databricks_spark_compute, str):
            candidate = self._settings.databricks_spark_compute.strip().lower()
            if candidate:
                mode = candidate

        if mode not in {"classic", "serverless"}:
            mode = "classic"

        self._compute_mode = mode
        return mode

    def _resolve_workspace_compute_name(self) -> str | None:
        params = self._get_ingestion_params()
        if not params:
            return None
        candidate = getattr(params, "warehouse_name", None)
        if isinstance(candidate, str):
            candidate = candidate.strip()
        return candidate or None

    def _get_workspace_client(self) -> DatabricksWorkspaceClient:
        if self._workspace_client is None:
            host, token = self._resolve_workspace_credentials()
            if not host or not token:
                raise ProfilingConfigurationError(
                    "Databricks workspace host and token are not configured. Update the Databricks settings before running profiling."
                )
            config = DatabricksWorkspaceConfig(host=host, token=token)
            try:
                self._workspace_client = DatabricksWorkspaceClient(config=config)
            except ValueError as exc:
                raise ProfilingConfigurationError(str(exc)) from exc
        return self._workspace_client

    def _get_configured_notebook_path(self) -> str | None:
        notebook_path = (self._settings.databricks_profile_notebook_path or "").strip()
        if notebook_path:
            return notebook_path
        params = self._get_ingestion_params()
        if not params:
            return None
        candidate = getattr(params, "profiling_notebook_path", None)
        if isinstance(candidate, str):
            candidate = candidate.strip()
        return candidate or None

    def _expand_workspace_notebook_path(self, notebook_path: str) -> str:
        status = self._workspace_get_status(notebook_path)
        if not status:
            raise ProfilingConfigurationError(
                f"Databricks workspace object '{notebook_path}' was not found."
            )
        object_type = str(status.get("object_type") or "").upper()
        if object_type in {"NOTEBOOK", "FILE"}:
            return notebook_path
        if object_type == "DIRECTORY":
            selection = self._select_notebook_from_directory(notebook_path)
            if selection:
                return selection
            raise ProfilingConfigurationError(
                f"Databricks folder '{notebook_path}' does not contain a supported notebook."
            )
        raise ProfilingConfigurationError(
            f"Databricks workspace object '{notebook_path}' is a {object_type.lower() or 'non-notebook'} and cannot be executed."
        )

    def _select_notebook_from_directory(self, directory_path: str) -> str | None:
        entries = self._workspace_list_directory(directory_path)
        notebooks: list[str] = []
        for entry in entries:
            if not isinstance(entry, Mapping):
                continue
            object_type = str(entry.get("object_type") or "").upper()
            if object_type != "NOTEBOOK":
                continue
            path = entry.get("path")
            if isinstance(path, str) and path:
                notebooks.append(path)
        if not notebooks:
            return None
        return self._choose_notebook_path(notebooks)

    def _choose_notebook_path(self, notebooks: Sequence[str]) -> str:
        normalized = {self._normalize_notebook_name(path): path for path in notebooks}
        for candidate in NOTEBOOK_CANDIDATE_BASENAMES:
            match = normalized.get(candidate)
            if match:
                return match
        return sorted(notebooks)[0]

    @staticmethod
    def _normalize_notebook_name(path: str) -> str:
        name = path.rsplit("/", 1)[-1]
        base = name.split(".", 1)[0]
        return base.lower()

    def _workspace_get_status(self, path: str) -> Mapping[str, Any] | None:
        client = self._get_workspace_client()
        try:
            return client.get_workspace_status(path)
        except (ValueError, DatabricksWorkspaceClientError) as exc:
            raise ProfilingConfigurationError(f"Databricks workspace lookup for '{path}' failed: {exc}") from exc

    def _workspace_list_directory(self, path: str) -> Sequence[Mapping[str, Any]]:
        client = self._get_workspace_client()
        try:
            return client.list_directory(path)
        except (ValueError, DatabricksWorkspaceClientError) as exc:
            raise ProfilingConfigurationError(f"Databricks workspace listing for '{path}' failed: {exc}") from exc

    @staticmethod
    def _extract_job_id(response: Mapping[str, Any] | None) -> int:
        if not isinstance(response, Mapping):
            raise ProfilingJobError("Databricks response did not include job metadata.")
        job_id = response.get("job_id")
        if not isinstance(job_id, int):
            raise ProfilingJobError("Databricks response is missing a numeric job_id.")
        return job_id

    @staticmethod
    def _coerce_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(str(value).strip())
        except (ValueError, TypeError):
            return None