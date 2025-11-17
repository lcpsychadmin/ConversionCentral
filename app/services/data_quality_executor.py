"""Execute DataOps TestGen validation runs and persist results."""

from __future__ import annotations

import json
import logging
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Optional

from app.ingestion.engine import get_ingestion_connection_params
from app.services.data_quality_notifications import DataQualityNotificationService, RunNotification
from app.services.data_quality_testgen import AlertRecord, TestGenClient, TestGenClientError, TestResultRecord

logger = logging.getLogger(__name__)

CommandRunner = Callable[[str, Optional[str]], dict[str, Any]]
ClientFactory = Callable[[object, str], TestGenClient]


@dataclass(frozen=True)
class ParsedRunResult:
    status: str
    failed_tests: int
    total_tests: int | None
    duration_ms: int | None
    results: tuple[TestResultRecord, ...]


class DataQualityRunExecutor:
    """Encapsulate the lifecycle for executing a TestGen validation run."""

    def __init__(
        self,
        *,
        params_resolver: Callable[[], object] | None = None,
        client_factory: ClientFactory | None = None,
        command_runner: CommandRunner | None = None,
        notification_service: DataQualityNotificationService | None = None,
    ) -> None:
        self._params_resolver = params_resolver or get_ingestion_connection_params
        self._client_factory = client_factory or (lambda params, schema: TestGenClient(params, schema=schema))
        self._command_runner = command_runner or _run_testgen_command
        self._notification_service = notification_service or DataQualityNotificationService()

    def execute(
        self,
        project_key: str,
        *,
        test_suite_key: str | None = None,
        trigger_source: str | None = None,
        start_status: str = "running",
    ) -> str | None:
        """Execute the TestGen CLI for the given project and persist the outcome."""

        try:
            params = self._params_resolver()
        except RuntimeError as exc:
            logger.info("Skipping data quality validation run for %s: %s", project_key, exc)
            return None
        except Exception as exc:  # pragma: no cover - defensive guard
            logger.exception("Failed to resolve Databricks params for data quality run %s", project_key)
            return None

        schema = (getattr(params, "data_quality_schema", "") or "").strip()
        if not schema:
            logger.info("Skipping data quality validation run for %s: no data quality schema configured.", project_key)
            return None

        client: TestGenClient | None = None
        run_id: str | None = None
        started_at = datetime.now(timezone.utc)

        try:
            client = self._client_factory(params, schema)
        except ValueError as exc:
            logger.info("Skipping data quality validation run for %s: %s", project_key, exc)
            return None
        except TestGenClientError as exc:  # pragma: no cover - defensive guard
            logger.warning("Unable to initialize TestGen client for %s: %s", project_key, exc)
            return None

        try:
            run_id = client.start_test_run(
                project_key=project_key,
                test_suite_key=test_suite_key,
                total_tests=None,
                trigger_source=trigger_source,
                status=start_status,
                started_at=started_at,
            )

            raw_result = self._command_runner(project_key, test_suite_key)
            parsed = _parse_run_result(raw_result)

            if parsed.results:
                client.record_test_results(run_id, parsed.results)

            final_status = parsed.status
            if final_status not in {"completed", "failed"}:
                final_status = "completed" if parsed.failed_tests == 0 else "failed"

            completed_at = datetime.now(timezone.utc)
            client.complete_test_run(
                run_id,
                status=final_status,
                failed_tests=parsed.failed_tests,
                duration_ms=parsed.duration_ms,
            )
            self._sync_alerts(
                client,
                project_key,
                test_suite_key,
                run_id,
                parsed,
            )
            self._emit_notification(
                run_id,
                project_key,
                parsed,
                trigger_source,
                started_at,
                completed_at,
            )
            return run_id
        except Exception as exc:
            logger.warning("Data quality validation run failed for %s: %s", project_key, exc)
            if run_id is not None:
                try:
                    client.complete_test_run(run_id, status="failed", failed_tests=None, duration_ms=None)
                except Exception:  # pragma: no cover - defensive guard
                    logger.exception("Unable to mark data quality run %s as failed", run_id)
                failure_parsed = ParsedRunResult(
                    status="failed",
                    failed_tests=1,
                    total_tests=None,
                    duration_ms=None,
                    results=tuple(),
                )
                self._sync_alerts(
                    client,
                    project_key,
                    test_suite_key,
                    run_id,
                    failure_parsed,
                )
                self._emit_notification(
                    run_id,
                    project_key,
                    failure_parsed,
                    trigger_source,
                    started_at,
                    datetime.now(timezone.utc),
                )
            return run_id
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:  # pragma: no cover - defensive guard
                    logger.debug("Ignoring error while closing TestGen client", exc_info=True)

    def _sync_alerts(
        self,
        client: TestGenClient,
        project_key: str,
        test_suite_key: str | None,
        run_id: str,
        parsed: ParsedRunResult,
    ) -> None:
        source_ref = _build_alert_source_ref(project_key, test_suite_key)
        try:
            client.delete_alert_by_source(source_type="test_run", source_ref=source_ref)
            if parsed.failed_tests > 0:
                details = _build_alert_details(project_key, test_suite_key, run_id, parsed)
                severity = "high" if parsed.failed_tests else "medium"
                alert = AlertRecord(
                    source_type="test_run",
                    source_ref=source_ref,
                    severity=severity,
                    title=_build_alert_title(project_key, test_suite_key),
                    details=details,
                )
                client.create_alert(alert)
        except TestGenClientError as exc:
            logger.warning(
                "Unable to synchronize data quality alerts for %s: %s",
                project_key,
                exc,
            )
        except Exception:  # pragma: no cover - defensive guard
            logger.exception("Unexpected error while synchronizing data quality alerts")

    def _emit_notification(
        self,
        run_id: str,
        project_key: str,
        parsed: ParsedRunResult,
        trigger_source: str | None,
        started_at: datetime,
        completed_at: datetime,
    ) -> None:
        if parsed.failed_tests <= 0 and not _has_failing_results(parsed.results):
            return

        results_payload = tuple(
            {
                "test_id": result.test_id,
                "table_name": result.table_name,
                "column_name": result.column_name,
                "status": result.result_status,
                "expected_value": result.expected_value,
                "actual_value": result.actual_value,
                "message": result.message,
                "detected_at": result.detected_at.isoformat() if result.detected_at else None,
            }
            for result in parsed.results
        )

        notification = RunNotification(
            run_id=run_id,
            project_key=project_key,
            status=parsed.status,
            failed_tests=parsed.failed_tests,
            total_tests=parsed.total_tests,
            duration_ms=parsed.duration_ms,
            trigger_source=trigger_source,
            started_at=started_at,
            completed_at=completed_at,
            results=results_payload,
        )
        self._notification_service.send(notification)


def _parse_run_result(payload: dict[str, Any] | None) -> ParsedRunResult:
    if payload is None:
        return ParsedRunResult("failed", failed_tests=0, total_tests=None, duration_ms=None, results=tuple())

    status_raw = str(payload.get("status", "")).strip().lower()
    duration_ms = payload.get("duration_ms")
    try:
        duration_ms = int(duration_ms) if duration_ms is not None else None
    except (TypeError, ValueError):
        duration_ms = None

    results_data = payload.get("results") or []
    results: list[TestResultRecord] = []
    for item in results_data:
        if not isinstance(item, dict):
            continue
        detected_at = _parse_timestamp(item.get("detected_at"))
        results.append(
            TestResultRecord(
                test_id=str(item.get("test_id") or item.get("id") or ""),
                table_name=item.get("table_name"),
                column_name=item.get("column_name"),
                result_status=str(item.get("status", "unknown")),
                expected_value=_safe_str(item.get("expected_value")),
                actual_value=_safe_str(item.get("actual_value")),
                message=_safe_str(item.get("message")),
                detected_at=detected_at,
            )
        )

    total_tests = payload.get("total_tests")
    try:
        total_tests = int(total_tests) if total_tests is not None else None
    except (TypeError, ValueError):
        total_tests = None

    failed_tests = payload.get("failed_tests")
    try:
        failed_tests = int(failed_tests) if failed_tests is not None else None
    except (TypeError, ValueError):
        failed_tests = None

    if failed_tests is None:
        failed_tests = sum(1 for result in results if str(result.result_status).lower() not in {"passed", "success"})

    if total_tests is None:
        total_tests = len(results) if results else None

    if status_raw in {"failed", "error"}:
        status = "failed"
    elif status_raw in {"completed", "passed", "success"}:
        status = "completed"
    else:
        status = "completed" if failed_tests == 0 else "failed"

    return ParsedRunResult(status=status, failed_tests=failed_tests, total_tests=total_tests, duration_ms=duration_ms, results=tuple(results))


def _parse_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value, tz=timezone.utc)
        except (OSError, OverflowError):
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _run_testgen_command(project_key: str, test_suite_key: str | None) -> dict[str, Any]:
    """Invoke the TestGen CLI and parse the JSON response."""

    command = ["testgen", "run", "--project", project_key, "--format", "json"]
    if test_suite_key:
        command += ["--suite", test_suite_key]

    logger.debug("Executing TestGen CLI: %s", " ".join(command))
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        stderr = completed.stderr.strip()
        raise RuntimeError(f"TestGen CLI failed (exit {completed.returncode}): {stderr or 'no error output'}")

    stdout = completed.stdout.strip()
    if not stdout:
        return {}

    try:
        return json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Unable to parse TestGen CLI output: {exc}") from exc


def _has_failing_results(results: Iterable[TestResultRecord]) -> bool:
    for result in results:
        if str(result.result_status).lower() not in {"passed", "success"}:
            return True
    return False


def _build_alert_source_ref(project_key: str, test_suite_key: str | None) -> str:
    suite_ref = test_suite_key if test_suite_key is not None else "__default__"
    return f"{project_key}:{suite_ref}"


def _build_alert_title(project_key: str, test_suite_key: str | None) -> str:
    suite_part = f" suite '{test_suite_key}'" if test_suite_key else ""
    return f"Data quality tests failed for project '{project_key}'{suite_part}".strip()


def _build_alert_details(
    project_key: str,
    test_suite_key: str | None,
    run_id: str,
    parsed: ParsedRunResult,
) -> str:
    suite_part = test_suite_key or "default"
    total_repr = parsed.total_tests if parsed.total_tests is not None else "unknown"
    lines = (
        f"Project: {project_key}",
        f"Suite: {suite_part}",
        f"Run ID: {run_id}",
        f"Status: {parsed.status}",
        f"Failed tests: {parsed.failed_tests}",
        f"Total tests: {total_repr}",
    )
    return "\n".join(lines)


__all__ = ["DataQualityRunExecutor", "ParsedRunResult"]
