"""Dispatch data quality run notifications to external webhooks."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Mapping

from app.config import get_settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunNotification:
    run_id: str
    project_key: str
    status: str
    failed_tests: int | None
    total_tests: int | None
    duration_ms: int | None
    trigger_source: str | None
    started_at: datetime | None
    completed_at: datetime | None
    results: Iterable[Mapping[str, Any]]


class DataQualityNotificationService:
    """Post run notifications to the configured webhook endpoint, if available."""

    def __init__(self, *, settings_provider=get_settings, request_client=None) -> None:
        self._settings_provider = settings_provider
        self._request_client = request_client

    def send(self, payload: RunNotification) -> None:
        settings = self._settings_provider()
        webhook_url = getattr(settings, "data_quality_notification_webhook_url", None)
        if not webhook_url:
            return

        body = self._build_payload(payload)

        try:
            client = self._request_client
            if client is None:
                import requests  # type: ignore

                response = requests.post(
                    webhook_url,
                    headers={"Content-Type": "application/json"},
                    data=json.dumps(body),
                    timeout=15,
                )
            else:
                response = client(
                    webhook_url,
                    body,
                )
        except ModuleNotFoundError as exc:  # pragma: no cover - defensive guard
            logger.warning("Cannot send data quality notification, requests missing: %s", exc)
            return
        except Exception as exc:
            logger.warning("Failed to deliver data quality notification for %s: %s", payload.run_id, exc)
            return

        status_code = getattr(response, "status_code", None)
        if status_code is not None and status_code >= 400:
            logger.warning(
                "Data quality webhook responded with %s for run %s",
                status_code,
                payload.run_id,
            )

    def _build_payload(self, payload: RunNotification) -> dict[str, Any]:
        results = [dict(result) for result in payload.results]
        return {
            "run_id": payload.run_id,
            "project_key": payload.project_key,
            "status": payload.status,
            "failed_tests": payload.failed_tests,
            "total_tests": payload.total_tests,
            "duration_ms": payload.duration_ms,
            "trigger_source": payload.trigger_source,
            "started_at": payload.started_at.isoformat() if payload.started_at else None,
            "completed_at": payload.completed_at.isoformat() if payload.completed_at else None,
            "results": results,
        }


__all__ = ["DataQualityNotificationService", "RunNotification"]
