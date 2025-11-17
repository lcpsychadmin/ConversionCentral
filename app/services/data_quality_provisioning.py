"""Background provisioning job for Data Quality metadata."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from typing import Callable, Optional

from app.ingestion.engine import get_ingestion_connection_params
from app.services.data_quality_metadata import ensure_data_quality_metadata

logger = logging.getLogger(__name__)

ParamsResolver = Callable[[], object]
MetadataProvisioner = Callable[[object], None]


class DataQualityProvisioner:
    """Schedules background refreshes for the TestGen metadata schema."""

    def __init__(
        self,
        *,
        params_resolver: Optional[ParamsResolver] = None,
        metadata_fn: Optional[MetadataProvisioner] = None,
        max_workers: int = 1,
    ) -> None:
        self._params_resolver = params_resolver or get_ingestion_connection_params
        self._metadata_fn = metadata_fn or ensure_data_quality_metadata
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="dq-provisioner")
        self._lock = Lock()
        self._running = False
        self._queued = False
        self._shutdown = False
        self._last_reason: Optional[str] = None

    def trigger(self, *, reason: Optional[str] = None, wait: bool = False) -> None:
        """Queue a metadata refresh, optionally running synchronously."""

        reason = (reason or "").strip() or None
        with self._lock:
            if self._shutdown:
                logger.debug("Data quality provisioner shutting down; ignoring trigger (%s)", reason)
                return
            if reason:
                self._last_reason = reason
            if self._running:
                self._queued = True
                return
            self._running = True

        if wait:
            self._run_loop()
        else:
            self._executor.submit(self._run_loop)

    def shutdown(self) -> None:
        with self._lock:
            self._shutdown = True
        self._executor.shutdown(wait=False)

    def _run_loop(self) -> None:
        while True:
            reason = self._consume_reason()
            self._run_once(reason)
            with self._lock:
                if self._queued:
                    self._queued = False
                    continue
                self._running = False
                break

    def _consume_reason(self) -> Optional[str]:
        with self._lock:
            reason = self._last_reason
            self._last_reason = None
            return reason

    def _run_once(self, reason: Optional[str]) -> None:
        label = reason or "unspecified trigger"
        try:
            params = self._params_resolver()
        except RuntimeError as exc:
            logger.info("Skipping data quality provisioning (%s): %s", label, exc)
            return
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception("Failed to resolve Databricks params for data quality provisioning (%s): %s", label, exc)
            return

        logger.debug("Running data quality metadata provisioning (%s)", label)
        try:
            self._metadata_fn(params)
        except Exception:  # pragma: no cover - defensive logging
            logger.exception("Data quality metadata provisioning failed (%s)", label)


data_quality_provisioner = DataQualityProvisioner()


def trigger_data_quality_provisioning(*, reason: Optional[str] = None, wait: bool = False) -> None:
    """Public helper to queue or run a metadata provisioning cycle."""

    data_quality_provisioner.trigger(reason=reason, wait=wait)


__all__ = [
    "DataQualityProvisioner",
    "data_quality_provisioner",
    "trigger_data_quality_provisioning",
]
