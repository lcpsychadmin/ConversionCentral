"""Utilities to queue and execute TestGen validation runs."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from app.services.data_quality_executor import DataQualityRunExecutor

logger = logging.getLogger(__name__)

_RUNNER_POOL = ThreadPoolExecutor(max_workers=2, thread_name_prefix="dq-run")


def queue_validation_run(
    project_key: str,
    *,
    test_suite_key: Optional[str] = None,
    trigger_source: Optional[str] = None,
    status: str = "running",
    wait: bool = False,
) -> Optional[str]:
    """Queue a TestGen validation run and optionally wait for completion."""

    executor = DataQualityRunExecutor()

    if wait:
        return executor.execute(
            project_key,
            test_suite_key=test_suite_key,
            trigger_source=trigger_source,
            start_status=status,
        )

    _RUNNER_POOL.submit(
        executor.execute,
        project_key,
        test_suite_key=test_suite_key,
        trigger_source=trigger_source,
        start_status=status,
    )
    return None


__all__ = ["queue_validation_run"]
