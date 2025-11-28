from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Callable, Generator
from uuid import UUID

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session, joinedload
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.database import SessionLocal
from app.ingestion.engine import get_ingestion_connection_params
from app.models import DataQualityProfilingSchedule, SystemConnection
from app.services.data_quality_profiling import (
    DataQualityProfilingService,
    ProfilingConfigurationError,
    ProfilingServiceError,
)
from app.services.data_quality_testgen import TestGenClient, TestGenClientError

logger = logging.getLogger(__name__)

RUN_STATUS_SUBMITTING = "submitting"
RUN_STATUS_SUBMITTED = "submitted"
RUN_STATUS_FAILED = "failed"


class ScheduledProfilingEngine:
    def __init__(self, session_factory: Callable[[], Session] = SessionLocal) -> None:
        self._session_factory = session_factory
        self._scheduler: AsyncIOScheduler | None = None

    def start(self) -> None:
        if self._scheduler is not None and self._scheduler.running:
            return
        self._scheduler = AsyncIOScheduler()
        self._scheduler.start()
        self.reload_jobs()

    def shutdown(self) -> None:
        if self._scheduler is not None:
            self._scheduler.shutdown(wait=False)
            self._scheduler = None

    def reload_jobs(self) -> None:
        scheduler = self._scheduler
        if scheduler is None:
            return
        logger.debug("Reloading profiling schedules")
        for job in scheduler.get_jobs():
            scheduler.remove_job(job.id)

        with self._session_scope() as session:
            schedules = (
                session.query(DataQualityProfilingSchedule)
                .filter(DataQualityProfilingSchedule.is_active.is_(True))
                .all()
            )

        for schedule in schedules:
            try:
                trigger = self._build_trigger(schedule.schedule_expression, schedule.timezone)
            except ValueError as exc:
                logger.warning(
                    "Skipping profiling schedule %s due to invalid cron expression: %s",
                    schedule.id,
                    exc,
                )
                continue
            scheduler.add_job(
                self.run_schedule,
                trigger=trigger,
                args=[str(schedule.id)],
                id=str(schedule.id),
                replace_existing=True,
            )

    def trigger_now(self, schedule_id: UUID) -> None:
        self.run_schedule(str(schedule_id))

    def run_schedule(self, schedule_id: str) -> None:
        schedule_uuid = UUID(schedule_id)
        logger.info("Launching profiling run for schedule %s", schedule_uuid)
        with self._session_scope() as session:
            schedule = (
                session.query(DataQualityProfilingSchedule)
                .options(
                    joinedload(DataQualityProfilingSchedule.connection).joinedload(SystemConnection.system),
                    joinedload(DataQualityProfilingSchedule.data_object),
                )
                .get(schedule_uuid)
            )
            if schedule is None:
                logger.info("Profiling schedule %s no longer exists", schedule_uuid)
                return
            if not schedule.is_active:
                logger.info("Profiling schedule %s is inactive; skipping", schedule_uuid)
                return

            schedule.last_run_started_at = datetime.now(timezone.utc)
            schedule.last_run_completed_at = None
            schedule.last_run_error = None
            schedule.last_run_status = RUN_STATUS_SUBMITTING
            session.add(schedule)

            try:
                client = self._create_testgen_client()
            except (ValueError, ProfilingConfigurationError) as exc:
                logger.warning("Unable to initialize TestGen client for profiling schedule %s: %s", schedule_uuid, exc)
                schedule.last_run_status = RUN_STATUS_FAILED
                schedule.last_run_completed_at = datetime.now(timezone.utc)
                schedule.last_run_error = str(exc)
                session.add(schedule)
                return

            result = None
            try:
                service = DataQualityProfilingService(client)
                result = service.start_profile_for_table_group(schedule.table_group_id)
            except (ProfilingServiceError, TestGenClientError, ValueError) as exc:
                logger.exception(
                    "Profiling run submission failed for schedule %s (table_group=%s)",
                    schedule_uuid,
                    schedule.table_group_id,
                )
                schedule.last_run_status = RUN_STATUS_FAILED
                schedule.last_run_completed_at = datetime.now(timezone.utc)
                schedule.last_run_error = str(exc)
            finally:
                client.close()

            if schedule.last_run_status != RUN_STATUS_FAILED and result is not None:
                schedule.total_runs += 1
                schedule.last_profile_run_id = result.profile_run_id
                schedule.last_run_status = RUN_STATUS_SUBMITTED
            session.add(schedule)

    def _build_trigger(self, expression: str, tz_name: str | None) -> CronTrigger:
        tz = timezone.utc
        if tz_name:
            try:
                tz = ZoneInfo(tz_name)
            except ZoneInfoNotFoundError:
                logger.warning("Unknown timezone '%s' for profiling schedule; defaulting to UTC", tz_name)
        return CronTrigger.from_crontab(expression, timezone=tz)

    def _create_testgen_client(self) -> TestGenClient:
        params = get_ingestion_connection_params()
        schema = getattr(params, "data_quality_schema", None)
        if not schema:
            raise ProfilingConfigurationError(
                "Data quality schema is not configured; unable to start profiling schedule."
            )
        return TestGenClient(params, schema=schema)

    @contextmanager
    def _session_scope(self) -> Generator[Session, None, None]:
        session = self._session_factory()
        session.expire_on_commit = False
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


scheduled_profiling_engine = ScheduledProfilingEngine()
