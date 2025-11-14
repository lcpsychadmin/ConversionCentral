from __future__ import annotations

import logging
import re
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any
from uuid import UUID

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import MetaData, Table as SqlTable, select, func
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, joinedload
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.config import get_settings
from app.database import SessionLocal
from app.models import (
    ConnectionTableSelection,
    IngestionRun,
    IngestionSchedule,
    SystemConnection,
)
from app.schemas import IngestionLoadStrategy, IngestionRunStatus, SystemConnectionType
from app.ingestion.engine import get_ingestion_connection_params
from app.services.connection_resolver import UnsupportedConnectionError, resolve_sqlalchemy_url
from app.services.ingestion_loader import (
    BaseTableLoader,
    DatabricksTableLoader,
    SparkTableLoader,
    build_loader_plan,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ScheduleSnapshot:
    id: UUID
    load_strategy: IngestionLoadStrategy
    watermark_column: str | None
    primary_key_column: str | None
    batch_size: int
    target_schema: str | None
    target_table_name: str
    connection_string: str
    connection_type: SystemConnectionType
    source_schema: str | None
    source_table: str
    last_watermark_timestamp: datetime | None
    last_watermark_id: int | None


@dataclass(frozen=True)
class IngestionOutcome:
    rows_loaded: int
    rows_expected: int | None
    watermark_timestamp: datetime | None
    watermark_id: int | None
    query_text: str


class RunAbortRequested(Exception):
    """Raised when an ingestion run has been aborted externally."""


class ScheduledIngestionEngine:
    """Coordinates cron-based ingestion runs."""

    def __init__(
        self,
        session_factory: Callable[[], Session] = SessionLocal,
        *,
        loader: BaseTableLoader | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._loader = loader
        self._loader_method: str | None = None
        if loader is not None:
            self._loader_method = "spark" if isinstance(loader, SparkTableLoader) else "sql"
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
        logger.debug("Reloading scheduled ingestion jobs")
        for job in scheduler.get_jobs():
            scheduler.remove_job(job.id)

        with self._session_scope() as session:
            schedules = (
                session.query(IngestionSchedule)
                .options(
                    joinedload(IngestionSchedule.table_selection).joinedload(
                        ConnectionTableSelection.system_connection
                    ).joinedload(SystemConnection.system)
                )
                .filter(IngestionSchedule.is_active.is_(True))
                .all()
            )

        for schedule in schedules:
            connection = schedule.table_selection.system_connection
            if not connection.ingestion_enabled:
                continue
            trigger = self._build_trigger(schedule.schedule_expression, schedule.timezone)
            logger.debug("Scheduling job %s with expression %s", schedule.id, schedule.schedule_expression)
            scheduler.add_job(
                self.run_schedule,
                trigger=trigger,
                args=[str(schedule.id)],
                id=str(schedule.id),
                replace_existing=True,
            )

    def run_schedule(self, schedule_id: str) -> None:
        schedule_uuid = UUID(schedule_id)
        logger.info("Starting ingestion run for schedule %s", schedule_uuid)
        expired = self._expire_stale_runs()
        if expired:
            logger.warning("Watchdog marked %s stuck ingestion run(s) as failed before starting new run", expired)
        with self._session_scope() as session:
            schedule = (
                session.query(IngestionSchedule)
                .options(
                    joinedload(IngestionSchedule.table_selection).joinedload(
                        ConnectionTableSelection.system_connection
                    ).joinedload(SystemConnection.system)
                )
                .get(schedule_uuid)
            )
            if schedule is None or not schedule.is_active:
                logger.info("Skipping schedule %s because it is missing or inactive", schedule_uuid)
                return
            if not schedule.table_selection.system_connection.ingestion_enabled:
                logger.info("Skipping schedule %s because its connection is disabled", schedule_uuid)
                return

            snapshot = self._snapshot(schedule)
            run = IngestionRun(
                ingestion_schedule_id=schedule.id,
                status=IngestionRunStatus.RUNNING.value,
                started_at=datetime.now(timezone.utc),
                rows_loaded=0,
                watermark_timestamp_before=schedule.last_watermark_timestamp,
                watermark_id_before=schedule.last_watermark_id,
            )
            schedule.last_run_started_at = run.started_at
            schedule.last_run_status = IngestionRunStatus.RUNNING.value
            session.add(run)
            session.flush()
            run_id = run.id

        try:
            outcome = self._execute(snapshot, run_id)
        except RunAbortRequested:
            logger.info(
                "Ingestion run %s for schedule %s was aborted; halting execution",
                run_id,
                schedule_uuid,
            )
            return
        except Exception as exc:  # noqa: B902
            self._mark_run_failed(schedule_uuid, run_id, exc)
            logger.exception("Ingestion run %s for schedule %s failed", run_id, schedule_uuid)
            raise
        else:
            self._mark_run_completed(schedule_uuid, run_id, outcome)
            expected_display = outcome.rows_expected if outcome.rows_expected is not None else "unknown"
            logger.info(
                "Completed ingestion run %s for schedule %s: %s rows loaded (expected=%s)",
                run_id,
                schedule_uuid,
                outcome.rows_loaded,
                expected_display,
            )

    def trigger_now(self, schedule_id: UUID) -> None:
        self.run_schedule(str(schedule_id))

    def _execute(self, snapshot: ScheduleSnapshot, run_id: UUID) -> IngestionOutcome:
        source_url = resolve_sqlalchemy_url(snapshot.connection_type, snapshot.connection_string)
        engine: Engine | None = None
        rows_loaded = 0
        latest_timestamp = snapshot.last_watermark_timestamp
        latest_id = snapshot.last_watermark_id
        query_text = ""
        expected_rows: int | None = None
        logger.info(
            "Executing ingestion snapshot for schedule %s (strategy=%s, batch_size=%s)",
            snapshot.id,
            snapshot.load_strategy.value,
            snapshot.batch_size,
        )
        try:
            from sqlalchemy import create_engine

            engine = create_engine(source_url, future=True, pool_pre_ping=True)
            metadata = MetaData()
            source_table = SqlTable(
                snapshot.source_table,
                metadata,
                schema=snapshot.source_schema,
                autoload_with=engine,
            )
            stmt = select(source_table)
            if snapshot.load_strategy is IngestionLoadStrategy.TIMESTAMP and snapshot.watermark_column:
                column = source_table.c.get(snapshot.watermark_column)
                if column is None:
                    raise RuntimeError(f"Column '{snapshot.watermark_column}' not found for incremental load.")
                if snapshot.last_watermark_timestamp is not None:
                    stmt = stmt.where(column > snapshot.last_watermark_timestamp)
                stmt = stmt.order_by(column)
            elif snapshot.load_strategy is IngestionLoadStrategy.NUMERIC_KEY and snapshot.primary_key_column:
                column = source_table.c.get(snapshot.primary_key_column)
                if column is None:
                    raise RuntimeError(f"Column '{snapshot.primary_key_column}' not found for incremental load.")
                if snapshot.last_watermark_id is not None:
                    stmt = stmt.where(column > snapshot.last_watermark_id)
                stmt = stmt.order_by(column)

            query_text = str(stmt.compile(engine, compile_kwargs={"literal_binds": True}))
            logger.debug("Compiled source query for schedule %s: %s", snapshot.id, query_text)
            batch: list[dict[str, Any]] = []
            replace_flag = snapshot.load_strategy is IngestionLoadStrategy.FULL
            dedupe_flag = snapshot.load_strategy is IngestionLoadStrategy.FULL
            with engine.connect() as connection:
                logger.debug("Streaming source rows for schedule %s", snapshot.id)
                try:
                    count_stmt = stmt.with_only_columns(func.count()).order_by(None)
                    expected_value = connection.execute(count_stmt).scalar_one()
                    expected_rows = int(expected_value or 0)
                except SQLAlchemyError:
                    logger.warning(
                        "Unable to compute expected row count for schedule %s; continuing without progress target",
                        snapshot.id,
                    )
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "Unexpected error computing expected row count for schedule %s",
                        snapshot.id,
                    )
                else:
                    self._update_run_progress(run_id, rows_expected=expected_rows)

                result = connection.execution_options(stream_results=True).execute(stmt)
                check_interval = max(50, snapshot.batch_size // 2)
                processed_since_check = 0
                for row in result.mappings():
                    row_dict = dict(row)
                    batch.append(row_dict)
                    latest_timestamp = self._compute_timestamp_watermark(snapshot, row_dict, latest_timestamp)
                    latest_id = self._compute_id_watermark(snapshot, row_dict, latest_id)
                    processed_since_check += 1
                    if processed_since_check >= check_interval:
                        processed_since_check = 0
                        if not self._is_run_active(run_id):
                            raise RunAbortRequested
                    if len(batch) >= snapshot.batch_size:
                        if not self._is_run_active(run_id):
                            raise RunAbortRequested
                        rows_loaded += self._flush_batch(snapshot, batch, replace_flag, dedupe_flag, source_table)
                        logger.debug(
                            "Flushed batch of %s rows for schedule %s (total=%s)",
                            snapshot.batch_size,
                            snapshot.id,
                            rows_loaded,
                        )
                        self._update_run_progress(run_id, rows_loaded=rows_loaded)
                        replace_flag = False
                        batch.clear()
                if batch:
                    if not self._is_run_active(run_id):
                        raise RunAbortRequested
                    rows_loaded += self._flush_batch(snapshot, batch, replace_flag, dedupe_flag, source_table)
                    logger.debug(
                        "Flushed final batch of %s rows for schedule %s (total=%s)",
                        len(batch),
                        snapshot.id,
                        rows_loaded,
                    )
                    self._update_run_progress(run_id, rows_loaded=rows_loaded)
        finally:
            if engine is not None:
                engine.dispose()

        if not self._is_run_active(run_id):
            raise RunAbortRequested

        return IngestionOutcome(
            rows_loaded=rows_loaded,
            rows_expected=expected_rows,
            watermark_timestamp=latest_timestamp,
            watermark_id=latest_id,
            query_text=query_text,
        )

    def _flush_batch(
        self,
        snapshot: ScheduleSnapshot,
        batch: list[dict[str, Any]],
        replace_flag: bool,
        dedupe_flag: bool,
        source_table: SqlTable,
    ) -> int:
        loader = self._get_loader()
        target_schema = snapshot.target_schema or loader.default_schema or snapshot.source_schema
        plan = build_loader_plan(
            schema=target_schema,
            table_name=snapshot.target_table_name,
            replace=replace_flag,
            deduplicate=dedupe_flag,
        )
        loaded = loader.load_rows(plan, batch, list(source_table.columns))
        logger.debug(
            "Loader wrote %s row(s) into %s.%s (replace=%s, dedupe=%s)",
            loaded,
            plan.schema,
            plan.table_name,
            replace_flag,
            dedupe_flag,
        )
        return loaded

    def _get_loader(self) -> BaseTableLoader:
        params = get_ingestion_connection_params()
        method = (params.ingestion_method or "sql").strip().lower()
        if method not in {"sql", "spark"}:
            method = "sql"

        if self._loader is None or self._loader_method != method:
            if method == "spark":
                self._loader = SparkTableLoader()
            else:
                self._loader = DatabricksTableLoader()
            self._loader_method = method
        return self._loader

    def _compute_timestamp_watermark(
        self,
        snapshot: ScheduleSnapshot,
        row: dict[str, Any],
        current: datetime | None,
    ) -> datetime | None:
        if snapshot.load_strategy is not IngestionLoadStrategy.TIMESTAMP:
            return current
        column = snapshot.watermark_column
        if not column:
            return current
        candidate = row.get(column)
        if isinstance(candidate, datetime):
            if candidate.tzinfo is None:
                candidate = candidate.replace(tzinfo=timezone.utc)
            else:
                candidate = candidate.astimezone(timezone.utc)
            if current is None or candidate > current:
                return candidate
        return current

    def _compute_id_watermark(
        self,
        snapshot: ScheduleSnapshot,
        row: dict[str, Any],
        current: int | None,
    ) -> int | None:
        if snapshot.load_strategy is not IngestionLoadStrategy.NUMERIC_KEY:
            return current
        column = snapshot.primary_key_column
        if not column:
            return current
        candidate = row.get(column)
        if candidate is None:
            return current
        try:
            integer_candidate = int(candidate)
        except (TypeError, ValueError):
            return current
        if current is None or integer_candidate > current:
            return integer_candidate
        return current

    def _snapshot(self, schedule: IngestionSchedule) -> ScheduleSnapshot:
        selection = schedule.table_selection
        connection = selection.system_connection
        target_table = schedule.target_table_name or selection.table_name
        composed_table = build_ingestion_table_name(connection, selection)
        return ScheduleSnapshot(
            id=schedule.id,
            load_strategy=IngestionLoadStrategy(schedule.load_strategy),
            watermark_column=schedule.watermark_column,
            primary_key_column=schedule.primary_key_column,
            batch_size=schedule.batch_size,
            target_schema=schedule.target_schema,
            target_table_name=composed_table,
            connection_string=connection.connection_string,
            connection_type=SystemConnectionType(connection.connection_type),
            source_schema=selection.schema_name,
            source_table=selection.table_name,
            last_watermark_timestamp=schedule.last_watermark_timestamp,
            last_watermark_id=schedule.last_watermark_id,
        )

    def _mark_run_failed(self, schedule_id: UUID, run_id: UUID, exc: Exception) -> None:
        with self._session_scope() as session:
            run = session.get(IngestionRun, run_id)
            schedule = session.get(IngestionSchedule, schedule_id)
            if run is None or schedule is None:
                return
            run.status = IngestionRunStatus.FAILED.value
            run.error_message = (str(exc) or "Ingestion failed.")[:2000]
            run.completed_at = datetime.now(timezone.utc)
            schedule.last_run_completed_at = run.completed_at
            schedule.last_run_status = IngestionRunStatus.FAILED.value
            schedule.last_run_error = run.error_message

    def _mark_run_completed(self, schedule_id: UUID, run_id: UUID, outcome: IngestionOutcome) -> None:
        with self._session_scope() as session:
            run = session.get(IngestionRun, run_id)
            schedule = session.get(IngestionSchedule, schedule_id)
            if run is None or schedule is None:
                return
            run.status = IngestionRunStatus.COMPLETED.value
            run.rows_loaded = outcome.rows_loaded
            run.rows_expected = outcome.rows_expected
            run.completed_at = datetime.now(timezone.utc)
            run.watermark_timestamp_after = outcome.watermark_timestamp
            run.watermark_id_after = outcome.watermark_id
            run.query_text = outcome.query_text[:2000]

            schedule.last_run_completed_at = run.completed_at
            schedule.last_run_status = IngestionRunStatus.COMPLETED.value
            schedule.last_run_error = None
            schedule.total_runs += 1
            schedule.total_rows_loaded += outcome.rows_loaded
            if outcome.watermark_timestamp is not None:
                schedule.last_watermark_timestamp = outcome.watermark_timestamp
            if outcome.watermark_id is not None:
                schedule.last_watermark_id = outcome.watermark_id
        logger.debug(
            "Persisted completion metadata for run %s (schedule %s)", run_id, schedule_id
        )

    def _update_run_progress(
        self,
        run_id: UUID,
        *,
        rows_loaded: int | None = None,
        rows_expected: int | None = None,
    ) -> None:
        if rows_loaded is None and rows_expected is None:
            return
        with self._session_scope() as session:
            run = session.get(IngestionRun, run_id)
            if run is None:
                return
            if rows_expected is not None:
                run.rows_expected = rows_expected
            if rows_loaded is not None:
                run.rows_loaded = rows_loaded

    def _is_run_active(self, run_id: UUID) -> bool:
        with self._session_scope() as session:
            run = session.get(IngestionRun, run_id)
            if run is None:
                return False
            return run.status == IngestionRunStatus.RUNNING.value

    def cleanup_stuck_runs(self) -> int:
        """Expose watchdog cleanup for manual or scheduled invocation."""
        expired = self._expire_stale_runs(force_log=True)
        if expired:
            logger.info("Watchdog manually marked %s stuck ingestion run(s) as failed", expired)
        return expired

    def _expire_stale_runs(self, *, force_log: bool = False) -> int:
        settings = get_settings()
        timeout_minutes = settings.ingestion_run_timeout_minutes
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=timeout_minutes)
        expired_count = 0
        with self._session_scope() as session:
            stale_runs = (
                session.query(IngestionRun)
                .filter(IngestionRun.status == IngestionRunStatus.RUNNING.value)
                .filter(IngestionRun.started_at.isnot(None))
                .filter(IngestionRun.started_at < cutoff)
                .all()
            )

            error_template = (
                "Ingestion run exceeded watchdog timeout of %s minute(s)." % timeout_minutes
            )
            for run in stale_runs:
                schedule = session.get(IngestionSchedule, run.ingestion_schedule_id)
                if not schedule:
                    continue
                run.status = IngestionRunStatus.FAILED.value
                run.completed_at = datetime.now(timezone.utc)
                run.error_message = error_template[:2000]
                schedule.last_run_completed_at = run.completed_at
                schedule.last_run_status = IngestionRunStatus.FAILED.value
                schedule.last_run_error = run.error_message
                expired_count += 1

        if expired_count or force_log:
            logger.debug(
                "Watchdog evaluated stale runs (timeout=%s min, expired=%s)",
                timeout_minutes,
                expired_count,
            )
        return expired_count

    def _build_trigger(self, expression: str, tz_name: str | None) -> CronTrigger:
        tz = timezone.utc
        if tz_name:
            try:
                tz = ZoneInfo(tz_name)
            except ZoneInfoNotFoundError:
                tz = timezone.utc
        return CronTrigger.from_crontab(expression, timezone=tz)

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


scheduled_ingestion_engine = ScheduledIngestionEngine()


def _sanitize_part(value: str | None) -> str:
    if not value:
        return "segment"
    sanitized = re.sub(r"[^A-Za-z0-9]+", "_", value.strip())
    sanitized = sanitized.strip("_")
    return sanitized.lower() or "segment"


def build_ingestion_table_name(
    connection: SystemConnection,
    selection: ConnectionTableSelection,
) -> str:
    if connection.system and connection.system.name:
        system_part = connection.system.name
    elif connection.system and connection.system.physical_name:
        system_part = connection.system.physical_name
    else:
        system_part = str(connection.system_id)
    schema_part = selection.schema_name or "default"
    table_part = selection.table_name
    parts = [_sanitize_part(system_part), _sanitize_part(schema_part), _sanitize_part(table_part)]
    return "_".join(parts)
