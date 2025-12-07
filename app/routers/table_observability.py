from __future__ import annotations

from datetime import timezone
from uuid import UUID

from apscheduler.triggers.cron import CronTrigger
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from sqlalchemy import func
from sqlalchemy.orm import Session
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.database import get_db
from app.models import (
    TableObservabilityCategorySchedule,
    TableObservabilityMetric as TableObservabilityMetricModel,
    TableObservabilityRun,
)
from app.schemas import (
    TableObservabilityMetricRead,
    TableObservabilityRunRead,
    TableObservabilityScheduleRead,
    TableObservabilityScheduleUpdate,
)
from app.services.table_observability import (
    TableObservabilityRunService,
    TableObservabilityScheduleService,
    get_observability_category_definition,
    table_observability_engine,
)

router = APIRouter(prefix="/table-observability", tags=["Table Observability"])

_schedule_service = TableObservabilityScheduleService()
_run_service = TableObservabilityRunService()


def _validate_cron_expression(expression: str, tz_name: str | None) -> None:
    tz = timezone.utc
    if tz_name:
        try:
            tz = ZoneInfo(tz_name)
        except ZoneInfoNotFoundError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Unknown timezone '{tz_name}'.",
            ) from exc
    try:
        CronTrigger.from_crontab(expression, timezone=tz)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid cron expression: {exc}",
        ) from exc


def _serialize_schedule(schedule: TableObservabilityCategorySchedule) -> TableObservabilityScheduleRead:
    definition = get_observability_category_definition(schedule.category_name)
    payload = {
        "scheduleId": schedule.id,
        "categoryKey": schedule.category_name,
        "categoryName": definition.name,
        "cronExpression": schedule.cron_expression,
        "timezone": schedule.timezone,
        "isActive": schedule.is_active,
        "defaultCronExpression": definition.default_cron_expression,
        "defaultTimezone": definition.default_timezone,
        "cadence": definition.cadence,
        "rationale": definition.rationale,
        "lastRunStatus": schedule.last_run_status,
        "lastRunStartedAt": schedule.last_run_started_at,
        "lastRunCompletedAt": schedule.last_run_completed_at,
        "lastRunError": schedule.last_run_error,
        "createdAt": schedule.created_at,
        "updatedAt": schedule.updated_at,
    }
    return TableObservabilityScheduleRead(**payload)


def _serialize_run(run: TableObservabilityRun) -> TableObservabilityRunRead:
    definition = get_observability_category_definition(run.category_name)
    payload = {
        "runId": run.id,
        "scheduleId": run.schedule_id,
        "categoryKey": run.category_name,
        "categoryName": definition.name,
        "status": run.status,
        "startedAt": run.started_at,
        "completedAt": run.completed_at,
        "tableCount": run.table_count,
        "metricsCollected": run.metrics_collected,
        "error": run.error,
        "createdAt": run.created_at,
        "updatedAt": run.updated_at,
    }
    return TableObservabilityRunRead(**payload)


def _serialize_metric(metric: TableObservabilityMetricModel) -> TableObservabilityMetricRead:
    payload = {
        "metricId": metric.id,
        "runId": metric.run_id,
        "selectionId": metric.selection_id,
        "systemConnectionId": metric.system_connection_id,
        "schemaName": metric.schema_name,
        "tableName": metric.table_name,
        "metricCategory": metric.metric_category,
        "metricName": metric.metric_name,
        "metricUnit": metric.metric_unit,
        "metricValueNumber": metric.metric_value_number,
        "metricValueText": metric.metric_value_text,
        "metricPayload": metric.metric_payload,
        "metricStatus": metric.metric_status,
        "recordedAt": metric.recorded_at,
    }
    return TableObservabilityMetricRead(**payload)


@router.get("/schedules", response_model=list[TableObservabilityScheduleRead])
def list_observability_schedules(db: Session = Depends(get_db)) -> list[TableObservabilityScheduleRead]:
    _schedule_service.ensure_category_schedules(session=db)
    if db.new:
        db.commit()
    schedules = (
        db.query(TableObservabilityCategorySchedule)
        .order_by(TableObservabilityCategorySchedule.category_name.asc())
        .all()
    )
    return [_serialize_schedule(schedule) for schedule in schedules]


@router.put("/schedules/{schedule_id}", response_model=TableObservabilityScheduleRead)
def update_observability_schedule(
    schedule_id: UUID,
    payload: TableObservabilityScheduleUpdate,
    db: Session = Depends(get_db),
) -> TableObservabilityScheduleRead:
    schedule = db.get(TableObservabilityCategorySchedule, schedule_id)
    if schedule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Schedule not found")

    updates = payload.dict(exclude_unset=True)
    if not updates:
        return _serialize_schedule(schedule)

    next_expression = updates.get("cron_expression", schedule.cron_expression)
    next_timezone = updates.get("timezone", schedule.timezone)
    _validate_cron_expression(next_expression, next_timezone)

    schedule.cron_expression = next_expression
    schedule.timezone = next_timezone
    if "is_active" in updates and updates["is_active"] is not None:
        schedule.is_active = updates["is_active"]

    db.add(schedule)
    db.commit()
    db.refresh(schedule)
    table_observability_engine.reload_jobs()
    return _serialize_schedule(schedule)


@router.post("/schedules/{schedule_id}/run", status_code=status.HTTP_202_ACCEPTED)
def trigger_observability_schedule(
    schedule_id: UUID,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
) -> dict[str, str]:
    schedule = db.get(TableObservabilityCategorySchedule, schedule_id)
    if schedule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Schedule not found")
    if not schedule.is_active:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Schedule is disabled")

    background_tasks.add_task(table_observability_engine.trigger_now, schedule.id)
    return {"detail": "Table observability run started"}


@router.get("/runs", response_model=list[TableObservabilityRunRead])
def list_observability_runs(
    category: str | None = Query(default=None, alias="categoryKey"),
    limit: int = Query(default=20, ge=1, le=200),
    db: Session = Depends(get_db),
) -> list[TableObservabilityRunRead]:
    query = db.query(TableObservabilityRun)
    if category:
        get_observability_category_definition(category)
        query = query.filter(TableObservabilityRun.category_name == category)

    runs = (
        query.order_by(
            TableObservabilityRun.started_at.desc(),
            TableObservabilityRun.created_at.desc(),
        )
        .limit(limit)
        .all()
    )
    return [_serialize_run(run) for run in runs]


@router.get("/runs/{run_id}", response_model=TableObservabilityRunRead)
def read_observability_run(run_id: UUID, db: Session = Depends(get_db)) -> TableObservabilityRunRead:
    run = db.get(TableObservabilityRun, run_id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    return _serialize_run(run)


@router.get("/metrics", response_model=list[TableObservabilityMetricRead])
def list_observability_metrics(
    run_id: UUID | None = Query(default=None, alias="runId"),
    selection_id: UUID | None = Query(default=None, alias="selectionId"),
    system_connection_id: UUID | None = Query(default=None, alias="systemConnectionId"),
    schema_name: str | None = Query(default=None, alias="schemaName"),
    table_name: str | None = Query(default=None, alias="tableName"),
    category: str | None = Query(default=None, alias="categoryKey"),
    metric_name: str | None = Query(default=None, alias="metricName"),
    limit: int = Query(default=200, ge=1, le=1000),
    db: Session = Depends(get_db),
) -> list[TableObservabilityMetricRead]:
    query = db.query(TableObservabilityMetricModel)
    if run_id:
        query = query.filter(TableObservabilityMetricModel.run_id == run_id)
    if selection_id:
        query = query.filter(TableObservabilityMetricModel.selection_id == selection_id)
    if system_connection_id:
        query = query.filter(TableObservabilityMetricModel.system_connection_id == system_connection_id)
    if schema_name is not None:
        if schema_name == "":
            query = query.filter(TableObservabilityMetricModel.schema_name.is_(None))
        else:
            query = query.filter(
                func.lower(TableObservabilityMetricModel.schema_name) == schema_name.strip().lower()
            )
    if table_name is not None:
        query = query.filter(func.lower(TableObservabilityMetricModel.table_name) == table_name.strip().lower())
    if category:
        get_observability_category_definition(category)
        query = query.filter(TableObservabilityMetricModel.metric_category == category)
    if metric_name:
        query = query.filter(func.lower(TableObservabilityMetricModel.metric_name) == metric_name.strip().lower())

    metrics = (
        query.order_by(TableObservabilityMetricModel.recorded_at.desc())
        .limit(limit)
        .all()
    )
    return [_serialize_metric(metric) for metric in metrics]


__all__ = ["router"]
