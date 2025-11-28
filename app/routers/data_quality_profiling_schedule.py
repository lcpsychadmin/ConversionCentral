from __future__ import annotations

from datetime import timezone
from typing import Optional
from uuid import UUID

from apscheduler.triggers.cron import CronTrigger
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Response, status
from sqlalchemy.orm import Session, joinedload
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.database import get_db
from app.models import DataObject, DataQualityProfilingSchedule, SystemConnection
from app.routers.data_quality_testgen import get_testgen_client
from app.schemas.data_quality import (
    ProfilingScheduleCreateRequest,
    ProfilingScheduleRead,
)
from app.services.data_quality_keys import parse_connection_id, parse_table_group_id
from app.services.data_quality_testgen import TestGenClient, TestGenClientError
from app.services.scheduled_profiling import scheduled_profiling_engine

router = APIRouter(prefix="/data-quality/profiling-schedules", tags=["Data Quality"])


def _validate_schedule_expression(expression: str, tz_name: Optional[str]) -> None:
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


def _coerce_uuid(value: Optional[str]) -> Optional[UUID]:
    if not value:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


def _resolve_connection_and_object_ids(
    db: Session,
    *,
    table_group_id: str,
    connection_identifier: Optional[str],
) -> tuple[Optional[UUID], Optional[UUID]]:
    conn_text, obj_text = parse_connection_id(connection_identifier) if connection_identifier else (None, None)
    if not conn_text or not obj_text:
        fallback_conn, fallback_obj = parse_table_group_id(table_group_id)
        conn_text = conn_text or fallback_conn
        obj_text = obj_text or fallback_obj

    conn_uuid = _coerce_uuid(conn_text)
    obj_uuid = _coerce_uuid(obj_text)

    if conn_uuid and not db.get(SystemConnection, conn_uuid):
        conn_uuid = None
    if obj_uuid and not db.get(DataObject, obj_uuid):
        obj_uuid = None
    return conn_uuid, obj_uuid


def _serialize_schedule(schedule: DataQualityProfilingSchedule) -> ProfilingScheduleRead:
    connection = schedule.connection
    system = connection.system if connection else None
    data_object = schedule.data_object
    payload = {
        "profilingScheduleId": schedule.id,
        "tableGroupId": schedule.table_group_id,
        "tableGroupName": schedule.table_group_name,
        "connectionId": connection.id if connection else None,
        "connectionName": system.name if system else None,
        "applicationId": system.id if system else None,
        "applicationName": system.name if system else None,
        "dataObjectId": data_object.id if data_object else None,
        "dataObjectName": data_object.name if data_object else None,
        "scheduleExpression": schedule.schedule_expression,
        "timezone": schedule.timezone,
        "isActive": schedule.is_active,
        "lastProfileRunId": schedule.last_profile_run_id,
        "lastRunStatus": schedule.last_run_status,
        "lastRunStartedAt": schedule.last_run_started_at,
        "lastRunCompletedAt": schedule.last_run_completed_at,
        "lastRunError": schedule.last_run_error,
        "totalRuns": schedule.total_runs,
        "createdAt": schedule.created_at,
        "updatedAt": schedule.updated_at,
    }
    return ProfilingScheduleRead(**payload)


def _load_schedule(schedule_id: UUID, db: Session) -> DataQualityProfilingSchedule:
    schedule = (
        db.query(DataQualityProfilingSchedule)
        .options(
            joinedload(DataQualityProfilingSchedule.connection).joinedload(SystemConnection.system),
            joinedload(DataQualityProfilingSchedule.data_object),
        )
        .filter(DataQualityProfilingSchedule.id == schedule_id)
        .one_or_none()
    )
    if schedule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Schedule not found")
    return schedule


@router.get("", response_model=list[ProfilingScheduleRead])
def list_profiling_schedules(db: Session = Depends(get_db)) -> list[ProfilingScheduleRead]:
    schedules = (
        db.query(DataQualityProfilingSchedule)
        .options(
            joinedload(DataQualityProfilingSchedule.connection).joinedload(SystemConnection.system),
            joinedload(DataQualityProfilingSchedule.data_object),
        )
        .order_by(DataQualityProfilingSchedule.created_at.desc())
        .all()
    )
    return [_serialize_schedule(schedule) for schedule in schedules]


@router.post("", response_model=ProfilingScheduleRead, status_code=status.HTTP_201_CREATED)
def create_profiling_schedule(
    payload: ProfilingScheduleCreateRequest,
    db: Session = Depends(get_db),
    client: TestGenClient = Depends(get_testgen_client),
) -> ProfilingScheduleRead:
    data = payload.dict(by_alias=True)
    _validate_schedule_expression(data["scheduleExpression"], data.get("timezone"))

    duplicate = (
        db.query(DataQualityProfilingSchedule)
        .filter(DataQualityProfilingSchedule.table_group_id == data["tableGroupId"])
        .filter(DataQualityProfilingSchedule.schedule_expression == data["scheduleExpression"])
        .filter(
            (DataQualityProfilingSchedule.timezone.is_(None) & (data.get("timezone") is None))
            | (DataQualityProfilingSchedule.timezone == data.get("timezone"))
        )
        .one_or_none()
    )
    if duplicate is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="An identical schedule already exists for this table group.")

    try:
        details = client.get_table_group_details(data["tableGroupId"])
    except TestGenClientError as exc:
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
    if not details:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table group not found")

    connection_uuid, data_object_uuid = _resolve_connection_and_object_ids(
        db,
        table_group_id=data["tableGroupId"],
        connection_identifier=details.get("connection_id"),
    )

    schedule = DataQualityProfilingSchedule(
        table_group_id=data["tableGroupId"],
        table_group_name=details.get("name"),
        connection_id=connection_uuid,
        data_object_id=data_object_uuid,
        schedule_expression=data["scheduleExpression"],
        timezone=data.get("timezone"),
        is_active=data.get("isActive", True),
    )
    db.add(schedule)
    db.commit()
    db.refresh(schedule)

    scheduled_profiling_engine.reload_jobs()
    return _serialize_schedule(schedule)


@router.delete("/{schedule_id}", status_code=status.HTTP_204_NO_CONTENT, response_class=Response)
def delete_profiling_schedule(schedule_id: UUID, db: Session = Depends(get_db)) -> Response:
    schedule = _load_schedule(schedule_id, db)
    db.delete(schedule)
    db.commit()
    scheduled_profiling_engine.reload_jobs()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/{schedule_id}/run", status_code=status.HTTP_202_ACCEPTED)
def trigger_profiling_schedule(
    schedule_id: UUID,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
) -> dict[str, str]:
    schedule = _load_schedule(schedule_id, db)
    if not schedule.is_active:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Schedule is disabled")
    background_tasks.add_task(scheduled_profiling_engine.trigger_now, schedule.id)
    return {"detail": "Profiling run started"}
