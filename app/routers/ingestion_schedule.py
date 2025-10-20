from __future__ import annotations

from uuid import UUID

from datetime import timezone

from apscheduler.triggers.cron import CronTrigger
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from sqlalchemy.orm import Session, joinedload
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.database import get_db
from app.models import ConnectionTableSelection, IngestionRun, IngestionSchedule, SystemConnection
from app.schemas import (
    IngestionLoadStrategy,
    IngestionRunRead,
    IngestionScheduleCreate,
    IngestionScheduleRead,
    IngestionScheduleUpdate,
)
from app.services.scheduled_ingestion import (
    build_ingestion_table_name,
    scheduled_ingestion_engine,
)

router = APIRouter(prefix="/ingestion-schedules", tags=["Ingestion Schedules"])


def _get_schedule_or_404(schedule_id: UUID, db: Session) -> IngestionSchedule:
    schedule = (
        db.query(IngestionSchedule)
        .options(
            joinedload(IngestionSchedule.table_selection)
            .joinedload(ConnectionTableSelection.system_connection)
            .joinedload(SystemConnection.system)
        )
        .filter(IngestionSchedule.id == schedule_id)
        .one_or_none()
    )
    if schedule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Schedule not found")
    return schedule


def _get_selection_for_ingestion(selection_id: UUID, db: Session) -> ConnectionTableSelection:
    selection = (
        db.query(ConnectionTableSelection)
        .options(
            joinedload(ConnectionTableSelection.system_connection).joinedload(
                SystemConnection.system
            )
        )
        .filter(ConnectionTableSelection.id == selection_id)
        .one_or_none()
    )
    if selection is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Catalog selection not found")
    if not selection.system_connection.ingestion_enabled:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Ingestion is disabled for the selected connection.",
        )
    return selection


def _validate_payload(strategy: IngestionLoadStrategy, payload: dict) -> None:
    if strategy is IngestionLoadStrategy.TIMESTAMP and not payload.get("watermark_column"):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Timestamp strategy requires a watermark column.")
    if strategy is IngestionLoadStrategy.NUMERIC_KEY and not payload.get("primary_key_column"):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Numeric key strategy requires a primary key column.")
    expression = payload.get("schedule_expression")
    if not expression:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Schedule expression is required.")
    _validate_schedule_expression(expression, payload.get("timezone"))


def _validate_schedule_expression(expression: str, tz_name: str | None) -> None:
    tz = timezone.utc
    if tz_name:
        try:
            tz = ZoneInfo(tz_name)
        except ZoneInfoNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"Unknown timezone '{tz_name}'.") from exc
    try:
        CronTrigger.from_crontab(expression, timezone=tz)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"Invalid cron expression: {exc}") from exc


@router.post("", response_model=IngestionScheduleRead, status_code=status.HTTP_201_CREATED)
def create_schedule(
    payload: IngestionScheduleCreate,
    db: Session = Depends(get_db),
) -> IngestionSchedule:
    selection = _get_selection_for_ingestion(payload.connection_table_selection_id, db)
    data = payload.dict()
    _validate_payload(payload.load_strategy, data)
    data["target_table_name"] = build_ingestion_table_name(
        selection.system_connection,
        selection,
    )

    schedule = IngestionSchedule(
        connection_table_selection_id=data["connection_table_selection_id"],
        schedule_expression=data["schedule_expression"],
        timezone=data.get("timezone"),
        load_strategy=data["load_strategy"].value,
        watermark_column=data.get("watermark_column"),
        primary_key_column=data.get("primary_key_column"),
        target_schema=data.get("target_schema"),
        target_table_name=data.get("target_table_name"),
        batch_size=data["batch_size"],
        is_active=data["is_active"],
    )
    db.add(schedule)
    db.commit()
    db.refresh(schedule)
    scheduled_ingestion_engine.reload_jobs()
    return schedule


@router.get("", response_model=list[IngestionScheduleRead])
def list_schedules(db: Session = Depends(get_db)) -> list[IngestionSchedule]:
    return db.query(IngestionSchedule).all()


@router.get("/{schedule_id}", response_model=IngestionScheduleRead)
def get_schedule(schedule_id: UUID, db: Session = Depends(get_db)) -> IngestionSchedule:
    return _get_schedule_or_404(schedule_id, db)


@router.put("/{schedule_id}", response_model=IngestionScheduleRead)
def update_schedule(
    schedule_id: UUID,
    payload: IngestionScheduleUpdate,
    db: Session = Depends(get_db),
) -> IngestionSchedule:
    schedule = _get_schedule_or_404(schedule_id, db)
    if not schedule.table_selection.system_connection.ingestion_enabled:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Ingestion is disabled for the associated connection.",
        )
    update_data = payload.dict(exclude_unset=True)
    strategy = IngestionLoadStrategy(update_data.get("load_strategy", schedule.load_strategy))
    merged = {
        "watermark_column": schedule.watermark_column,
        "primary_key_column": schedule.primary_key_column,
        "schedule_expression": schedule.schedule_expression,
        "timezone": schedule.timezone,
    }
    merged.update(update_data)
    _validate_payload(strategy, merged)

    if "load_strategy" in update_data and isinstance(update_data["load_strategy"], IngestionLoadStrategy):
        update_data["load_strategy"] = update_data["load_strategy"].value

    for field, value in update_data.items():
        setattr(schedule, field, value)

    schedule.target_table_name = build_ingestion_table_name(
        schedule.table_selection.system_connection,
        schedule.table_selection,
    )

    db.add(schedule)
    db.commit()
    db.refresh(schedule)
    scheduled_ingestion_engine.reload_jobs()
    return schedule


@router.post("/{schedule_id}/run", status_code=status.HTTP_202_ACCEPTED)
def trigger_schedule(
    schedule_id: UUID,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
) -> dict[str, str]:
    schedule = _get_schedule_or_404(schedule_id, db)
    if not schedule.is_active:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Schedule is disabled")
    if not schedule.table_selection.system_connection.ingestion_enabled:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Ingestion is disabled for the associated connection.",
        )
    background_tasks.add_task(scheduled_ingestion_engine.trigger_now, schedule.id)
    return {"detail": "Ingestion run started"}


@router.get("/{schedule_id}/runs", response_model=list[IngestionRunRead])
def list_runs(schedule_id: UUID, db: Session = Depends(get_db)) -> list[IngestionRun]:
    _get_schedule_or_404(schedule_id, db)
    return (
        db.query(IngestionRun)
        .filter(IngestionRun.ingestion_schedule_id == schedule_id)
        .order_by(IngestionRun.created_at.desc())
        .all()
    )
