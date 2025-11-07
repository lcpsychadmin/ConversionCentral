from __future__ import annotations

from uuid import UUID

from datetime import datetime, timezone

from apscheduler.triggers.cron import CronTrigger
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Response, status
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.database import get_db
from app.models import ConnectionTableSelection, IngestionRun, IngestionSchedule, SapHanaSetting, SystemConnection
from app.schemas import (
    IngestionLoadStrategy,
    IngestionRunRead,
    IngestionScheduleCreate,
    IngestionScheduleRead,
    IngestionScheduleUpdate,
    DataWarehouseTarget,
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


def _resolve_target_warehouse(
    db: Session,
    target: DataWarehouseTarget,
    sap_hana_setting_id: UUID | None,
) -> tuple[str, UUID | None]:
    if target is DataWarehouseTarget.SAP_HANA:
        if sap_hana_setting_id is not None:
            setting = db.get(SapHanaSetting, sap_hana_setting_id)
            if setting is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="SAP HANA setting not found")
        else:
            stmt = (
                select(SapHanaSetting)
                .order_by(SapHanaSetting.is_active.desc(), SapHanaSetting.updated_at.desc())
                .limit(1)
            )
            setting = db.execute(stmt).scalars().first()
            if setting is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="SAP HANA warehouse is not configured.",
                )
        if not setting.is_active:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Selected SAP HANA configuration is inactive.",
            )
        return (DataWarehouseTarget.SAP_HANA.value, setting.id)

    if sap_hana_setting_id is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="sap_hana_setting_id is only valid when targeting the SAP HANA warehouse.",
        )
    return (DataWarehouseTarget.DATABRICKS_SQL.value, None)


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
    target_table_name = build_ingestion_table_name(
        selection.system_connection,
        selection,
    )

    target_value, sap_hana_setting_id = _resolve_target_warehouse(
        db,
        payload.target_warehouse,
        payload.sap_hana_setting_id,
    )

    # Prevent creating duplicate schedules for the same selection/expression/timezone
    existing = (
        db.query(IngestionSchedule)
        .filter(IngestionSchedule.connection_table_selection_id == payload.connection_table_selection_id)
        .filter(IngestionSchedule.schedule_expression == payload.schedule_expression)
        .filter((IngestionSchedule.timezone.is_(None) & (payload.timezone is None)) | (IngestionSchedule.timezone == payload.timezone))
        .one_or_none()
    )
    if existing is not None:
        # A schedule with identical selection/expression/timezone already exists - treat as conflict
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="An identical schedule already exists")

    schedule = IngestionSchedule(
        connection_table_selection_id=payload.connection_table_selection_id,
        schedule_expression=payload.schedule_expression,
        timezone=payload.timezone,
        load_strategy=payload.load_strategy.value,
        watermark_column=payload.watermark_column,
        primary_key_column=payload.primary_key_column,
        target_schema=payload.target_schema,
        target_table_name=target_table_name,
        target_warehouse=target_value,
        sap_hana_setting_id=sap_hana_setting_id,
        batch_size=payload.batch_size,
        is_active=payload.is_active,
    )
    db.add(schedule)
    db.commit()
    db.refresh(schedule)
    scheduled_ingestion_engine.reload_jobs()
    return schedule



@router.delete("/{schedule_id}", status_code=status.HTTP_204_NO_CONTENT, response_class=Response)
def delete_schedule(schedule_id: UUID, db: Session = Depends(get_db)) -> Response:
    schedule = _get_schedule_or_404(schedule_id, db)
    # Delete associated runs (optional) and the schedule itself
    # Removing runs keeps history cleanup optional; we'll remove runs to fully clear state for now.
    try:
        db.query(IngestionRun).filter(IngestionRun.ingestion_schedule_id == schedule.id).delete(synchronize_session=False)
    except Exception:
        # ignore if unable to delete runs; proceed to delete schedule
        pass
    db.delete(schedule)
    db.commit()
    scheduled_ingestion_engine.reload_jobs()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


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
    target_override = update_data.pop("target_warehouse", None)
    hana_override = update_data.pop("sap_hana_setting_id", None)
    strategy = IngestionLoadStrategy(update_data.get("load_strategy", schedule.load_strategy))
    merged = {
        "watermark_column": schedule.watermark_column,
        "primary_key_column": schedule.primary_key_column,
        "schedule_expression": schedule.schedule_expression,
        "timezone": schedule.timezone,
    }
    merged.update(update_data)
    _validate_payload(strategy, merged)

    current_target = DataWarehouseTarget(schedule.target_warehouse)
    if target_override is not None:
        current_target = DataWarehouseTarget(target_override)

    if current_target is DataWarehouseTarget.DATABRICKS_SQL:
        if hana_override is not None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="sap_hana_setting_id is only valid when targeting the SAP HANA warehouse.",
            )
        requested_hana_id: UUID | None = None
    elif hana_override is not None:
        requested_hana_id = hana_override
    else:
        requested_hana_id = schedule.sap_hana_setting_id

    target_value, sap_hana_setting_id = _resolve_target_warehouse(db, current_target, requested_hana_id)
    schedule.target_warehouse = target_value
    schedule.sap_hana_setting_id = sap_hana_setting_id

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


@router.post("/{schedule_id}/runs/{run_id}/abort", status_code=status.HTTP_200_OK)
def abort_run(schedule_id: UUID, run_id: UUID, db: Session = Depends(get_db)) -> dict[str, str]:
    # Allow manual abort of a run - marks as failed/completed so the UI stops showing RUNNING
    _get_schedule_or_404(schedule_id, db)
    run = db.get(IngestionRun, run_id)
    if not run or str(run.ingestion_schedule_id) != str(schedule_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found for schedule")

    if run.status != "running":
        return {"detail": "Run is not running"}

    run.status = "failed"
    run.completed_at = datetime.now(timezone.utc)
    run.error_message = "Manually aborted"
    db.add(run)

    schedule = db.get(IngestionSchedule, schedule_id)
    if schedule:
        schedule.last_run_completed_at = run.completed_at
        schedule.last_run_status = "failed"
        schedule.last_run_error = run.error_message
        db.add(schedule)

    db.commit()
    return {"detail": "Run aborted"}


@router.post("/cleanup-stuck-runs", status_code=status.HTTP_200_OK)
def cleanup_stuck_runs() -> dict[str, object]:
    expired = scheduled_ingestion_engine.cleanup_stuck_runs()
    detail = (
        "No stuck runs detected" if expired == 0 else f"Marked {expired} run(s) as failed"
    )
    return {"detail": detail, "expired_runs": expired}
