from enum import Enum
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import ExecutionContext, IngestionJob, SystemConnection, Table
from app.schemas import (
    IngestionJobCreate,
    IngestionJobRead,
    IngestionJobRunRequest,
    IngestionJobUpdate,
)
from app.services.ingestion_runner import IngestionRunner
from app.services.connection_testing import ConnectionTestError

router = APIRouter(prefix="/ingestion-jobs", tags=["Ingestion Jobs"])


def _normalize_payload(data: dict) -> dict:
    return {key: value.value if isinstance(value, Enum) else value for key, value in data.items()}


def _get_ingestion_job_or_404(ingestion_job_id: UUID, db: Session) -> IngestionJob:
    ingestion_job = db.get(IngestionJob, ingestion_job_id)
    if not ingestion_job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ingestion job not found")
    return ingestion_job


def _ensure_execution_context_exists(execution_context_id: UUID | None, db: Session) -> None:
    if execution_context_id and not db.get(ExecutionContext, execution_context_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Execution context not found",
        )


def _ensure_table_exists(table_id: UUID | None, db: Session) -> None:
    if table_id and not db.get(Table, table_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")


@router.post("", response_model=IngestionJobRead, status_code=status.HTTP_201_CREATED)
def create_ingestion_job(
    payload: IngestionJobCreate, db: Session = Depends(get_db)
) -> IngestionJobRead:
    _ensure_execution_context_exists(payload.execution_context_id, db)
    _ensure_table_exists(payload.table_id, db)

    ingestion_job = IngestionJob(**_normalize_payload(payload.dict()))
    db.add(ingestion_job)
    db.commit()
    db.refresh(ingestion_job)
    return ingestion_job


@router.get("", response_model=list[IngestionJobRead])
def list_ingestion_jobs(db: Session = Depends(get_db)) -> list[IngestionJobRead]:
    return db.query(IngestionJob).all()


@router.get("/{ingestion_job_id}", response_model=IngestionJobRead)
def get_ingestion_job(
    ingestion_job_id: UUID, db: Session = Depends(get_db)
) -> IngestionJobRead:
    return _get_ingestion_job_or_404(ingestion_job_id, db)


@router.put("/{ingestion_job_id}", response_model=IngestionJobRead)
def update_ingestion_job(
    ingestion_job_id: UUID,
    payload: IngestionJobUpdate,
    db: Session = Depends(get_db),
) -> IngestionJobRead:
    ingestion_job = _get_ingestion_job_or_404(ingestion_job_id, db)

    update_data = _normalize_payload(payload.dict(exclude_unset=True))
    _ensure_execution_context_exists(update_data.get("execution_context_id"), db)
    _ensure_table_exists(update_data.get("table_id"), db)

    for field_name, value in update_data.items():
        setattr(ingestion_job, field_name, value)

    db.commit()
    db.refresh(ingestion_job)
    return ingestion_job


@router.delete("/{ingestion_job_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_ingestion_job(ingestion_job_id: UUID, db: Session = Depends(get_db)) -> None:
    ingestion_job = _get_ingestion_job_or_404(ingestion_job_id, db)
    db.delete(ingestion_job)
    db.commit()


def _get_active_connection_for_table(table: Table, db: Session) -> SystemConnection | None:
    return (
        db.query(SystemConnection)
        .filter(
            SystemConnection.system_id == table.system_id,
            SystemConnection.active.is_(True),
        )
        .order_by(SystemConnection.created_at.desc())
        .first()
    )


@router.post("/{ingestion_job_id}/run", response_model=IngestionJobRead)
def run_ingestion_job(
    ingestion_job_id: UUID,
    payload: IngestionJobRunRequest = IngestionJobRunRequest(),
    db: Session = Depends(get_db),
) -> IngestionJobRead:
    ingestion_job = _get_ingestion_job_or_404(ingestion_job_id, db)
    table = ingestion_job.table

    if table is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Ingestion job is missing table metadata.")

    connection = _get_active_connection_for_table(table, db)
    if connection is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No active system connection available for the table's system.")

    runner = IngestionRunner()

    try:
        runner.ingest_table(
            connection,
            table,
            job=ingestion_job,
            batch_size=payload.batch_size or 5_000,
            replace=payload.replace,
        )
        db.commit()
    except ConnectionTestError as exc:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception:
        db.rollback()
        raise

    db.refresh(ingestion_job)
    return ingestion_job
