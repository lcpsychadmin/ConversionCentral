from enum import Enum
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import System, SystemConnection
from app.schemas import (
    SystemConnectionCreate,
    SystemConnectionRead,
    SystemConnectionTestRequest,
    SystemConnectionTestResult,
    SystemConnectionUpdate,
)
from app.services.connection_testing import ConnectionTestError, test_connection

router = APIRouter(prefix="/system-connections", tags=["System Connections"])


def _normalize_payload(data: dict) -> dict:
    return {key: value.value if isinstance(value, Enum) else value for key, value in data.items()}


def _get_system_connection_or_404(
    system_connection_id: UUID, db: Session
) -> SystemConnection:
    system_connection = db.get(SystemConnection, system_connection_id)
    if not system_connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="System connection not found",
        )
    return system_connection


def _ensure_system_exists(system_id: UUID | None, db: Session) -> None:
    if system_id and not db.get(System, system_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="System not found")


@router.post("", response_model=SystemConnectionRead, status_code=status.HTTP_201_CREATED)
def create_system_connection(
    payload: SystemConnectionCreate, db: Session = Depends(get_db)
) -> SystemConnectionRead:
    _ensure_system_exists(payload.system_id, db)

    system_connection = SystemConnection(**_normalize_payload(payload.dict()))
    db.add(system_connection)
    db.commit()
    db.refresh(system_connection)
    return system_connection


@router.post("/test", response_model=SystemConnectionTestResult)
def test_system_connection(payload: SystemConnectionTestRequest) -> SystemConnectionTestResult:
    try:
        duration_ms, sanitized = test_connection(payload.connection_type, payload.connection_string)
    except ConnectionTestError as exc:
        return SystemConnectionTestResult(success=False, message=str(exc))

    return SystemConnectionTestResult(
        success=True,
        message="Connection successful.",
        duration_ms=duration_ms,
        connection_summary=sanitized,
    )


@router.get("", response_model=list[SystemConnectionRead])
def list_system_connections(db: Session = Depends(get_db)) -> list[SystemConnectionRead]:
    return db.query(SystemConnection).all()


@router.get("/{system_connection_id}", response_model=SystemConnectionRead)
def get_system_connection(
    system_connection_id: UUID, db: Session = Depends(get_db)
) -> SystemConnectionRead:
    return _get_system_connection_or_404(system_connection_id, db)


@router.put("/{system_connection_id}", response_model=SystemConnectionRead)
def update_system_connection(
    system_connection_id: UUID,
    payload: SystemConnectionUpdate,
    db: Session = Depends(get_db),
) -> SystemConnectionRead:
    system_connection = _get_system_connection_or_404(system_connection_id, db)

    update_data = _normalize_payload(payload.dict(exclude_unset=True))
    _ensure_system_exists(update_data.get("system_id"), db)

    for field_name, value in update_data.items():
        setattr(system_connection, field_name, value)

    db.commit()
    db.refresh(system_connection)
    return system_connection


@router.delete("/{system_connection_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_system_connection(
    system_connection_id: UUID, db: Session = Depends(get_db)
) -> None:
    system_connection = _get_system_connection_or_404(system_connection_id, db)
    db.delete(system_connection)
    db.commit()
