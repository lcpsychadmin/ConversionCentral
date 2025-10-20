from datetime import datetime
from enum import Enum
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import ConnectionTableSelection, System, SystemConnection
from app.schemas import (
    ConnectionCatalogSelectionUpdate,
    ConnectionCatalogTable,
    ConnectionTablePreview,
    SystemConnectionCreate,
    SystemConnectionRead,
    SystemConnectionTestRequest,
    SystemConnectionTestResult,
    SystemConnectionUpdate,
)
from app.schemas import SystemConnectionType
from app.services.catalog_browser import (
    CatalogTable,
    ConnectionCatalogError,
    TablePreviewError,
    fetch_connection_catalog,
    fetch_table_preview,
)
from app.services.scheduled_ingestion import scheduled_ingestion_engine
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


def _assemble_catalog_response(
    system_connection: SystemConnection,
    catalog_entries: list[CatalogTable],
    db: Session,
) -> list[ConnectionCatalogTable]:
    selections = {
        (selection.schema_name, selection.table_name): selection
        for selection in system_connection.catalog_selections
    }

    now = datetime.utcnow()
    updated = False
    rows: list[ConnectionCatalogTable] = []
    seen: set[tuple[str, str]] = set()

    for entry in catalog_entries:
        key = (entry.schema_name, entry.table_name)
        selection = selections.get(key)
        if selection:
            if (
                selection.table_type != entry.table_type
                or selection.column_count != entry.column_count
                or selection.estimated_rows != entry.estimated_rows
            ):
                selection.table_type = entry.table_type
                selection.column_count = entry.column_count
                selection.estimated_rows = entry.estimated_rows
                updated = True
            if selection.last_seen_at != now:
                selection.last_seen_at = now
                updated = True

        rows.append(
            ConnectionCatalogTable(
                schema_name=entry.schema_name,
                table_name=entry.table_name,
                table_type=entry.table_type,
                column_count=entry.column_count,
                estimated_rows=entry.estimated_rows,
                selected=selection is not None,
                available=True,
                selection_id=selection.id if selection else None,
            )
        )
        seen.add(key)

    for key, selection in selections.items():
        if key in seen:
            continue
        rows.append(
            ConnectionCatalogTable(
                schema_name=selection.schema_name,
                table_name=selection.table_name,
                table_type=selection.table_type,
                column_count=selection.column_count,
                estimated_rows=selection.estimated_rows,
                selected=True,
                available=False,
                selection_id=selection.id,
            )
        )

    if updated:
        db.commit()
        db.refresh(system_connection)

    return sorted(rows, key=lambda item: (not item.available, item.schema_name, item.table_name))


@router.post("", response_model=SystemConnectionRead, status_code=status.HTTP_201_CREATED)
def create_system_connection(
    payload: SystemConnectionCreate, db: Session = Depends(get_db)
) -> SystemConnectionRead:
    _ensure_system_exists(payload.system_id, db)

    system_connection = SystemConnection(**_normalize_payload(payload.dict()))
    db.add(system_connection)
    db.commit()
    db.refresh(system_connection)
    scheduled_ingestion_engine.reload_jobs()
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


@router.get("/{system_connection_id}/catalog", response_model=list[ConnectionCatalogTable])
def browse_system_connection_catalog(
    system_connection_id: UUID,
    db: Session = Depends(get_db),
) -> list[ConnectionCatalogTable]:
    system_connection = _get_system_connection_or_404(system_connection_id, db)

    try:
        connection_type = SystemConnectionType(system_connection.connection_type)
    except ValueError as exc:  # pragma: no cover - defensive guard
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unsupported connection type for catalog browsing.",
        ) from exc

    try:
        catalog_entries = fetch_connection_catalog(
            connection_type,
            system_connection.connection_string,
        )
    except ConnectionCatalogError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return _assemble_catalog_response(system_connection, catalog_entries, db)


@router.get("/{system_connection_id}/catalog/preview", response_model=ConnectionTablePreview)
def preview_system_connection_table(
    system_connection_id: UUID,
    schema_name: str | None = Query(None, max_length=120),
    table_name: str = Query(..., min_length=1, max_length=200),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
) -> ConnectionTablePreview:
    system_connection = _get_system_connection_or_404(system_connection_id, db)

    try:
        connection_type = SystemConnectionType(system_connection.connection_type)
    except ValueError as exc:  # pragma: no cover - defensive guard
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unsupported connection type for data preview.",
        ) from exc

    schema = schema_name or None

    try:
        preview = fetch_table_preview(
            connection_type,
            system_connection.connection_string,
            schema,
            table_name,
            limit,
        )
    except TablePreviewError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return ConnectionTablePreview(columns=preview.columns, rows=preview.rows)


@router.put("/{system_connection_id}", response_model=SystemConnectionRead)
def update_system_connection(
    system_connection_id: UUID,
    payload: SystemConnectionUpdate,
    db: Session = Depends(get_db),
) -> SystemConnectionRead:
    system_connection = _get_system_connection_or_404(system_connection_id, db)

    update_data = _normalize_payload(payload.dict(exclude_unset=True))
    _ensure_system_exists(update_data.get("system_id"), db)

    ingestion_before = system_connection.ingestion_enabled
    for field_name, value in update_data.items():
        setattr(system_connection, field_name, value)

    db.commit()
    db.refresh(system_connection)
    if "ingestion_enabled" in update_data and system_connection.ingestion_enabled != ingestion_before:
        scheduled_ingestion_engine.reload_jobs()
    return system_connection


@router.put("/{system_connection_id}/catalog/selection", status_code=status.HTTP_204_NO_CONTENT)
def update_system_connection_catalog_selection(
    system_connection_id: UUID,
    payload: ConnectionCatalogSelectionUpdate,
    db: Session = Depends(get_db),
) -> None:
    system_connection = _get_system_connection_or_404(system_connection_id, db)

    desired = {
        (item.schema_name, item.table_name): item
        for item in payload.selected_tables
    }
    existing = {
        (selection.schema_name, selection.table_name): selection
        for selection in system_connection.catalog_selections
    }

    now = datetime.utcnow()
    changed = False

    for key, selection in list(existing.items()):
        if key not in desired:
            db.delete(selection)
            changed = True

    for key, item in desired.items():
        selection = existing.get(key)
        if selection is None:
            db.add(
                ConnectionTableSelection(
                    system_connection_id=system_connection.id,
                    schema_name=item.schema_name,
                    table_name=item.table_name,
                    table_type=item.table_type,
                    column_count=item.column_count,
                    estimated_rows=item.estimated_rows,
                    last_seen_at=now,
                )
            )
            changed = True
            continue

        if (
            selection.table_type != item.table_type
            or selection.column_count != item.column_count
            or selection.estimated_rows != item.estimated_rows
        ):
            selection.table_type = item.table_type
            selection.column_count = item.column_count
            selection.estimated_rows = item.estimated_rows
            changed = True
        if selection.last_seen_at != now:
            selection.last_seen_at = now
            changed = True

    if changed:
        db.commit()
        db.refresh(system_connection)


@router.delete("/{system_connection_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_system_connection(
    system_connection_id: UUID, db: Session = Depends(get_db)
) -> None:
    system_connection = _get_system_connection_or_404(system_connection_id, db)
    db.delete(system_connection)
    db.commit()
    scheduled_ingestion_engine.reload_jobs()

