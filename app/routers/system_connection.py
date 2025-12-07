from datetime import datetime, timezone
from enum import Enum
from urllib.parse import urlencode
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.ingestion.engine import get_ingestion_connection_params
from app.models import ConnectionTableSelection, SystemConnection
from app.schemas import (
    ConnectionCatalogSelectionUpdate,
    ConnectionCatalogTable,
    ConnectionTablePreview,
    SystemConnectionCreate,
    SystemConnectionRead,
    SystemConnectionTestRequest,
    SystemConnectionTestResult,
    SystemConnectionType,
    SystemConnectionUpdate,
    TableObservabilityPlan,
)
from app.services.catalog_browser import (
    CatalogTable,
    ConnectionCatalogError,
    TablePreviewError,
    fetch_connection_catalog,
    fetch_table_preview,
)
from app.services.connection_testing import ConnectionTestError, test_connection
from app.services.connection_catalog_cleanup import (
    CatalogRemoval,
    cascade_cleanup_for_catalog_removals,
)
from app.services.data_quality_provisioning import trigger_data_quality_provisioning
from app.services.table_observability import build_table_observability_plan

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


def get_managed_databricks_connection_string(
    *, catalog_override: str | None = None, schema_override: str | None = None
) -> str:
    try:
        params = get_ingestion_connection_params()
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    host = (params.workspace_host or "").strip()
    http_path = (params.http_path or "").strip()
    if not host or not http_path:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Databricks managed connection is not configured.",
        )

    catalog = (catalog_override or params.catalog or "").strip() or None
    schema_name = (schema_override or params.schema_name or "").strip() or None

    query: dict[str, str] = {"http_path": http_path}
    if catalog:
        query["catalog"] = catalog
    if schema_name:
        query["schema"] = schema_name

    query_string = urlencode(query)
    return f"jdbc:databricks://token:@{host}:443/default?{query_string}"

def _assemble_catalog_response(
    system_connection: SystemConnection,
    catalog_entries: list[CatalogTable],
    db: Session,
) -> list[ConnectionCatalogTable]:
    selections = {
        (selection.schema_name, selection.table_name): selection
        for selection in system_connection.catalog_selections
    }

    now = datetime.now(timezone.utc)
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
    raw_payload = payload.dict()
    use_managed = raw_payload.pop("use_databricks_managed_connection", False)
    catalog_override = raw_payload.pop("databricks_catalog", None)
    schema_override = raw_payload.pop("databricks_schema", None)

    if use_managed:
        raw_payload["connection_string"] = get_managed_databricks_connection_string(
            catalog_override=catalog_override,
            schema_override=schema_override,
        )
        raw_payload["connection_type"] = SystemConnectionType.JDBC

    raw_payload["use_databricks_managed_connection"] = use_managed
    raw_payload["databricks_catalog"] = catalog_override
    raw_payload["databricks_schema"] = schema_override
    data = _normalize_payload(raw_payload)

    system_connection = SystemConnection(**data)
    db.add(system_connection)
    db.commit()
    db.refresh(system_connection)
    trigger_data_quality_provisioning(reason="connection-created")
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


@router.get(
    "/{system_connection_id}/catalog/observability",
    response_model=TableObservabilityPlan,
)
def get_table_observability_plan(
    system_connection_id: UUID,
    db: Session = Depends(get_db),
) -> TableObservabilityPlan:
    system_connection = _get_system_connection_or_404(system_connection_id, db)
    return build_table_observability_plan(system_connection)


@router.put("/{system_connection_id}", response_model=SystemConnectionRead)
def update_system_connection(
    system_connection_id: UUID,
    payload: SystemConnectionUpdate,
    db: Session = Depends(get_db),
) -> SystemConnectionRead:
    system_connection = _get_system_connection_or_404(system_connection_id, db)

    raw_payload = payload.dict(exclude_unset=True)
    sentinel = object()
    use_managed = raw_payload.pop("use_databricks_managed_connection", sentinel)
    catalog_override = raw_payload.pop("databricks_catalog", sentinel)
    schema_override = raw_payload.pop("databricks_schema", sentinel)

    catalog_value = None if catalog_override is sentinel else catalog_override
    schema_value = None if schema_override is sentinel else schema_override

    if use_managed is not sentinel and use_managed:
        catalog_for_connection = (
            catalog_value if catalog_override is not sentinel else system_connection.databricks_catalog
        )
        schema_for_connection = (
            schema_value if schema_override is not sentinel else system_connection.databricks_schema
        )
        raw_payload["connection_string"] = get_managed_databricks_connection_string(
            catalog_override=catalog_for_connection,
            schema_override=schema_for_connection,
        )
        raw_payload["connection_type"] = SystemConnectionType.JDBC

    if use_managed is not sentinel:
        raw_payload["use_databricks_managed_connection"] = use_managed

    if catalog_override is not sentinel:
        raw_payload["databricks_catalog"] = catalog_value

    if schema_override is not sentinel:
        raw_payload["databricks_schema"] = schema_value

    update_data = _normalize_payload(raw_payload)

    for field_name, value in update_data.items():
        setattr(system_connection, field_name, value)

    db.commit()
    db.refresh(system_connection)
    trigger_data_quality_provisioning(reason="connection-updated")
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

    now = datetime.now(timezone.utc)
    changed = False
    removed_catalogs: list[CatalogRemoval] = []

    for key, selection in list(existing.items()):
        if key not in desired:
            removed_catalogs.append(
                CatalogRemoval(
                    system_connection_id=system_connection.id,
                    schema_name=selection.schema_name,
                    table_name=selection.table_name,
                )
            )
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

    if removed_catalogs:
        cascade_cleanup_for_catalog_removals(db, removed_catalogs)
        changed = True

    if changed:
        db.commit()
        db.refresh(system_connection)
        trigger_data_quality_provisioning(reason="connection-catalog-updated")


@router.delete("/{system_connection_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_system_connection(
    system_connection_id: UUID, db: Session = Depends(get_db)
) -> None:
    system_connection = _get_system_connection_or_404(system_connection_id, db)
    removals = [
        CatalogRemoval(
            system_connection_id=system_connection.id,
            schema_name=selection.schema_name,
            table_name=selection.table_name,
        )
        for selection in system_connection.catalog_selections
    ]

    if removals:
        cascade_cleanup_for_catalog_removals(db, removals)

    db.delete(system_connection)
    db.commit()
    trigger_data_quality_provisioning(reason="connection-deleted")

